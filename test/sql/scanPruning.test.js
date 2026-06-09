import { describe, expect, it, vi } from 'vitest'
import { ByteWriter, parquetWrite } from 'hyparquet-writer'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { readDataFile } from '../../src/read.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { memResolver } from '../helpers.js'

/**
 * @import {AsyncBuffer, SchemaElement} from 'hyparquet'
 * @import {ColumnSource} from 'hyparquet-writer'
 * @import {ExprNode} from 'squirreling'
 * @import {ManifestEntry, Resolver, Schema, TableMetadata} from '../../src/types.js'
 */

/**
 * @param {string} url
 * @returns {boolean}
 */
function isDataUrl(url) {
  return /\/data\/.*\.parquet$/.test(url)
}

/**
 * Wrap a resolver, counting bytes sliced from data parquet files and the set
 * of distinct data files opened.
 *
 * @param {Resolver} inner
 * @returns {{ resolver: Resolver, dataBytes: () => number, dataFilesRead: () => number }}
 */
function countingResolver(inner) {
  let dataBytes = 0
  /** @type {Set<string>} */
  const dataFiles = new Set()
  /** @type {Resolver} */
  const resolver = {
    async reader(url, byteLength) {
      const r = await inner.reader(url, byteLength)
      if (!isDataUrl(url)) return r
      dataFiles.add(url)
      return {
        byteLength: r.byteLength,
        slice(s, e) {
          dataBytes += (e ?? r.byteLength) - (s ?? 0)
          return r.slice(s, e)
        },
      }
    },
    writer: inner.writer ? inner.writer.bind(inner) : undefined,
    deleter: inner.deleter ? inner.deleter.bind(inner) : undefined,
  }
  return { resolver, dataBytes: () => dataBytes, dataFilesRead: () => dataFiles.size }
}

/**
 * @param {string} column
 * @param {string} op
 * @param {any} value
 * @returns {ExprNode}
 */
function cmp(column, op, value) {
  return /** @type {ExprNode} */ ({
    type: 'binary', op,
    left: { type: 'identifier', name: column },
    right: { type: 'literal', value },
  })
}

describe('#20 file-level bounds pruning (icebird-written)', () => {
  /** @type {Schema} */
  const schema = {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'v', required: false, type: 'int' },
    ],
  }

  /**
   * Build a 3-file unpartitioned table with disjoint id ranges, returning a
   * counting resolver bound to the written bytes.
   *
   * @returns {Promise<{ tableUrl: string, resolver: Resolver, dataFilesRead: () => number }>}
   */
  async function build3FileTable() {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://prune20'
    const { resolver: memR } = memResolver()
    let metadata = await icebergCreate({ tableUrl, resolver: memR, schema })
    for (const base of [0, 100, 1000]) {
      const records = []
      for (let i = 0; i < 10; i++) records.push({ id: BigInt(base + i), v: base + i })
      const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver: memR })
      metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver: memR })
    }
    const { resolver, dataFilesRead } = countingResolver({ reader: memR.reader })
    return { tableUrl, resolver, dataFilesRead }
  }

  it('opens only the data file whose id bounds can match a range predicate', async () => {
    const { tableUrl, resolver, dataFilesRead } = await build3FileTable()
    const source = await icebergDataSource({ tableUrl, resolver })
    const out = []
    for await (const row of source.scan({ where: cmp('id', '>=', 1000n) }).rows()) out.push(row.resolved)

    expect(out.map(r => r?.id).sort((a, b) => Number(a) - Number(b)))
      .toEqual([1000n, 1001n, 1002n, 1003n, 1004n, 1005n, 1006n, 1007n, 1008n, 1009n])
    expect(dataFilesRead()).toBe(1)
  })

  it('opens only the matching file for an equality predicate', async () => {
    const { tableUrl, resolver, dataFilesRead } = await build3FileTable()
    const source = await icebergDataSource({ tableUrl, resolver })
    const out = []
    for await (const row of source.scan({ where: cmp('id', '=', 105n) }).rows()) out.push(row.resolved)

    expect(out).toEqual([{ id: 105n, v: 105 }])
    expect(dataFilesRead()).toBe(1)
  })

  it('reads all files when the predicate spans every file', async () => {
    const { tableUrl, resolver, dataFilesRead } = await build3FileTable()
    const source = await icebergDataSource({ tableUrl, resolver })
    const out = []
    for await (const row of source.scan({ where: cmp('id', '>=', 0n) }).rows()) out.push(row.resolved)

    expect(out.length).toBe(30)
    expect(dataFilesRead()).toBe(3)
  })
})

describe('#21 row-group pruning (readDataFile)', () => {
  /** @type {Schema} */
  const schemaV = {
    type: 'struct',
    'schema-id': 0,
    fields: [{ id: 1, name: 'v', required: false, type: 'int' }],
  }
  /** @type {Schema} */
  const schemaVP = {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: 'v', required: false, type: 'int' },
      { id: 2, name: 'payload', required: false, type: 'string' },
    ],
  }

  /**
   * Write a single parquet file with `rowGroupSize` rows per group (so the
   * file has many disjoint-range row groups), into a counting resolver. When
   * `payloadBytes` > 0 a large string column is added so the file exceeds
   * hyparquet's 512KB whole-file prefetch threshold and per-row-group byte
   * reads become observable.
   *
   * @param {object} opts
   * @param {number} opts.count
   * @param {number} opts.rowGroupSize
   * @param {boolean} opts.statistics
   * @param {number} [opts.payloadBytes]
   * @returns {{ resolver: Resolver, dataBytes: () => number, dataEntry: ManifestEntry, metadata: TableMetadata, schema: Schema, count: number }}
   */
  function writeFile({ count, rowGroupSize, statistics, payloadBytes = 0 }) {
    const schema = payloadBytes > 0 ? schemaVP : schemaV
    const values = []
    const payloads = []
    for (let i = 0; i < count; i++) {
      values.push(i)
      if (payloadBytes > 0) payloads.push(String(i).padEnd(payloadBytes, 'x'))
    }
    /** @type {ColumnSource[]} */
    const columnData = [{ name: 'v', data: values }]
    /** @type {SchemaElement[]} */
    const parquetSchema = [
      { name: 'root', num_children: payloadBytes > 0 ? 2 : 1 },
      { name: 'v', type: 'INT32', repetition_type: 'OPTIONAL', field_id: 1 },
    ]
    if (payloadBytes > 0) {
      columnData.push({ name: 'payload', data: payloads })
      parquetSchema.push({ name: 'payload', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL', field_id: 2 })
    }
    const writer = new ByteWriter()
    parquetWrite({
      writer,
      columnData,
      schema: parquetSchema,
      kvMetadata: [{ key: 'iceberg.schema', value: JSON.stringify(schema) }],
      codec: 'UNCOMPRESSED',
      statistics,
      rowGroupSize,
    })
    const bytes = writer.getBytes()
    const filePath = 'mem://data/file.parquet'
    /** @type {Resolver} */
    const inner = {
      reader() {
        return {
          byteLength: bytes.byteLength,
          slice(start, end = bytes.byteLength) {
            const s = bytes.subarray(start, end)
            return s.buffer.slice(s.byteOffset, s.byteOffset + s.byteLength)
          },
        }
      },
    }
    const { resolver, dataBytes } = countingResolver(inner)
    /** @type {ManifestEntry} */
    const dataEntry = {
      status: 1,
      sequence_number: 0n,
      partition_spec_id: 0,
      data_file: {
        content: 0,
        file_path: filePath,
        file_format: 'parquet',
        partition: {},
        record_count: BigInt(count),
        file_size_in_bytes: BigInt(bytes.byteLength),
      },
    }
    /** @type {TableMetadata} */
    const metadata = {
      'format-version': 2,
      'table-uuid': 'test',
      location: 'mem://table',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': payloadBytes > 0 ? 2 : 1,
      'current-schema-id': 0,
      schemas: [schema],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': payloadBytes > 0 ? 2 : 1,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
    }
    return { resolver, dataBytes, dataEntry, metadata, schema, count }
  }

  /**
   * @param {{ resolver: Resolver, dataEntry: ManifestEntry, metadata: TableMetadata, schema: Schema, count: number }} f
   * @param {any} filter
   * @returns {Promise<any[]>}
   */
  async function read(f, filter) {
    const rows = []
    for await (const batch of readDataFile({
      dataEntry: f.dataEntry,
      fileRowStart: 0,
      fileRowEnd: f.count,
      schema: f.schema,
      metadata: f.metadata,
      resolver: f.resolver,
      rowLineage: false,
      positionDeletesMap: new Map(),
      equalityDeleteGroups: [],
      filter,
    })) {
      for (const row of batch) rows.push(row)
    }
    return rows
  }

  it('reads fewer bytes for a selective predicate on a multi-row-group file', async () => {
    // 8 single-row groups, each carrying a ~100KB payload so the file exceeds
    // the 512KB whole-file prefetch threshold; v ranges are [0]..[7].
    const full = writeFile({ count: 8, rowGroupSize: 1, statistics: true, payloadBytes: 100_000 })
    const fullRows = await read(full, undefined)

    const selective = writeFile({ count: 8, rowGroupSize: 1, statistics: true, payloadBytes: 100_000 })
    // v >= 7 matches only the last row group; the other 7 are pruned by stats,
    // so their ~100KB payload chunks are never fetched.
    const selRows = await read(selective, { v: { $gte: 7 } })

    expect(fullRows.length).toBe(8)
    expect(selRows.length).toBe(1)
    expect(selRows[0].v).toBe(7)
    // Reads roughly one row group instead of eight.
    expect(selective.dataBytes()).toBeLessThan(full.dataBytes() / 2)
  })

  it('falls back to a full decode (correctly) when row-group stats are absent', async () => {
    // Many single-row groups, NO statistics: hyparquet cannot stat-skip, so it
    // must decode every group and match per-row. Result must still be correct.
    const f = writeFile({ count: 50, rowGroupSize: 1, statistics: false })
    const all = await read(f, undefined)
    const filtered = await read(f, { v: { $gte: 25 } })

    expect(all.length).toBe(50)
    expect(filtered).toEqual(all.filter(r => r.v >= 25))
    expect(filtered.length).toBe(25)
  })
})
