import { collect, executeSql, readBatchColumn, selectedRowCount, valueAt } from 'squirreling'
import { describe, expect, it } from 'vitest'
import { icebergCreate } from '../../src/create.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergStageDeletionVector } from '../../src/write/stage-deletion-vector.js'
import { icebergStagePositionDelete } from '../../src/write/stage-position-delete.js'
import { computeColumnStats } from '../../src/write/stats.js'
import { serializeValue } from '../../src/write/serde.js'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { icebergQuery } from '../../src/sql/icebergQuery.js'
import { pruneTopKFiles, scanTopKFiles } from '../../src/sql/topK.js'
import { localResolver, memResolver } from '../helpers.js'

/**
 * @import {AsyncDataSource, AsyncBatch, Field, ScanTopK, SqlPrimitive} from 'squirreling'
 * @import {ManifestEntry, Resolver, Schema} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct', 'schema-id': 0,
  fields: [
    { id: 1, name: 'id', type: 'int', required: true },
    { id: 2, name: 'date', type: 'date', required: false },
    { id: 3, name: 'ts', type: 'timestamptz', required: false },
    { id: 4, name: 'iso', type: 'string', required: false },
  ],
}

/**
 * @param {{deleted?: number, vector?: boolean, duplicate?: boolean}} [options]
 * @returns {Promise<{ source: Awaited<ReturnType<typeof icebergDataSource>>, opened: Set<string>, tableUrl: string, resolver: Resolver }>}
 */
async function fixture({ deleted = 0, vector = false, duplicate = false } = {}) {
  const tableUrl = 'mem://top-k'
  const { resolver } = memResolver()
  let metadata = await icebergCreate({ tableUrl, resolver, schema, formatVersion: vector ? 3 : 2 })
  let lastPath = ''
  const dataPaths = new Set()
  for (const base of [0, 1000, 2000]) {
    const records = Array.from({ length: 150 }, (_, i) => {
      const id = base + i
      const date = new Date(Date.UTC(2020, 0, 1) + id * 86400000)
      return { id, date, ts: date, iso: date.toISOString() }
    })
    const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    lastPath = staged.writtenFiles[0]
    dataPaths.add(lastPath)
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  for (const count of deleted ? duplicate ? [50, deleted] : [deleted] : []) {
    const stage = vector ? icebergStageDeletionVector : icebergStagePositionDelete
    const staged = await stage({
      tableUrl, metadata, resolver,
      deletes: Array.from({ length: count }, (_, pos) => ({ file_path: lastPath, pos })),
    })
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  const opened = new Set()
  /** @type {Resolver} */
  const counting = {
    reader(path, length) {
      if (dataPaths.has(path)) opened.add(path)
      return resolver.reader(path, length)
    },
  }
  const source = await icebergDataSource({ tableUrl, metadata, resolver: counting })
  return { source, opened, tableUrl, resolver: counting }
}

/**
 * @param {Awaited<ReturnType<typeof icebergDataSource>>} source
 * @returns {AsyncDataSource}
 */
function withoutTopK(source) {
  return {
    ...source,
    prepareScan(request) {
      return source.prepareScan({ ...request, topK: undefined })
    },
  }
}

/** @type {Field[]} */
const idFields = [{ id: 1, name: 'id', dataType: { type: 'unknown' }, nullable: true }]

/** @type {Field[]} */
const payloadFields = [...idFields, { id: 4, name: 'iso', dataType: { type: 'unknown' }, nullable: false }]

/**
 * @param {number} limit
 * @returns {ScanTopK}
 */
function descending(limit) {
  return { orderBy: [{ field: 1, direction: 'DESC', nulls: 'LAST' }], limit }
}

/**
 * @param {(number | null)[]} values
 * @returns {AsyncBatch}
 */
function idBatch(values) {
  return {
    selection: { type: 'all', length: values.length },
    columns: [{ type: 'values', values, length: values.length }],
  }
}

/**
 * @param {(number | null)[]} ids
 * @param {SqlPrimitive[]} payloads
 * @returns {AsyncBatch}
 */
function payloadBatch(ids, payloads) {
  return {
    selection: { type: 'all', length: ids.length },
    columns: [
      { type: 'values', values: ids, length: ids.length },
      { type: 'values', values: payloads, length: ids.length },
    ],
  }
}

/** @returns {Promise<Awaited<ReturnType<typeof icebergDataSource>>>} */
async function listFixture() {
  const tableUrl = 'mem://top-k-list'
  const { resolver } = memResolver()
  /** @type {Schema} */
  const listSchema = {
    type: 'struct', 'schema-id': 0,
    fields: [
      { id: 1, name: 'id', type: 'int', required: true },
      { id: 2, name: 's', required: false, type: { type: 'list', 'element-id': 3, 'element-required': true, element: 'int' } },
    ],
  }
  let metadata = await icebergCreate({ tableUrl, resolver, schema: listSchema })
  for (const record of [{ id: 1, s: [1] }, { id: 10, s: [1, 2] }, { id: 20, s: null }]) {
    const staged = await icebergStageAppend({ tableUrl, metadata, records: [record], resolver })
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  return icebergDataSource({ tableUrl, metadata, resolver })
}

describe('metadata Top-K', () => {
  it('prunes older files using integer bounds', async () => {
    const { source, opened } = await fixture()
    const rows = await collect(executeSql({ query: 'SELECT id FROM t ORDER BY id DESC LIMIT 1', tables: { t: source } }))
    expect(rows).toEqual([{ id: 2149 }])
    expect(opened.size).toBe(1)
  })

  it('prunes older files using date bounds', async () => {
    const { source, opened } = await fixture()
    const rows = await collect(executeSql({ query: 'SELECT id FROM t ORDER BY date DESC LIMIT 1', tables: { t: source } }))
    expect(rows).toEqual([{ id: 2149 }])
    expect(opened.size).toBe(1)
  })

  it('prunes older files using timestamp bounds', async () => {
    const { source, opened } = await fixture()
    const rows = await collect(executeSql({ query: 'SELECT id FROM t ORDER BY ts DESC LIMIT 1', tables: { t: source } }))
    expect(rows).toEqual([{ id: 2149 }])
    expect(opened.size).toBe(1)
  })

  it('prunes older files using string bounds', async () => {
    const { source, opened } = await fixture()
    const rows = await collect(executeSql({ query: 'SELECT id FROM t ORDER BY iso DESC LIMIT 1', tables: { t: source } }))
    expect(rows).toEqual([{ id: 2149 }])
    expect(opened.size).toBe(1)
  })

  it('leaves a source reusable after pruning a query', async () => {
    const { source } = await fixture()
    await collect(executeSql({ query: 'SELECT id FROM t ORDER BY id DESC LIMIT 1', tables: { t: source } }))
    expect(await collect(executeSql({ query: 'SELECT COUNT(*) AS n FROM t', tables: { t: source } })))
      .toEqual([{ n: 450 }])
  })

  it('loads a Top-K source from a table URL', async () => {
    const { tableUrl, resolver } = await fixture()
    const result = await icebergQuery({
      query: 'SELECT id FROM t ORDER BY id DESC LIMIT 1', tables: { t: tableUrl }, resolver,
    })
    expect(await collect(result)).toEqual([{ id: 2149 }])
  })

  it('prunes higher bounds for ascending order', async () => {
    const { source, opened } = await fixture()
    expect(await collect(executeSql({ query: 'SELECT id FROM t ORDER BY id ASC LIMIT 1', tables: { t: source } })))
      .toEqual([{ id: 0 }])
    expect(opened.size).toBe(1)
  })

  it('resolves a projected alias used as the sort key', async () => {
    const { source } = await fixture()
    expect(await collect(executeSql({ query: 'SELECT id AS d FROM t ORDER BY d DESC LIMIT 1', tables: { t: source } })))
      .toEqual([{ d: 2149 }])
  })

  it('keeps enough files for an offset crossing a file boundary', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id FROM t ORDER BY id ASC LIMIT 2 OFFSET 149'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 149 }, { id: 1000 }])
    expect(opened.size).toBe(2)
  })

  it('evaluates a computed projection after selecting winners', async () => {
    const { source } = await fixture()
    const query = 'SELECT id + 1 AS next FROM t ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ next: 2150 }])
  })

  it('excludes files rejected by WHERE before choosing a winner', async () => {
    const { source } = await fixture()
    const query = 'SELECT id FROM t WHERE id < 2000 ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1149 }])
  })

  it('applies DISTINCT before the limit', async () => {
    const { source } = await filteredFixture([[{ id: 5, iso: 'a' }, { id: 5, iso: 'b' }, { id: 4, iso: 'c' }]])
    const query = 'SELECT DISTINCT id FROM t ORDER BY id DESC LIMIT 2'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 5 }, { id: 4 }])
  })

  it('does not push a limit below aggregation', async () => {
    const { source } = await fixture()
    const query = 'SELECT COUNT(*) AS n FROM t ORDER BY n DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ n: 450 }])
  })

  it('leaves expression sort keys to the engine', async () => {
    const { source } = await fixture()
    const query = 'SELECT id FROM t ORDER BY -id DESC LIMIT 2'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 0 }, { id: 1 }])
  })

  it('reads every file when ORDER BY has no limit', async () => {
    const { source, opened } = await fixture()
    const rows = await collect(executeSql({ query: 'SELECT id FROM t ORDER BY id DESC', tables: { t: source } }))
    expect(rows).toHaveLength(450)
    expect(opened.size).toBe(3)
  })

  it('preserves window numbering before Top-K selection', async () => {
    const { source } = await filteredFixture([[{ id: 1, iso: 'a' }, { id: 2, iso: 'b' }]])
    const query = 'SELECT id, ROW_NUMBER() OVER () AS n FROM t ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2, n: 2 }])
  })

  it('sorts a common table expression', async () => {
    const { source } = await fixture()
    const query = 'WITH a AS (SELECT id FROM t) SELECT id FROM a ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2149 }])
  })

  it('skips a file fully removed by position deletes', async () => {
    const { source, opened } = await fixture({ deleted: 150 })
    const query = 'SELECT id FROM t ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1149 }])
    expect(opened.size).toBe(1)
  })

  it('skips a file fully removed by a deletion vector', async () => {
    const { source, opened } = await fixture({ deleted: 150, vector: true })
    const query = 'SELECT id FROM t ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1149 }])
    expect(opened.size).toBe(1)
  })

  it('counts overlapping position deletes only once', async () => {
    const { source, opened } = await fixture({ deleted: 100, duplicate: true })
    const query = 'SELECT id FROM t ORDER BY id DESC LIMIT 100'
    const rows = await collect(executeSql({ query, tables: { t: source } }))
    expect(rows.map(row => row.id)).toEqual([
      ...Array.from({ length: 50 }, (_, i) => 2149 - i),
      ...Array.from({ length: 50 }, (_, i) => 1149 - i),
    ])
    expect(opened.size).toBe(2)
  })

  it('counts overlapping deletion vectors only once', async () => {
    const { source, opened } = await fixture({ deleted: 100, vector: true, duplicate: true })
    const query = 'SELECT id FROM t ORDER BY id DESC LIMIT 100'
    const rows = await collect(executeSql({ query, tables: { t: source } }))
    expect(rows.map(row => row.id)).toEqual([
      ...Array.from({ length: 50 }, (_, i) => 2149 - i),
      ...Array.from({ length: 50 }, (_, i) => 1149 - i),
    ])
    expect(opened.size).toBe(2)
  })

  it('returns all survivors when fewer than K remain', async () => {
    const { source } = await fixture({ deleted: 150 })
    const query = 'SELECT id FROM t ORDER BY id DESC LIMIT 500'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toHaveLength(300)
  })

  it('preserves equality-delete results with a Top-K hint', async () => {
    const source = await icebergDataSource({
      tableUrl: 's3://hyperparam-iceberg/java/bunnies', metadataFileName: 'v5.metadata.json', resolver: localResolver('test/files'),
    })
    const query = 'SELECT "Breed Name", "Popularity Rank" FROM t ORDER BY "Popularity Rank" DESC LIMIT 10'
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
  })

  it('resolves a sort alias referring to an earlier alias', async () => {
    const { source } = await fixture()
    const query = 'SELECT id AS a, a AS b FROM t ORDER BY b DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ a: 2149, b: 2149 }])
  })

  it('keeps sub-millisecond timestamp ties at SQL Date precision', () => {
    /** @type {ManifestEntry[]} */
    const entries = [1001n, 1999n, 999n].map(micros => ({
      status: 1,
      data_file: {
        content: 0, file_path: '', file_format: 'parquet', partition: {},
        record_count: 1n, file_size_in_bytes: 0n,
        value_counts: { 3: 1n }, null_value_counts: { 3: 0n },
        lower_bounds: { 3: serializeValue(micros, 'timestamptz') },
        upper_bounds: { 3: serializeValue(micros, 'timestamptz') },
      },
    }))
    expect(pruneTopKFiles(entries, schema, { orderBy: [{ field: 3, direction: 'DESC', nulls: 'FIRST' }], limit: 1 }))
      .toEqual(entries.slice(0, 2))
  })

  it('retains files tied with the metadata cutoff', () => {
    const entries = entriesFor([[5], [5], [1]])
    expect(pruneTopKFiles(entries, schema, descending(1))).toEqual(entries.slice(0, 2))
  })

  it('retains a file whose bounds overlap the metadata cutoff', () => {
    const entries = entriesFor([[4, 10], [3, 8], [1, 2]])
    expect(pruneTopKFiles(entries, schema, descending(1))).toEqual(entries.slice(0, 2))
  })

  it('retains a null-bearing file despite its low non-null bounds', () => {
    const entries = entriesFor([[5], [null, 1]])
    expect(pruneTopKFiles(entries, schema, descending(1))).toEqual(entries)
  })

  it('retains a file with missing bounds', () => {
    const entries = entriesFor([[5], [1]])
    delete entries[1].data_file.upper_bounds
    expect(pruneTopKFiles(entries, schema, descending(1))).toEqual(entries)
  })

  it('does not prune when metadata cannot certify K rows', () => {
    const entries = entriesFor([[5], [1]])
    expect(pruneTopKFiles(entries, schema, descending(3))).toEqual(entries)
  })
})



/**
 * @param {(number | null)[][]} groups
 * @returns {ManifestEntry[]}
 */
function entriesFor(groups) {
  return groups.map((values, index) => ({
    status: 1,
    data_file: {
      content: 0, file_path: String(index), file_format: 'parquet', partition: {},
      record_count: BigInt(values.length), file_size_in_bytes: 0n,
      ...computeColumnStats(values.map(id => ({ id })), schema),
    },
  }))
}

describe('filtered Top-K', () => {
  it('compares a whole list value for IN in prepared scans', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s IN (1) ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1 }])
  })

  it('compares a whole list value for IN without a Top-K hint', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s IN (1) ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))).toEqual([{ id: 1 }])
  })

  it('compares a whole list value for IN in legacy scans', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s IN (1) ORDER BY id DESC LIMIT 1'
    const legacy = { columns: source.columns, scan: source.scan }
    expect(await collect(executeSql({ query, tables: { t: legacy } }))).toEqual([{ id: 1 }])
  })

  it('matches a whole list against multiple IN alternatives', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s IN (1, 3) ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1 }])
  })

  it('compares a whole list value for NOT IN in prepared scans', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s NOT IN (1) ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 10 }])
  })

  it('compares a whole list value for NOT IN without a Top-K hint', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s NOT IN (1) ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))).toEqual([{ id: 10 }])
  })

  it('compares a whole list value for NOT IN in legacy scans', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s NOT IN (1) ORDER BY id DESC LIMIT 1'
    const legacy = { columns: source.columns, scan: source.scan }
    expect(await collect(executeSql({ query, tables: { t: legacy } }))).toEqual([{ id: 10 }])
  })

  it('ignores a NULL IN alternative when another alternative matches', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s IN (1, NULL) ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1 }])
  })

  it('returns no matches for NOT IN containing NULL', async () => {
    const source = await listFixture()
    const query = 'SELECT id FROM t WHERE s NOT IN (1, NULL) ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([])
  })

  it('chooses an integer winner after filtering', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id FROM t WHERE id != 2149 ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2148 }])
    expect(opened.size).toBe(1)
  })

  it('chooses a date winner after filtering', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id FROM t WHERE id != 2149 ORDER BY date DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2148 }])
    expect(opened.size).toBe(1)
  })

  it('chooses a timestamp winner after filtering', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id FROM t WHERE id != 2149 ORDER BY ts DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2148 }])
    expect(opened.size).toBe(1)
  })

  it('chooses a string winner after filtering', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id FROM t WHERE id != 2149 ORDER BY iso DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2148 }])
    expect(opened.size).toBe(1)
  })

  it('counts only matching rows toward an ascending offset', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id FROM t WHERE id IN (149, 1149, 2149) ORDER BY id ASC LIMIT 1 OFFSET 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1149 }])
    expect(opened.size).toBe(2)
  })

  it('counts only matching rows toward a descending offset', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id FROM t WHERE id IN (149, 1149, 2149) ORDER BY id DESC LIMIT 1 OFFSET 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1149 }])
    expect(opened.size).toBe(2)
  })

  it('does not count position-deleted matches toward K', async () => {
    const { source } = await fixture({ deleted: 100 })
    const query = 'SELECT id FROM t WHERE id != 2149 ORDER BY id DESC LIMIT 50'
    const rows = await collect(executeSql({ query, tables: { t: source } }))
    expect(rows.map(row => row.id)).toEqual([...Array.from({ length: 49 }, (_, i) => 2148 - i), 1149])
  })

  it('does not count matches removed by a deletion vector toward K', async () => {
    const { source } = await fixture({ deleted: 100, vector: true })
    const query = 'SELECT id FROM t WHERE id != 2149 ORDER BY id DESC LIMIT 50'
    const rows = await collect(executeSql({ query, tables: { t: source } }))
    expect(rows.map(row => row.id)).toEqual([...Array.from({ length: 49 }, (_, i) => 2148 - i), 1149])
  })

  it('leaves a partial predicate to the engine before limiting', async () => {
    const { source } = await fixture()
    const query = 'SELECT id FROM t WHERE id >= 0 AND CAST(id AS TEXT) LIKE \'%49\' ORDER BY id DESC LIMIT 4'
    expect(await collect(executeSql({ query, tables: { t: source } })))
      .toEqual([{ id: 2149 }, { id: 2049 }, { id: 1149 }, { id: 1049 }])
  })

  it('returns fewer than K predicate matches', async () => {
    const { source } = await fixture()
    const query = 'SELECT id FROM t WHERE id IN (149, 1149, 2149) ORDER BY id DESC LIMIT 10'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2149 }, { id: 1149 }, { id: 149 }])
  })

  it('returns no rows for a predicate rejected by file bounds', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id FROM t WHERE id = -1 ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([])
    expect(opened.size).toBe(0)
  })

  it('leaves a secondary sort key to the engine', async () => {
    const { source } = await filteredFixture([[{ id: 5, iso: 'b' }], [{ id: 5, iso: 'a' }]])
    const query = 'SELECT iso FROM t WHERE id >= 0 ORDER BY id DESC, iso ASC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ iso: 'a' }])
  })

  it('selects nulls before non-null values for NULLS FIRST', async () => {
    const { source } = await filteredFixture([[{ id: 5, iso: 'value' }], [{ id: null, iso: 'null' }]])
    const query = 'SELECT id FROM t WHERE iso IS NOT NULL ORDER BY id DESC NULLS FIRST LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: null }])
  })

  it('selects non-null values before nulls for NULLS LAST', async () => {
    const { source } = await filteredFixture([[{ id: null, iso: 'null' }], [{ id: 5, iso: 'value' }]])
    const query = 'SELECT id FROM t WHERE iso IS NOT NULL ORDER BY id DESC NULLS LAST LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 5 }])
  })

  it('does not certify rows removed by equality deletes', async () => {
    const source = await icebergDataSource({
      tableUrl: 's3://hyperparam-iceberg/java/bunnies',
      metadataFileName: 'v5.metadata.json',
      resolver: localResolver('test/files'),
    })
    const query = 'SELECT "Breed Name" FROM t WHERE "Breed Name" IS NOT NULL ORDER BY "Popularity Rank" DESC LIMIT 10'
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
  })

  it('reads more than four small files without rereading them', async () => {
    const { source, visits } = await filteredFixture(Array.from({ length: 10 }, (_, i) => [
      { id: i * 10, iso: 'keep' }, { id: i * 10 + 1, iso: 'drop' },
    ]))
    const query = 'SELECT id FROM t WHERE iso = \'keep\' ORDER BY id DESC LIMIT 5'
    expect(await collect(executeSql({ query, tables: { t: source } })))
      .toEqual([90, 80, 70, 60, 50].map(id => ({ id })))
    expect(visits).toEqual([9, 8, 7, 6, 5])
  })

  it('tightens the cutoff from actual matches when file bounds overlap', async () => {
    const { source, visits } = await filteredFixture([
      [{ id: 0, iso: 'keep' }, { id: 100, iso: 'drop' }],
      [{ id: 50, iso: 'keep' }, { id: 99, iso: 'drop' }],
      [{ id: 40, iso: 'keep' }],
    ])
    expect(await collect(executeSql({
      query: 'SELECT id FROM t WHERE iso = \'keep\' ORDER BY id DESC LIMIT 1', tables: { t: source },
    }))).toEqual([{ id: 50 }])
    expect(visits).toEqual([0, 1])
  })

  it('restores stable ties after visiting a later file first', async () => {
    const early = new Date('2020-01-01')
    const late = new Date('2021-01-01')
    const { source, visits } = await filteredFixture([
      [{ id: 5, iso: 'keep', ts: early }, { id: 6, iso: 'drop' }],
      [{ id: 5, iso: 'keep', ts: late }, { id: 10, iso: 'drop' }],
    ])
    expect(await collect(executeSql({
      query: 'SELECT id, ts FROM t WHERE iso = \'keep\' ORDER BY id DESC LIMIT 1', tables: { t: source },
    }))).toEqual([{ id: 5, ts: early }])
    expect(visits).toEqual([1, 0])
  })

  it('buffers timestamp ties across files without rereading', async () => {
    const ts = new Date('2026-10-05T12:00:00Z')
    const newer = new Date('2026-10-05T13:00:00Z')
    const older = new Date('2026-10-05T11:00:00Z')
    const { source, visits } = await filteredFixture([
      [{ id: 1, iso: 'keep', ts: newer }, { id: 2, iso: 'keep', ts }],
      [{ id: 3, iso: 'keep', ts }, { id: 4, iso: 'keep', ts: older }],
      [{ id: 5, iso: 'keep', ts: older }],
    ])
    const query = 'SELECT id, ts FROM t WHERE iso = \'keep\' ORDER BY ts DESC LIMIT 1 OFFSET 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2, ts }])
    expect(visits).toEqual([0, 1])
  })

  it('reads every file only once when the predicate finds no matches', async () => {
    const { source, visits } = await filteredFixture(Array.from({ length: 7 }, (_, i) => [
      { id: i, iso: 'a' }, { id: i, iso: 'z' },
    ]))
    expect(await collect(executeSql({
      query: 'SELECT id FROM t WHERE iso = \'missing\' ORDER BY id DESC LIMIT 2', tables: { t: source },
    }))).toEqual([])
    expect(visits).toEqual([6, 5, 4, 3, 2, 1, 0])
  })

  it('emits every buffered boundary tie without rereading', async () => {
    const groups = [[5, 5, 5], [5, 5, 5], [1]]
    const entries = entriesFor(groups)
    /** @type {string[]} */
    const visits = []
    const fields = [{ id: 1, name: 'id', dataType: { type: /** @type {const} */ ('unknown') }, nullable: true }]
    const scan = scanTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'LAST' }], limit: 1,
    }, fields, async function* (entry) {
      visits.push(entry.data_file.file_path)
      const values = groups[Number(entry.data_file.file_path)]
      yield { selection: { type: 'all', length: values.length }, columns: [{ type: 'values', values, length: values.length }] }
    })
    if (!scan) throw new Error('expected supported Top-K')
    expect(await scanRows(scan)).toEqual(Array.from({ length: 6 }, () => [5]))
    expect(visits).toEqual(['0', '1'])
  })

  it('forgets an overflowing tie when a later batch improves the cutoff', async () => {
    const entries = entriesFor([[5, 10]])
    const fields = [{ id: 1, name: 'id', dataType: { type: /** @type {const} */ ('unknown') }, nullable: false }]
    let visits = 0
    const scan = scanTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'LAST' }], limit: 1,
    }, fields, async function* () {
      visits++
      for (const values of [Array.from({ length: 1030 }, () => 5), [10, 10]]) {
        yield { selection: { type: 'all', length: values.length }, columns: [{ type: 'values', values, length: values.length }] }
      }
    })
    if (!scan) throw new Error('expected supported Top-K')
    expect(await scanRows(scan)).toEqual([[10], [10]])
    expect(visits).toBe(1)
  })

  it('accounts for payloads already gathered before a heap row becomes a tie', async () => {
    const entries = entriesFor([[5], [5, 10]])
    /** @type {Field[]} */
    const fields = [
      { id: 1, name: 'id', dataType: { type: 'unknown' }, nullable: false },
      { id: 4, name: 'iso', dataType: { type: 'unknown' }, nullable: false },
    ]
    const payloads = ['early', 'x'.repeat(600000)]
    /** @type {number[]} */
    const visits = []
    const scan = scanTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'LAST' }], limit: 1,
    }, fields, async function* (entry) {
      const index = Number(entry.data_file.file_path)
      visits.push(index)
      // The row with id 10 does not survive the filter.
      yield {
        selection: { type: 'all', length: 1 },
        columns: [
          { type: 'values', values: [5], length: 1 },
          { type: 'values', values: [payloads[index]], length: 1 },
        ],
      }
    })
    if (!scan) throw new Error('expected supported Top-K')
    expect(await scanRows(scan)).toEqual(payloads.map(payload => [5, payload]))
    expect(visits).toEqual([1, 0, 0, 1])
  })

  it('propagates cancellation during execution', async () => {
    const entries = entriesFor([[1], [2]])
    const controller = new AbortController()
    const scan = scanTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'FIRST' }], limit: 1,
    }, [], async function* () {
      controller.abort(new Error('cancelled scan'))
      yield { selection: { type: 'all', length: 1 }, columns: [{ type: 'values', values: [2], length: 1 }] }
    }, controller.signal)
    if (!scan) throw new Error('expected supported Top-K')
    await expect(scan.next()).rejects.toThrow('cancelled scan')
  })

  it('reads an unknown-bound file even after filling the heap', async () => {
    const entries = entriesFor([[5], [9]])
    delete entries[1].data_file.upper_bounds
    /** @type {string[]} */
    const visits = []
    const scan = scanTopKFiles(entries, schema, descending(1), idFields, async function* (entry) {
      visits.push(entry.data_file.file_path)
      yield idBatch(entry.data_file.file_path === '0' ? [5] : [9])
    })
    expect(await scanRows(scan)).toEqual([[9]])
    expect(visits).toEqual(['0', '1'])
  })

  it('evicts worse candidates within a descending batch', async () => {
    const scan = scanTopKFiles(entriesFor([[3, 1, 5, 2, 4]]), schema, descending(2), idFields, async function* () {
      yield idBatch([3, 1, 5, 2, 4])
    })
    expect(await scanRows(scan)).toEqual([[5], [4]])
  })

  it('evicts worse candidates within an ascending batch', async () => {
    const scan = scanTopKFiles(entriesFor([[5, 2, 1, 4, 3]]), schema, {
      orderBy: [{ field: 1, direction: 'ASC', nulls: 'LAST' }], limit: 2,
    }, idFields, async function* () {
      yield idBatch([5, 2, 1, 4, 3])
    })
    expect(await scanRows(scan)).toEqual([[2], [1]])
  })

  it('keeps winners from earlier batches when a later batch improves the heap', async () => {
    const scan = scanTopKFiles(entriesFor([[3, 8, 9, 1]]), schema, descending(2), idFields, async function* () {
      yield idBatch([3, 8])
      yield idBatch([9, 1])
    })
    expect(await scanRows(scan)).toEqual([[8], [9]])
  })

  it('discards buffered ties when the cutoff improves within a batch', async () => {
    const scan = scanTopKFiles(entriesFor([[5, 5, 7, 7]]), schema, descending(1), idFields, async function* () {
      yield idBatch([5, 5, 7, 7])
    })
    expect(await scanRows(scan)).toEqual([[7], [7]])
  })

  it('discards buffered ties when a later batch improves the cutoff', async () => {
    const scan = scanTopKFiles(entriesFor([[5, 5, 7, 7]]), schema, descending(1), idFields, async function* () {
      yield idBatch([5, 5])
      yield idBatch([7, 7])
    })
    expect(await scanRows(scan)).toEqual([[7], [7]])
  })

  it('gathers tied payloads at the physical indices of an index selection', async () => {
    const scan = scanTopKFiles(entriesFor([[100, 5, 200, 5, 1]]), schema, descending(1), payloadFields, async function* () {
      const batch = payloadBatch([100, 5, 200, 5, 1], ['excluded', 'first', 'excluded', 'second', 'worse'])
      batch.selection = { type: 'indices', indices: Uint32Array.from([1, 3, 4]), length: 5 }
      yield batch
    })
    expect(await scanRows(scan)).toEqual([[5, 'first'], [5, 'second']])
  })

  it('gathers tied payloads at the physical indices of a range selection', async () => {
    const scan = scanTopKFiles(entriesFor([[100, 5, 5, 1, 200]]), schema, descending(1), payloadFields, async function* () {
      const batch = payloadBatch([100, 5, 5, 1, 200], ['excluded', 'first', 'second', 'worse', 'excluded'])
      batch.selection = { type: 'range', start: 1, end: 4, length: 5 }
      yield batch
    })
    expect(await scanRows(scan)).toEqual([[5, 'first'], [5, 'second']])
  })

  it('returns all input rows when the heap never fills', async () => {
    const scan = scanTopKFiles(entriesFor([[1, 5, 3]]), schema, descending(4), idFields, async function* () {
      yield idBatch([1, 5, 3])
    })
    expect(await scanRows(scan)).toEqual([[1], [5], [3]])
  })

  it('buffers ascending boundary ties', async () => {
    const scan = scanTopKFiles(entriesFor([[2, 1, 1]]), schema, {
      orderBy: [{ field: 1, direction: 'ASC', nulls: 'LAST' }], limit: 1,
    }, idFields, async function* () {
      yield idBatch([2, 1, 1])
    })
    expect(await scanRows(scan)).toEqual([[1], [1]])
  })

  it('buffers null boundary ties without rereading', async () => {
    let reads = 0
    const scan = scanTopKFiles(entriesFor([[9, null, null]]), schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'FIRST' }], limit: 1,
    }, idFields, async function* () {
      reads++
      yield idBatch([9, null, null])
    })
    expect(await scanRows(scan)).toEqual([[null], [null]])
    expect(reads).toBe(1)
  })

  it('does not emit an unprojected sort key with buffered ties', async () => {
    const scan = scanTopKFiles(entriesFor([[5, 5]]), schema, descending(1), [payloadFields[1]], async function* () {
      yield {
        selection: { type: 'all', length: 2 },
        columns: [
          { type: 'values', values: ['first', 'second'], length: 2 },
          { type: 'values', values: [5, 5], length: 2 },
        ],
      }
    })
    expect(await scanRows(scan)).toEqual([['first'], ['second']])
  })

  it('rereads all boundary ties when the extra-row budget overflows', async () => {
    const values = Array.from({ length: 1030 }, () => 5)
    let reads = 0
    const scan = scanTopKFiles(entriesFor([values]), schema, descending(1), idFields, async function* () {
      reads++
      yield idBatch(values)
    })
    expect(await scanRows(scan)).toEqual(values.map(value => [value]))
    expect(reads).toBe(2)
  })

  it('rereads all boundary ties when their total payload exceeds the byte budget', async () => {
    const payload = 'x'.repeat(300000)
    let reads = 0
    const scan = scanTopKFiles(entriesFor([[5, 5, 5]]), schema, descending(1), payloadFields, async function* () {
      reads++
      yield payloadBatch([5, 5, 5], [payload, payload, payload])
    })
    expect(await scanRows(scan)).toEqual([[5, payload], [5, payload], [5, payload]])
    expect(reads).toBe(2)
  })

  it('uses the fallback for nested tie payloads', async () => {
    const payload = { nested: ['x'] }
    let reads = 0
    const scan = scanTopKFiles(entriesFor([[5, 5]]), schema, descending(1), payloadFields, async function* () {
      reads++
      yield payloadBatch([5, 5], [payload, payload])
    })
    expect(await scanRows(scan)).toEqual([[5, payload], [5, payload]])
    expect(reads).toBe(2)
  })
})

/**
 * @param {Array<Array<{id: number | null, iso: string, ts?: Date}>>} groups
 * @returns {Promise<{source: Awaited<ReturnType<typeof icebergDataSource>>, visits: number[]}>}
 */
async function filteredFixture(groups) {
  const tableUrl = 'mem://best-first'
  const { resolver } = memResolver()
  const nullableSchema = { ...schema, fields: schema.fields.map(field => ({ ...field, required: false })) }
  let metadata = await icebergCreate({ tableUrl, resolver, schema: nullableSchema })
  /** @type {string[]} */
  const paths = []
  for (const records of groups) {
    const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    paths.push(staged.writtenFiles[0])
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  /** @type {number[]} */
  const visits = []
  const source = await icebergDataSource({ tableUrl, metadata, resolver: {
    reader(path, length) {
      const index = paths.indexOf(path)
      if (index >= 0) visits.push(index)
      return resolver.reader(path, length)
    },
  } })
  return { source, visits }
}

/**
 * @param {AsyncIterable<AsyncBatch> | undefined} scan
 * @returns {Promise<SqlPrimitive[][]>}
 */
async function scanRows(scan) {
  if (!scan) throw new Error('expected supported Top-K')
  const rows = []
  for await (const batch of scan) {
    const columns = await Promise.all(batch.columns.map((_, columnIndex) => readBatchColumn({ batch, columnIndex })))
    for (let i = 0; i < selectedRowCount(batch.selection); i++) rows.push(columns.map(column => valueAt(column, i)))
  }
  return rows
}
