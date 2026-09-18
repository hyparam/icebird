import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { readDataFile, readDataFileColumn } from '../src/read.js'
import { icebergCreate } from '../src/create.js'
import { memResolver } from './helpers.js'

/**
 * @import {ManifestEntry, Schema} from '../src/types.js'
 */

describe('native compatibility column reads', () => {
  it('returns contiguous numeric vectors without creating ordinary arrays', async () => {
    const options = await fixture()
    const chunks = []
    for await (const chunk of readDataFileColumn({ ...options, column: 'x' })) chunks.push(chunk)
    expect(chunks).toHaveLength(2)
    expect(chunks.every(chunk => chunk instanceof Float64Array)).toBe(true)
    expect(chunks.flatMap(chunk => Array.from(chunk))).toEqual([0, 1, 2, 3, 4, 5, 6, 7])
  })

  it('bounds physical ranges and limits before reading output values', async () => {
    const options = await fixture()
    const chunks = []
    for await (const chunk of readDataFileColumn({ ...options, column: 'x', fileRowStart: 3, fileRowEnd: 7, limit: 2 })) chunks.push(chunk)
    expect(chunks.flatMap(chunk => Array.from(chunk))).toEqual([3, 4])
    expect(chunks.every(chunk => chunk instanceof Float64Array)).toBe(true)
  })

  it('matches a converted predicate on another column and gathers only survivors', async () => {
    const options = await fixture()
    const chunks = []
    for await (const chunk of readDataFileColumn({ ...options, column: 'x', filter: { g: { $eq: 1 } }, limit: 3 })) chunks.push(chunk)
    expect(chunks.flatMap(chunk => Array.from(chunk))).toEqual([1, 3, 5])
  })

  it('composes position deletes and predicates using physical row positions', async () => {
    const options = await fixture()
    /** @type {ManifestEntry} */
    const deleteEntry = { ...options.dataEntry, data_file: { ...options.dataEntry.data_file, content: 1 } }
    const positionDeletesMap = new Map([['file.parquet', [{ deleteEntry, positions: new Set([1n, 5n]) }]]])
    const values = []
    for await (const chunk of readDataFileColumn({ ...options, column: 'x', filter: { g: { $eq: 1 } }, positionDeletesMap })) values.push(...Array.from(chunk))
    expect(values).toEqual([3, 7])
  })

  it('retains equality-delete semantics before emitting column chunks', async () => {
    const options = await fixture()
    /** @type {ManifestEntry} */
    const deleteEntry = { ...options.dataEntry, sequence_number: 2n, data_file: { ...options.dataEntry.data_file, content: 2, equality_ids: [1] } }
    const equalityDeleteGroups = [{ deleteEntry, rows: [{ 1: 2 }, { 1: 6 }] }]
    const values = []
    for await (const chunk of readDataFileColumn({ ...options, column: 'x', equalityDeleteGroups })) values.push(...Array.from(chunk))
    expect(values).toEqual([0, 1, 3, 4, 5, 7])
  })

  it('maps renamed fields by ID and preserves missing-field defaults', async () => {
    const options = await fixture()
    /** @type {Schema} */
    const schema = {
      ...options.schema,
      fields: [
        { ...options.schema.fields[0], name: 'renamed' },
        options.schema.fields[1],
        { id: 3, name: 'defaulted', type: 'double', required: false, 'initial-default': 42 },
      ],
    }
    const values = []
    for await (const chunk of readDataFileColumn({ ...options, schema, column: 'renamed', filter: { renamed: { $gte: 6 } } })) values.push(...Array.from(chunk))
    expect(values).toEqual([6, 7])
    const defaults = []
    for await (const chunk of readDataFileColumn({ ...options, schema, column: 'defaulted', limit: 3 })) defaults.push(...Array.from(chunk))
    expect(defaults).toEqual([42, 42, 42])
  })

  for (const mode of ['rows', 'columns']) {
    it(`${mode}: matches defaults, nulls and partition values when predicate columns are absent`, async () => {
      const options = await fixture()
      /** @type {Schema} */
      const schema = { ...options.schema, fields: [
        { ...options.schema.fields[0], name: 'renamed' },
        options.schema.fields[1],
        { id: 3, name: 'defaulted', type: 'double', required: false, 'initial-default': 42 },
        { id: 4, name: 'missing', type: 'double', required: false },
        { id: 5, name: 'partitioned', type: 'double', required: false, 'initial-default': 99 },
      ] }
      const metadata = { ...options.metadata, 'partition-specs': [{
        'spec-id': 0,
        fields: [{ 'source-id': 5, 'field-id': 1000, name: 'p', transform: /** @type {const} */ ('identity') }],
      }] }
      const dataEntry = { ...options.dataEntry, data_file: { ...options.dataEntry.data_file, partition: { p: 7 } } }
      /** @type {Array<[import('hyparquet').ParquetQueryFilter, number[]]>} */
      const cases = [
        [{ defaulted: { $eq: 99 } }, []],
        [{ defaulted: { $eq: 42 } }, [0, 1, 2, 3, 4, 5, 6, 7]],
        [{ missing: { $eq: null } }, [0, 1, 2, 3, 4, 5, 6, 7]],
        [{ missing: { $ne: null } }, []],
        [{ partitioned: { $eq: 7 } }, [0, 1, 2, 3, 4, 5, 6, 7]],
        [{ partitioned: { $eq: 99 } }, []],
        // g is a physical predicate column excluded from the output projection.
        [{ $and: [{ defaulted: { $eq: 42 } }, { g: { $eq: 1 } }] }, [1, 3, 5, 7]],
        [{ $or: [{ defaulted: { $eq: 99 } }, { renamed: { $gte: 6 } }] }, [6, 7]],
        [{ $nor: [{ defaulted: { $eq: 99 } }, { renamed: { $lt: 6 } }] }, [6, 7]],
      ]
      for (const [filter, expected] of cases) {
        const readOptions = { ...options, schema, metadata, dataEntry, column: 'renamed', wantedColumns: ['renamed'], filter }
        const values = []
        if (mode === 'columns') {
          for await (const chunk of readDataFileColumn(readOptions)) values.push(...Array.from(chunk))
        } else {
          for await (const rows of readDataFile(readOptions)) {
            for (const row of rows) {
              expect(Object.keys(row)).toEqual(['renamed'])
              values.push(row.renamed)
            }
          }
        }
        expect(values, JSON.stringify(filter)).toEqual(expected)
      }
    })
  }

  it('preserves synthesized v3 lineage through the compatibility fallback', async () => {
    const options = await fixture()
    const values = []
    for await (const chunk of readDataFileColumn({
      ...options,
      rowLineage: true,
      metadata: { ...options.metadata, 'format-version': 3 },
      dataEntry: { ...options.dataEntry, data_file: { ...options.dataEntry.data_file, first_row_id: 100n } },
      column: '_row_id',
      wantedColumns: ['_row_id'],
      fileRowStart: 3,
      fileRowEnd: 7,
      limit: 2,
    })) values.push(...Array.from(chunk))
    expect(values).toEqual([103n, 104n])
  })

  it('opens no file for a zero limit', async () => {
    const options = await fixture()
    const resolver = { reader() { throw new Error('must not open a file') } }
    const chunks = []
    for await (const chunk of readDataFileColumn({ ...options, resolver, column: 'x', limit: 0 })) chunks.push(chunk)
    expect(chunks).toEqual([])
  })
})

/** @returns {Promise<Parameters<typeof readDataFileColumn>[0]>} */
async function fixture() {
  /** @type {Schema} */
  const schema = { type: 'struct', 'schema-id': 0, fields: [
    { id: 1, name: 'x', type: 'double', required: true },
    { id: 2, name: 'g', type: 'int', required: true },
  ] }
  const file = parquetWriteBuffer({
    columnData: [
      { name: 'x', data: Float64Array.from([0, 1, 2, 3, 4, 5, 6, 7]), type: 'DOUBLE', encoding: 'PLAIN', nullable: false },
      { name: 'g', data: Int32Array.from([0, 1, 0, 1, 0, 1, 0, 1]), type: 'INT32', encoding: 'PLAIN', nullable: false },
    ],
    rowGroupSize: 4,
    kvMetadata: [{ key: 'iceberg.schema', value: JSON.stringify(schema) }],
  })
  const metadata = await icebergCreate({ tableUrl: 'http://test/columns', schema, resolver: memResolver().resolver })
  return {
    dataEntry: {
      status: 1, sequence_number: 1n, partition_spec_id: 0,
      data_file: { content: 0, file_path: 'file.parquet', file_format: 'parquet', partition: {}, record_count: 8n, file_size_in_bytes: BigInt(file.byteLength) },
    },
    schema,
    metadata,
    resolver: { reader() { return { byteLength: file.byteLength, slice(start, end) { return file.slice(start, end) } } } },
    column: 'x',
    fileRowStart: 0,
    fileRowEnd: 8,
    rowLineage: false,
  }
}
