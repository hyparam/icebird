import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { readBatchColumn, selectedRowCount, valueAt } from 'squirreling'
import { readDataFileBatches } from '../src/read.js'
import { icebergCreate } from '../src/create.js'
import { memResolver } from './helpers.js'

/** @import {ManifestEntry, Schema} from '../src/types.js' */

/**
 * @param {Parameters<typeof readDataFileBatches>[0]} options
 * @returns {Promise<unknown[]>}
 */
async function values(options) {
  const result = []
  for await (const batch of readDataFileBatches(options)) {
    const vector = batch.columns.length ? await readBatchColumn({ batch, columnIndex: 0 }) : undefined
    for (let i = 0; i < selectedRowCount(batch.selection); i++) result.push(vector ? valueAt(vector, i) : null)
  }
  return result
}

describe('metadata-only prepared batches', () => {
  it('returns constant values and empty projections without opening Parquet', async () => {
    const { options, opened } = await fixture()
    expect(await values(options)).toEqual(Array(4).fill('2026-09-18'))
    expect(await values({ ...options, fields: [] })).toHaveLength(4)
    expect(opened()).toBe(0)
  })

  it('applies bounds and only applicable position deletes', async () => {
    const { options, opened } = await fixture()
    const deleteEntry = { ...options.dataEntry, data_file: { ...options.dataEntry.data_file, content: /** @type {const} */ (1) } }
    const staleDelete = { ...deleteEntry, sequence_number: 0n }
    const positionDeletesMap = new Map([['file.parquet', [
      { deleteEntry, positions: new Set([-1n, 0n, 2n, 99n]) },
      { deleteEntry, positions: new Set([2n]) },
      { deleteEntry: staleDelete, positions: new Set([1n]) },
    ]]])
    expect(await values({ ...options, positionDeletesMap, fileRowStart: 1, fileRowEnd: 3 })).toEqual(['2026-09-18'])
    expect(await values({ ...options, fileRowStart: 4 })).toEqual([])
    expect(await values({ ...options, fileRowEnd: 0 })).toEqual([])
    expect(opened()).toBe(0)
  })

  it('uses identity partitions without metrics, including renamed and null partitions', async () => {
    const { options, opened } = await fixture()
    options.metadata['partition-specs'][0].fields = [{ 'source-id': 1, 'field-id': 1000, name: 'old_day', transform: 'identity' }]
    options.dataEntry.data_file.partition = { old_day: '2026-09-18' }
    options.dataEntry.data_file.value_counts = undefined
    expect(await values(options)).toEqual(Array(4).fill('2026-09-18'))
    options.dataEntry.data_file.partition = { old_day: undefined }
    expect(await values(options)).toEqual(Array(4).fill(null))
    expect(opened()).toBe(0)
  })

  it('keeps partition columns constant alongside physical payload columns', async () => {
    const { options, opened } = await fixture()
    options.metadata['partition-specs'][0].fields = [{ 'source-id': 1, 'field-id': 1000, name: 'day', transform: 'identity' }]
    options.dataEntry.data_file.partition = { day: '2026-09-18' }
    options.dataEntry.data_file.value_counts = undefined
    const fields = [...options.fields, { id: 2, name: 'x', dataType: { type: /** @type {const} */ ('number') }, nullable: false }]
    for await (const batch of readDataFileBatches({ ...options, fields })) {
      expect(batch.columns[0]).toMatchObject({ type: 'constant', value: '2026-09-18' })
      const x = await readBatchColumn({ batch, columnIndex: 1 })
      expect(Array.from({ length: x.length }, (_, i) => valueAt(x, i))).toEqual([0, 1, 2, 3])
    }
    expect(opened()).toBe(1)
  })

  it('ignores equality deletes that do not apply to this file', async () => {
    const { options, opened } = await fixture()
    const deleteEntry = { ...options.dataEntry, data_file: { ...options.dataEntry.data_file, content: /** @type {const} */ (2), equality_ids: [2] } }
    const equalityDeleteGroups = [{ deleteEntry, rows: [{ 2: 1 }] }]
    expect(await values({ ...options, equalityDeleteGroups })).toHaveLength(4)
    expect(await values({ ...options, equalityDeleteGroups, fields: [] })).toHaveLength(4)
    expect(opened()).toBe(0)
  })

  it('falls back for equality deletes even if the projection is constant', async () => {
    const { options, opened } = await fixture()
    const deleteEntry = { ...options.dataEntry, sequence_number: 2n, data_file: { ...options.dataEntry.data_file, content: /** @type {const} */ (2), equality_ids: [2] } }
    const equalityDeleteGroups = [{ deleteEntry, rows: [{ 2: 1 }, { 2: 3 }] }]
    expect(await values({ ...options, equalityDeleteGroups })).toEqual(['2026-09-18', '2026-09-18'])
    expect(opened()).toBe(1)
  })

  it('falls back for missing metrics and still enforces exact source filters', async () => {
    const { options, opened } = await fixture()
    const dataEntry = { ...options.dataEntry, data_file: { ...options.dataEntry.data_file, value_counts: undefined } }
    expect(await values({ ...options, dataEntry })).toHaveLength(4)
    expect(await values({ ...options, applyFilter: true, filter: { day: { $eq: 'other' } } })).toEqual([])
    expect(opened()).toBe(2)
  })

  it('leaves conservative filters for the engine and validates requested fields', async () => {
    const { options, opened } = await fixture()
    // Prepared readers do not claim to have applied WHERE. Residual evaluation
    // may reject this entire constant batch without decoding payload columns.
    expect(await values({ ...options, filter: { day: { $eq: 'other' } } })).toHaveLength(4)
    expect(opened()).toBe(0)
    const fields = [{ ...options.fields[0], id: 99 }]
    await expect(values({ ...options, fields })).rejects.toThrow('Iceberg field id 99 not found')
  })

  it('honors cancellation before I/O and when resumed after a metadata batch', async () => {
    const { options, opened } = await fixture()
    const controller = new AbortController()
    const iterator = readDataFileBatches({ ...options, signal: controller.signal })
    expect((await iterator.next()).done).toBe(false)
    controller.abort()
    await expect(iterator.next()).rejects.toThrow()
    await expect(values({ ...options, signal: controller.signal })).rejects.toThrow()
    expect(opened()).toBe(0)
  })
})

/** @returns {Promise<{options: Parameters<typeof readDataFileBatches>[0], opened: () => number}>} */
async function fixture() {
  /** @type {Schema} */
  const schema = { type: 'struct', 'schema-id': 0, fields: [
    { id: 1, name: 'day', type: 'string', required: true },
    { id: 2, name: 'x', type: 'int', required: true },
  ] }
  const file = parquetWriteBuffer({
    columnData: [{ name: 'day', data: Array(4).fill('2026-09-18') }, { name: 'x', data: [0, 1, 2, 3], type: 'INT32' }],
    kvMetadata: [{ key: 'iceberg.schema', value: JSON.stringify(schema) }],
  })
  const metadata = await icebergCreate({ tableUrl: 'http://test/metadata', schema, resolver: memResolver().resolver })
  const bound = new TextEncoder().encode('2026-09-18')
  /** @type {ManifestEntry} */
  const dataEntry = { status: 1, sequence_number: 1n, partition_spec_id: 0, data_file: {
    content: 0, file_path: 'file.parquet', file_format: 'parquet', partition: {}, record_count: 4n, file_size_in_bytes: BigInt(file.byteLength),
    value_counts: { 1: 4n }, null_value_counts: { 1: 0n }, lower_bounds: { 1: bound }, upper_bounds: { 1: bound },
  } }
  let opens = 0
  return { opened: () => opens, options: {
    dataEntry, metadata, schema,
    fields: [{ id: 1, name: 'day', dataType: { type: 'string' }, nullable: false }],
    resolver: { reader() { opens++; return { byteLength: file.byteLength, slice: (start, end) => file.slice(start, end) } } },
  } }
}
