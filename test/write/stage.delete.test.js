import { describe, expect, it, vi } from 'vitest'
import { fetchAvroRecords } from '../../src/fetch.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'
import { icebergStageAppend, icebergStagePositionDelete } from '../../src/write/stage.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
}

describe('icebergStagePositionDelete', () => {
  it('round-trips: row 0 disappears after delete is committed', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/del-rt'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const records = [
      { id: 1n, name: 'alice' },
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
    ]
    const appended = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })
    const dataPath = appended.writtenFiles[0]

    const staged = await icebergStagePositionDelete({
      tableUrl,
      metadata: afterAppend,
      deletes: [{ file_path: dataPath, pos: 0n }],
      resolver,
    })
    expect(staged.snapshot.summary?.operation).toBe('delete')
    expect(staged.snapshot.summary?.['added-delete-files']).toBe('1')
    expect(staged.snapshot.summary?.['added-position-deletes']).toBe('1')
    expect(staged.snapshot.summary?.['total-data-files']).toBe('1')
    expect(staged.snapshot.summary?.['total-delete-files']).toBe('1')
    expect(staged.snapshot.summary?.['total-position-deletes']).toBe('1')
    expect(staged.snapshot.summary?.['total-records']).toBe('3')
    expect(staged.writtenFiles).toHaveLength(3)

    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: afterAppend, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: afterDelete, resolver })
    expect(read).toEqual([
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
    ])
  })

  it('sets referenced_data_file when all deletes target one file', async () => {
    const tableUrl = 'http://test/del-ref-single'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    const appended = await icebergStageAppend({
      tableUrl, metadata: created, records: [{ id: 1n, name: 'a' }], resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })

    const staged = await icebergStagePositionDelete({
      tableUrl,
      metadata: afterAppend,
      deletes: [{ file_path: appended.writtenFiles[0], pos: 0n }],
      resolver,
    })
    const manifestPath = staged.writtenFiles[1]
    const entries = await fetchAvroRecords(manifestPath, resolver)
    expect(entries).toHaveLength(1)
    expect(entries[0].data_file.referenced_data_file).toBe(appended.writtenFiles[0])
  })

  it('omits referenced_data_file when deletes span multiple files', async () => {
    const tableUrl = 'http://test/del-ref-multi'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    const append1 = await icebergStageAppend({
      tableUrl, metadata: created, records: [{ id: 1n, name: 'a' }], resolver,
    })
    const after1 = await fileCatalogCommit({ tableUrl, metadata: created, staged: append1, resolver })
    const append2 = await icebergStageAppend({
      tableUrl, metadata: after1, records: [{ id: 2n, name: 'b' }], resolver,
    })
    const after2 = await fileCatalogCommit({ tableUrl, metadata: after1, staged: append2, resolver })

    const staged = await icebergStagePositionDelete({
      tableUrl,
      metadata: after2,
      deletes: [
        { file_path: append1.writtenFiles[0], pos: 0n },
        { file_path: append2.writtenFiles[0], pos: 0n },
      ],
      resolver,
    })
    const manifestPath = staged.writtenFiles[1]
    const entries = await fetchAvroRecords(manifestPath, resolver)
    expect(entries[0].data_file.referenced_data_file).toBeUndefined()
    expect(entries[0].data_file.record_count).toBe(2n)

    // Read should drop the deleted row from each data file.
    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: after2, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: afterDelete, resolver })
    expect(read).toEqual([])
  })

  it('rejects partitioned tables', async () => {
    const tableUrl = 'http://test/del-partitioned'
    const { resolver } = memResolver()
    /** @type {Schema} */
    const partitionedSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'country', required: true, type: 'string' },
      ],
    }
    const created = await icebergCreate({ tableUrl, resolver, schema: partitionedSchema })
    const partitioned = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'country', transform: 'identity' }],
      }],
    }
    await expect(icebergStagePositionDelete({
      tableUrl, metadata: partitioned, resolver,
      deletes: [{ file_path: 'x', pos: 0n }],
    })).rejects.toThrow('unpartitioned tables only')
  })

  it('rejects empty deletes', async () => {
    const tableUrl = 'http://test/del-empty'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    await expect(icebergStagePositionDelete({
      tableUrl, metadata: created, deletes: [], resolver,
    })).rejects.toThrow('non-empty')
  })

  it('preserves v3 next-row-id and emits added-rows=0', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/del-v3'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    const v3Metadata = { ...created, 'format-version': /** @type {3} */ (3), 'next-row-id': 100 }
    const appended = await icebergStageAppend({
      tableUrl, metadata: v3Metadata, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }], resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: v3Metadata, staged: appended, resolver })
    expect(afterAppend['next-row-id']).toBe(102)

    const staged = await icebergStagePositionDelete({
      tableUrl,
      metadata: afterAppend,
      deletes: [{ file_path: appended.writtenFiles[0], pos: 0n }],
      resolver,
    })
    expect(staged.snapshot['first-row-id']).toBe(102)
    expect(staged.snapshot['added-rows']).toBe(0)
    expect(staged.requirements).toContainEqual({ type: 'assert-next-row-id', 'next-row-id': 102 })

    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: afterAppend, staged, resolver })
    expect(afterDelete['next-row-id']).toBe(102)
  })
})
