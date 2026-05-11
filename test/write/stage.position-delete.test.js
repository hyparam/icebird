import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { fetchAvroRecords } from '../../src/fetch.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'
import { icebergStagePositionDelete } from '../../src/write/stage-position-delete.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergDelete } from '../../src/write/write.js'
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
    expect(staged.snapshot.summary?.['total-records']).toBe('3') // deletes don't count
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

  it('writes null sort_order_id for position delete manifest entries', async () => {
    const tableUrl = 'http://test/del-sort-order-null'
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
    const manifestPath = staged.writtenFiles.find(f => f.endsWith('.avro') && !f.includes('snap-'))
    if (!manifestPath) throw new Error('delete manifest path missing')
    const entries = await fetchAvroRecords(manifestPath, resolver)

    expect(entries[0].data_file.sort_order_id).toBeUndefined()
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

  it('rejects deletes targeting an unknown data file', async () => {
    const tableUrl = 'http://test/del-unknown'
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
    const created = await icebergCreate({
      tableUrl, resolver, schema: partitionedSchema,
      partitionSpec: {
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'country', transform: 'identity' }],
      },
    })
    const appended = await icebergStageAppend({
      tableUrl, metadata: created,
      records: [{ id: 1n, country: 'us' }],
      resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })
    await expect(icebergStagePositionDelete({
      tableUrl, metadata: afterAppend, resolver,
      deletes: [{ file_path: 'no-such-file.parquet', pos: 0n }],
    })).rejects.toThrow('target data file not found in current snapshot')
  })

  it('writes per-partition delete files for partitioned tables', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/del-partitioned-rt'
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
    const created = await icebergCreate({
      tableUrl, resolver, schema: partitionedSchema,
      partitionSpec: {
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'country', transform: 'identity' }],
      },
    })
    const records = [
      { id: 1n, country: 'us' },
      { id: 2n, country: 'us' },
      { id: 3n, country: 'ca' },
      { id: 4n, country: 'ca' },
    ]
    const appended = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })

    // appended.writtenFiles[0..N-1] are the per-partition data files; identify
    // each by its partition value via the manifest entry.
    const dataPaths = appended.writtenFiles.filter(f => f.endsWith('.parquet'))
    expect(dataPaths).toHaveLength(2)

    // Delete row 0 from each partition's file. The stager must look the
    // partition up from the existing manifest and write one delete file per
    // partition group.
    const staged = await icebergStagePositionDelete({
      tableUrl,
      metadata: afterAppend,
      deletes: dataPaths.map(p => ({ file_path: p, pos: 0n })),
      resolver,
    })
    expect(staged.snapshot.summary?.['added-delete-files']).toBe('2')
    expect(staged.snapshot.summary?.['added-position-deletes']).toBe('2')
    expect(staged.snapshot.summary?.['changed-partition-count']).toBe('2')

    // Two delete files, one per partition, each referencing one data file.
    const deletePaths = staged.writtenFiles.filter(f => f.endsWith('-deletes.parquet'))
    expect(deletePaths).toHaveLength(2)

    // Read the new delete manifest and verify partition tuples are populated.
    const manifestPaths = staged.writtenFiles.filter(f => f.endsWith('.avro') && !f.includes('snap-'))
    expect(manifestPaths).toHaveLength(1)
    const entries = await fetchAvroRecords(manifestPaths[0], resolver)
    expect(entries).toHaveLength(2)
    const countries = entries.map(e => e.data_file.partition.country).sort()
    expect(countries).toEqual(['ca', 'us'])
    for (const entry of entries) {
      // Each delete file targets exactly one data file, so referenced_data_file is set.
      expect(entry.data_file.referenced_data_file).toBeTypeOf('string')
    }

    // After commit, both deleted rows are gone, others remain.
    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: afterAppend, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: afterDelete, resolver })
    const ids = read.map(r => r.id).sort((/** @type {bigint} */ a, /** @type {bigint} */ b) => Number(a - b))
    // We deleted pos 0 from each file. With one file per partition, each file
    // has 2 rows; pos 0 within each is removed, leaving one row per partition.
    expect(ids).toHaveLength(2)
  })

  it('rejects empty deletes', async () => {
    const tableUrl = 'http://test/del-empty'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    await expect(icebergStagePositionDelete({
      tableUrl, metadata: created, deletes: [], resolver,
    })).rejects.toThrow('non-empty')
  })

  it('rejects adding new parquet position delete files to v3 tables', async () => {
    const tableUrl = 'http://test/del-v3-parquet-reject'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    const created = await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })
    const appended = await icebergStageAppend({
      tableUrl, metadata: created, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }], resolver,
    })
    await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })

    await expect(icebergDelete({
      catalog,
      tableUrl,
      deletes: [{ file_path: appended.writtenFiles[0], pos: 0n }],
      mode: 'parquet',
    })).rejects.toThrow(/v3|position delete|deletion vector/i)
  })

  it('rejects format-version 3 tables', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/del-v3'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    const v3Metadata = { ...created, 'format-version': /** @type {3} */ (3), 'next-row-id': 100 }

    await expect(icebergStagePositionDelete({
      tableUrl,
      metadata: v3Metadata,
      deletes: [{ file_path: 'x.parquet', pos: 0n }],
      resolver,
    })).rejects.toThrow(/deletion vectors/)
  })
})
