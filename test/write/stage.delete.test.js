import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { fetchAvroRecords } from '../../src/fetch.js'
import { puffinReadDeletionVector, readPuffinMetadata } from '../../src/puffin/puffin.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'
import { icebergStageAppend, icebergStageDeletionVector, icebergStagePositionDelete } from '../../src/write/stage.js'
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

/**
 * @param {import('../../src/types.js').TableMetadata} metadata
 * @param {import('../../src/types.js').Resolver} resolver
 * @returns {Promise<import('../../src/types.js').ManifestEntry[]>}
 */
async function currentManifestEntries(metadata, resolver) {
  const snapshot = metadata.snapshots?.find(s => s['snapshot-id'] === metadata['current-snapshot-id'])
  if (!snapshot?.['manifest-list']) throw new Error('current snapshot manifest-list missing')
  const manifests = await fetchAvroRecords(snapshot['manifest-list'], resolver)
  const entries = await Promise.all(manifests.map(async manifest => {
    const records = /** @type {import('../../src/types.js').ManifestEntry[]} */ (
      await fetchAvroRecords(manifest.manifest_path, resolver)
    )
    return records.map(record => ({
      ...record,
      partition_spec_id: manifest.partition_spec_id,
    }))
  }))
  return entries.flat()
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

describe('icebergStageDeletionVector', () => {
  /**
   * @param {string} tableUrl
   * @returns {Promise<{ resolver: ReturnType<typeof memResolver>['resolver'], files: ReturnType<typeof memResolver>['files'], metadata: import('../../src/types.js').TableMetadata }>}
   */
  async function v3Table(tableUrl) {
    const { resolver, files } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    const metadata = { ...created, 'format-version': /** @type {3} */ (3), 'next-row-id': 0 }
    return { resolver, files, metadata }
  }

  it('round-trips: row 0 disappears after DV is committed', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/dv-rt'
    const { resolver, files, metadata } = await v3Table(tableUrl)

    const records = [
      { id: 1n, name: 'alice' },
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
    ]
    const appended = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata, staged: appended, resolver })
    const dataPath = appended.writtenFiles[0]

    const staged = await icebergStageDeletionVector({
      tableUrl,
      metadata: afterAppend,
      deletes: [{ file_path: dataPath, pos: 0n }],
      resolver,
    })
    expect(staged.snapshot.summary?.operation).toBe('delete')
    expect(staged.snapshot.summary?.['added-delete-files']).toBe('1')
    expect(staged.snapshot.summary?.['added-position-deletes']).toBe('1')
    expect(staged.snapshot.summary?.['total-delete-files']).toBe('1')
    expect(staged.snapshot.summary?.['total-position-deletes']).toBe('1')
    expect(staged.writtenFiles).toHaveLength(3) // 1 puffin + 1 manifest + 1 manifest list
    const puffinPath = staged.writtenFiles[0]
    expect(puffinPath.endsWith('.puffin')).toBe(true)
    expect(files.has(puffinPath)).toBe(true)

    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: afterAppend, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: afterDelete, resolver })
    expect(read.map(r => ({ id: r.id, name: r.name }))).toEqual([
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
    ])
  })

  it('writes one puffin file per target data file', async () => {
    const tableUrl = 'http://test/dv-multi'
    const { resolver, metadata } = await v3Table(tableUrl)

    const append1 = await icebergStageAppend({ tableUrl, metadata, records: [{ id: 1n, name: 'a' }], resolver })
    const after1 = await fileCatalogCommit({ tableUrl, metadata, staged: append1, resolver })
    const append2 = await icebergStageAppend({ tableUrl, metadata: after1, records: [{ id: 2n, name: 'b' }], resolver })
    const after2 = await fileCatalogCommit({ tableUrl, metadata: after1, staged: append2, resolver })

    const staged = await icebergStageDeletionVector({
      tableUrl,
      metadata: after2,
      deletes: [
        { file_path: append1.writtenFiles[0], pos: 0n },
        { file_path: append2.writtenFiles[0], pos: 0n },
      ],
      resolver,
    })
    // 2 puffin files + 1 manifest + 1 manifest list
    expect(staged.writtenFiles).toHaveLength(4)
    expect(staged.snapshot.summary?.['added-delete-files']).toBe('2')

    const manifestPath = staged.writtenFiles[2]
    const entries = await fetchAvroRecords(manifestPath, resolver)
    expect(entries).toHaveLength(2)
    const targets = entries.map(e => e.data_file.referenced_data_file).sort()
    expect(targets).toEqual([append1.writtenFiles[0], append2.writtenFiles[0]].sort())

    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: after2, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: afterDelete, resolver })
    expect(read).toEqual([])
  })

  it('merges repeated deletion vectors for the same data file', async () => {
    const tableUrl = 'http://test/dv-repeat-merge'
    const { resolver, metadata } = await v3Table(tableUrl)

    const appended = await icebergStageAppend({
      tableUrl, metadata,
      records: [
        { id: 1n, name: 'a' },
        { id: 2n, name: 'b' },
        { id: 3n, name: 'c' },
        { id: 4n, name: 'd' },
      ],
      resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata, staged: appended, resolver })
    const dataPath = appended.writtenFiles[0]

    const deleteOne = await icebergStageDeletionVector({
      tableUrl,
      metadata: afterAppend,
      deletes: [{ file_path: dataPath, pos: 1n }],
      resolver,
    })
    const afterDeleteOne = await fileCatalogCommit({ tableUrl, metadata: afterAppend, staged: deleteOne, resolver })
    const deleteTwo = await icebergStageDeletionVector({
      tableUrl,
      metadata: afterDeleteOne,
      deletes: [{ file_path: dataPath, pos: 3n }],
      resolver,
    })
    const afterDeleteTwo = await fileCatalogCommit({ tableUrl, metadata: afterDeleteOne, staged: deleteTwo, resolver })

    const entries = await currentManifestEntries(afterDeleteTwo, resolver)
    const vectorsForDataFile = entries.filter(entry =>
      entry.data_file.content === 1 &&
      entry.data_file.file_format.toLowerCase() === 'puffin' &&
      entry.data_file.referenced_data_file === dataPath)

    expect(vectorsForDataFile).toHaveLength(1)
    expect(vectorsForDataFile[0].data_file.record_count).toBe(2n)
  })

  it('replaces existing position delete files when writing a deletion vector', async () => {
    const tableUrl = 'http://test/dv-replaces-position-delete'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const appended = await icebergStageAppend({
      tableUrl, metadata: created,
      records: [
        { id: 1n, name: 'a' },
        { id: 2n, name: 'b' },
        { id: 3n, name: 'c' },
      ],
      resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })
    const dataPath = appended.writtenFiles[0]

    const positionDelete = await icebergStagePositionDelete({
      tableUrl,
      metadata: afterAppend,
      deletes: [{ file_path: dataPath, pos: 1n }],
      resolver,
    })
    const afterPositionDelete = await fileCatalogCommit({
      tableUrl,
      metadata: afterAppend,
      staged: positionDelete,
      resolver,
    })

    const upgraded = await fileCatalogCommit({
      tableUrl,
      metadata: { ...afterPositionDelete, 'format-version': /** @type {3} */ (3), 'next-row-id': 0 },
      resolver,
      staged: {
        snapshot: /** @type {any} */ (null),
        requirements: [{ type: /** @type {const} */ ('assert-table-uuid'), uuid: afterPositionDelete['table-uuid'] }],
        updates: [],
        writtenFiles: [],
      },
    })

    const deletionVector = await icebergStageDeletionVector({
      tableUrl,
      metadata: upgraded,
      deletes: [{ file_path: dataPath, pos: 2n }],
      resolver,
    })
    const afterDeletionVector = await fileCatalogCommit({
      tableUrl,
      metadata: upgraded,
      staged: deletionVector,
      resolver,
    })

    const entries = await currentManifestEntries(afterDeletionVector, resolver)
    const deletesForTarget = entries.filter(entry =>
      entry.data_file.content === 1 &&
      entry.data_file.referenced_data_file === dataPath)

    expect(deletesForTarget).toHaveLength(1)
    expect(deletesForTarget[0].data_file.file_format.toLowerCase()).toBe('puffin')
    expect(deletesForTarget[0].data_file.record_count).toBe(2n)
    expect(deletionVector.snapshot.summary?.['removed-delete-files']).toBe('1')
    expect(deletionVector.snapshot.summary?.['removed-position-deletes']).toBe('1')
    expect(deletionVector.snapshot.summary?.['removed-dvs']).toBeUndefined()

    const read = await icebergRead({ tableUrl, metadata: afterDeletionVector, resolver })
    expect(read.map(r => r.id)).toEqual([1n])
  })

  it('records v3 DV fields on the manifest entry and a readable blob in the puffin file', async () => {
    const tableUrl = 'http://test/dv-fields'
    const { resolver, metadata } = await v3Table(tableUrl)
    const appended = await icebergStageAppend({
      tableUrl, metadata, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }], resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata, staged: appended, resolver })

    const staged = await icebergStageDeletionVector({
      tableUrl,
      metadata: afterAppend,
      deletes: [
        { file_path: appended.writtenFiles[0], pos: 0n },
        { file_path: appended.writtenFiles[0], pos: 1n },
      ],
      resolver,
    })
    const puffinPath = staged.writtenFiles[0]
    const manifestPath = staged.writtenFiles[1]
    const entries = await fetchAvroRecords(manifestPath, resolver)
    expect(entries).toHaveLength(1)
    const { data_file } = entries[0]
    expect(data_file.file_format.toLowerCase()).toBe('puffin')
    expect(data_file.record_count).toBe(2n)
    expect(data_file.referenced_data_file).toBe(appended.writtenFiles[0])
    expect(data_file.content_offset).toBe(4n)
    expect(typeof data_file.content_size_in_bytes).toBe('bigint')

    // Decode the puffin file directly and verify the blob matches
    const puffinFile = await resolver.reader(puffinPath, Number(data_file.file_size_in_bytes))
    const buffer = await puffinFile.slice(0, puffinFile.byteLength)
    const meta = readPuffinMetadata(new Uint8Array(buffer))
    expect(meta.blobs).toHaveLength(1)
    expect(meta.blobs[0].type).toBe('deletion-vector-v1')
    expect(meta.blobs[0]['snapshot-id']).toBe(-1)
    expect(meta.blobs[0]['sequence-number']).toBe(-1)
    expect(meta.blobs[0].properties?.['referenced-data-file']).toBe(appended.writtenFiles[0])
    expect(meta.blobs[0].properties?.cardinality).toBe('2')
    const positions = await puffinReadDeletionVector(puffinFile, {
      offset: meta.blobs[0].offset,
      length: meta.blobs[0].length,
      referencedDataFile: appended.writtenFiles[0],
    })
    expect(positions).toEqual(new Set([0n, 1n]))
  })

  it('writes null sort_order_id for deletion vector manifest entries', async () => {
    const tableUrl = 'http://test/dv-sort-order-null'
    const { resolver, metadata } = await v3Table(tableUrl)
    const appended = await icebergStageAppend({
      tableUrl, metadata, records: [{ id: 1n, name: 'a' }], resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata, staged: appended, resolver })

    const staged = await icebergStageDeletionVector({
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

  it('rejects format-version 2', async () => {
    const tableUrl = 'http://test/dv-v2'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    await expect(icebergStageDeletionVector({
      tableUrl, metadata: created, deletes: [{ file_path: 'x', pos: 0n }], resolver,
    })).rejects.toThrow('format-version 3')
  })

  it('rejects empty deletes', async () => {
    const tableUrl = 'http://test/dv-bad'
    const { resolver, metadata } = await v3Table(tableUrl)
    await expect(icebergStageDeletionVector({
      tableUrl, metadata, deletes: [], resolver,
    })).rejects.toThrow('non-empty')
  })

  it('rejects deletes targeting an unknown data file (partitioned)', async () => {
    const tableUrl = 'http://test/dv-unknown'
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
      tableUrl, resolver, schema: partitionedSchema, formatVersion: 3,
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
    await expect(icebergStageDeletionVector({
      tableUrl, metadata: afterAppend,
      deletes: [{ file_path: 'no-such-file.parquet', pos: 0n }], resolver,
    })).rejects.toThrow('target data file not found in current snapshot')
  })

  it('writes per-partition deletion vectors for partitioned tables', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/dv-partitioned-rt'
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
      tableUrl, resolver, schema: partitionedSchema, formatVersion: 3,
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

    const dataPaths = appended.writtenFiles.filter(f => f.endsWith('.parquet'))
    expect(dataPaths).toHaveLength(2)

    const staged = await icebergStageDeletionVector({
      tableUrl,
      metadata: afterAppend,
      deletes: dataPaths.map(p => ({ file_path: p, pos: 0n })),
      resolver,
    })
    expect(staged.snapshot.summary?.['added-delete-files']).toBe('2')
    expect(staged.snapshot.summary?.['added-position-deletes']).toBe('2')
    expect(staged.snapshot.summary?.['changed-partition-count']).toBe('2')

    const puffinPaths = staged.writtenFiles.filter(f => f.endsWith('.puffin'))
    expect(puffinPaths).toHaveLength(2)

    const manifestPaths = staged.writtenFiles.filter(f => f.endsWith('.avro') && !f.includes('snap-'))
    expect(manifestPaths).toHaveLength(1)
    const entries = await fetchAvroRecords(manifestPaths[0], resolver)
    expect(entries).toHaveLength(2)
    const countries = entries.map(e => e.data_file.partition.country).sort()
    expect(countries).toEqual(['ca', 'us'])
    for (const entry of entries) {
      expect(entry.data_file.file_format.toLowerCase()).toBe('puffin')
      expect(entry.data_file.referenced_data_file).toBeTypeOf('string')
    }

    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: afterAppend, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: afterDelete, resolver })
    expect(read).toHaveLength(2)
  })

  it('uses the target data file partition spec after default spec evolves to unpartitioned', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/dv-partition-evolution'
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
      tableUrl, resolver, schema: partitionedSchema, formatVersion: 3,
      partitionSpec: {
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'country', transform: 'identity' }],
      },
    })
    const appended = await icebergStageAppend({
      tableUrl, metadata: created,
      records: [
        { id: 1n, country: 'us' },
        { id: 2n, country: 'us' },
        { id: 3n, country: 'ca' },
      ],
      resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })
    const targetPath = appended.writtenFiles.find(f => f.endsWith('.parquet'))
    if (!targetPath) throw new Error('data path missing')

    const evolved = await fileCatalogCommit({
      tableUrl,
      metadata: afterAppend,
      resolver,
      staged: {
        snapshot: /** @type {any} */ (null),
        requirements: [{ type: /** @type {const} */ ('assert-table-uuid'), uuid: afterAppend['table-uuid'] }],
        updates: [
          { action: /** @type {const} */ ('add-spec'), spec: { 'spec-id': -1, fields: [] } },
          { action: /** @type {const} */ ('set-default-spec'), 'spec-id': -1 },
        ],
        writtenFiles: [],
      },
    })

    const staged = await icebergStageDeletionVector({
      tableUrl,
      metadata: evolved,
      deletes: [{ file_path: targetPath, pos: 0n }],
      resolver,
    })
    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: evolved, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: afterDelete, resolver })
    const ids = read.map(r => r.id).sort((a, b) => Number(a - b))

    expect(ids).toEqual([2n, 3n])
  })

  it('preserves v3 next-row-id and emits added-rows=0', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/dv-nextrow'
    const { resolver, metadata } = await v3Table(tableUrl)
    const seeded = { ...metadata, 'next-row-id': 100 }
    const appended = await icebergStageAppend({
      tableUrl, metadata: seeded, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }], resolver,
    })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: seeded, staged: appended, resolver })
    expect(afterAppend['next-row-id']).toBe(102)

    const staged = await icebergStageDeletionVector({
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
