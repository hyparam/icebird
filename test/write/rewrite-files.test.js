import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergManifests, splitManifestEntries } from '../../src/manifest.js'
import { icebergRead } from '../../src/read.js'
import { icebergRewrite } from '../../src/write/write.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergStagePositionDelete } from '../../src/write/stage-position-delete.js'
import { icebergStageRewrite } from '../../src/write/rewrite.js'
import { memResolver } from '../helpers.js'

/**
 * @import {ManifestEntry, Schema, SortOrder, TableMetadata} from '../../src/types.js'
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

/** @type {SortOrder} */
const sortById = {
  'order-id': 1,
  fields: [{ transform: 'identity', 'source-id': 1, direction: 'asc', 'null-order': 'nulls-last' }],
}

/**
 * @param {any[]} a
 * @returns {any[]}
 */
function byId(a) {
  return [...a].sort((x, y) => Number(x.id - y.id))
}

/**
 * Live data entries of the current snapshot, in manifest-list order.
 * @param {TableMetadata} metadata
 * @param {import('../../src/types.js').Resolver} resolver
 * @returns {Promise<ManifestEntry[]>}
 */
async function dataEntriesOf(metadata, resolver) {
  return splitManifestEntries(await icebergManifests({ metadata, resolver })).dataEntries
}

/**
 * Create a sorted, multi-file table (one data file per batch) and return its
 * committed metadata plus the data file path of each batch in append order.
 * @param {object} [opts]
 * @param {SortOrder} [opts.sortOrder]
 * @param {2 | 3} [opts.formatVersion]
 * @returns {Promise<{ tableUrl: string, resolver: import('../../src/types.js').Resolver, metadata: TableMetadata, filePaths: string[] }>}
 */
async function makeMultiFileTable({ sortOrder, formatVersion } = {}) {
  vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
  const tableUrl = 'mem://rewrite-files'
  const { resolver } = memResolver()
  let metadata = await icebergCreate({ tableUrl, resolver, schema, sortOrder, formatVersion })
  const batches = [
    [{ id: 5n, name: 'e' }, { id: 2n, name: 'b' }],
    [{ id: 1n, name: 'a' }, { id: 6n, name: 'f' }],
    [{ id: 4n, name: 'd' }, { id: 3n, name: 'c' }],
  ]
  /** @type {string[]} */
  const filePaths = []
  for (const records of batches) {
    const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    filePaths.push(staged.writtenFiles.find(p => p.endsWith('.parquet')) ?? '')
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  return { tableUrl, resolver, metadata, filePaths }
}

describe('icebergStageRewrite with files — subset compaction', () => {
  it('merges only the named files and carries the rest forward untouched', async () => {
    const { tableUrl, resolver, metadata, filePaths } = await makeMultiFileTable({ sortOrder: sortById })

    const before = await icebergRead({ tableUrl, metadata, resolver })
    const beforeEntries = await dataEntriesOf(metadata, resolver)
    expect(beforeEntries.length).toBe(3)
    const survivorBefore = beforeEntries.find(e => e.data_file.file_path === filePaths[2])

    const staged = await icebergStageRewrite({
      tableUrl, metadata, resolver, files: [filePaths[0], filePaths[1]],
    })
    const after = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })

    // Same multiset of rows.
    const rows = await icebergRead({ tableUrl, metadata: after, resolver })
    expect(byId(rows)).toEqual(byId(before))

    // Two files rewritten into one; the third untouched.
    const afterEntries = await dataEntriesOf(after, resolver)
    expect(afterEntries.length).toBe(2)
    const survivor = afterEntries.find(e => e.data_file.file_path === filePaths[2])
    const merged = afterEntries.find(e => e.data_file.file_path !== filePaths[2])
    expect(survivor).toBeDefined()
    expect(merged).toBeDefined()
    // The survivor keeps its original identity: sequence numbers and snapshot.
    expect(survivor?.sequence_number).toEqual(survivorBefore?.sequence_number)
    expect(survivor?.file_sequence_number).toEqual(survivorBefore?.file_sequence_number)
    expect(survivor?.snapshot_id).toEqual(survivorBefore?.snapshot_id)
    // The merged file holds exactly the victims' rows, sorted by id.
    expect(merged?.data_file.record_count).toBe(4n)
    expect(merged?.data_file.sort_order_id).toBe(1)

    // Summary counts the victims as deleted and the survivor in the totals.
    expect(staged.snapshot.summary.operation).toBe('replace')
    expect(staged.snapshot.summary['added-data-files']).toBe('1')
    expect(staged.snapshot.summary['deleted-data-files']).toBe('2')
    expect(staged.snapshot.summary['deleted-records']).toBe('4')
    expect(staged.snapshot.summary['total-data-files']).toBe('2')
    expect(staged.snapshot.summary['total-records']).toBe('6')
  })

  it('splits rewritten output at targetFileRows', async () => {
    const { tableUrl, resolver, metadata, filePaths } = await makeMultiFileTable({ sortOrder: sortById })

    const staged = await icebergStageRewrite({
      tableUrl, metadata, resolver, files: [filePaths[0], filePaths[1]], targetFileRows: 2,
    })
    const after = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })

    const entries = await dataEntriesOf(after, resolver)
    // 4 victim rows in 2 files of 2, plus the survivor.
    expect(entries.length).toBe(3)
    const rows = await icebergRead({ tableUrl, metadata: after, resolver })
    expect(byId(rows).map(r => r.id)).toEqual([1n, 2n, 3n, 4n, 5n, 6n])
  })

  it('naming every live file behaves as a full rewrite, consuming deletes', async () => {
    const { tableUrl, resolver, metadata, filePaths } = await makeMultiFileTable({ sortOrder: sortById })
    const delStaged = await icebergStagePositionDelete({
      tableUrl, metadata, resolver, deletes: [{ file_path: filePaths[0], pos: 0n }],
    })
    const afterDelete = await fileCatalogCommit({ tableUrl, metadata, staged: delStaged, resolver })

    const staged = await icebergStageRewrite({ tableUrl, metadata: afterDelete, resolver, files: filePaths })
    const after = await fileCatalogCommit({ tableUrl, metadata: afterDelete, staged, resolver })

    const { dataEntries, deleteEntries } = splitManifestEntries(await icebergManifests({ metadata: after, resolver }))
    expect(dataEntries.length).toBe(1)
    expect(deleteEntries.length).toBe(0)
    expect(staged.snapshot.summary['total-delete-files']).toBe('0')
    const rows = await icebergRead({ tableUrl, metadata: after, resolver })
    // Appends sort by the declared order, so pos 0 of the first file is id 2.
    expect(rows.map(r => r.id)).toEqual([1n, 3n, 4n, 5n, 6n])
  })
})

describe('icebergStageRewrite with files — deletes', () => {
  it('consumes deletes on rewritten files and keeps deletes on survivors applying', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://rewrite-files-del'
    const { resolver } = memResolver()
    let metadata = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {string[]} */
    const filePaths = []
    for (const records of [
      [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }],
      [{ id: 3n, name: 'c' }, { id: 4n, name: 'd' }],
    ]) {
      const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
      filePaths.push(staged.writtenFiles.find(p => p.endsWith('.parquet')) ?? '')
      metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
    }

    // Delete one row in the future victim and one in the future survivor.
    const delStaged = await icebergStagePositionDelete({
      tableUrl, metadata, resolver,
      deletes: [{ file_path: filePaths[0], pos: 0n }, { file_path: filePaths[1], pos: 1n }],
    })
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged: delStaged, resolver })
    expect(await icebergRead({ tableUrl, metadata, resolver }))
      .toEqual([{ id: 2n, name: 'b' }, { id: 3n, name: 'c' }])

    const staged = await icebergStageRewrite({ tableUrl, metadata, resolver, files: [filePaths[0]] })
    const after = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })

    // The rewritten file physically dropped its deleted row; the survivor's
    // delete still applies through its carried-forward sequence numbers.
    const rows = await icebergRead({ tableUrl, metadata: after, resolver })
    expect(byId(rows)).toEqual([{ id: 2n, name: 'b' }, { id: 3n, name: 'c' }])
    const merged = (await dataEntriesOf(after, resolver)).find(e => e.data_file.file_path !== filePaths[1])
    expect(merged?.data_file.record_count).toBe(1n)
  })
})

describe('icebergStageRewrite with files — validation', () => {
  it('rejects a path that is not a live data file of the current snapshot', async () => {
    const { tableUrl, resolver, metadata } = await makeMultiFileTable()
    await expect(icebergStageRewrite({
      tableUrl, metadata, resolver, files: [`${tableUrl}/data/nope.parquet`],
    })).rejects.toThrow(/data file not found in current snapshot/)
  })

  it('rejects an empty or duplicate file list', async () => {
    const { tableUrl, resolver, metadata, filePaths } = await makeMultiFileTable()
    await expect(icebergStageRewrite({ tableUrl, metadata, resolver, files: [] }))
      .rejects.toThrow(/files must be a non-empty array/)
    await expect(icebergStageRewrite({
      tableUrl, metadata, resolver, files: [filePaths[0], filePaths[0]],
    })).rejects.toThrow(/duplicate/)
  })
})

describe('icebergStageRewrite with files — v3 row lineage', () => {
  it('preserves lineage on rewritten rows and does not advance next-row-id', async () => {
    const { tableUrl, resolver, metadata, filePaths } = await makeMultiFileTable({ sortOrder: sortById, formatVersion: 3 })
    expect(metadata['next-row-id']).toBe(6)
    const before = await icebergRead({ tableUrl, metadata, resolver })
    const lineageBefore = new Map(before.map(r => [r.id, r._row_id]))

    const staged = await icebergStageRewrite({
      tableUrl, metadata, resolver, files: [filePaths[0], filePaths[1]],
    })
    expect(staged.snapshot['added-rows']).toBe(0)
    expect(staged.requirements).toContainEqual({ type: 'assert-next-row-id', 'next-row-id': 6 })

    const after = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
    expect(after['next-row-id']).toBe(6)
    const rows = await icebergRead({ tableUrl, metadata: after, resolver })
    for (const row of rows) {
      expect(row._row_id).toBe(lineageBefore.get(row.id))
    }
  })
})

describe('icebergRewrite with files — layer 2', () => {
  it('compacts a subset through the file catalog in one call', async () => {
    const { tableUrl, resolver, metadata, filePaths } = await makeMultiFileTable({ sortOrder: sortById })
    const before = await icebergRead({ tableUrl, metadata, resolver })

    const catalog = fileCatalog({ resolver })
    const after = await icebergRewrite({ catalog, tableUrl, files: [filePaths[0], filePaths[1]] })

    const rows = await icebergRead({ tableUrl, metadata: after, resolver })
    expect(byId(rows)).toEqual(byId(before))
    expect((await dataEntriesOf(after, resolver)).length).toBe(2)
  })
})
