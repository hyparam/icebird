import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergManifests, splitManifestEntries } from '../../src/manifest.js'
import { icebergRead } from '../../src/read.js'
import { icebergAppend, icebergRewrite } from '../../src/write/write.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergStagePositionDelete } from '../../src/write/stage-position-delete.js'
import { icebergStageRewrite } from '../../src/write/rewrite.js'
import { deserializeValue } from '../../src/write/serde.js'
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
 * Decode a long-typed bound from a (read-decoded) manifest entry.
 * @param {ManifestEntry} entry
 * @param {number} fieldId
 * @param {'lower'|'upper'} side
 * @returns {any}
 */
function longBound(entry, fieldId, side) {
  const map = side === 'lower' ? entry.data_file.lower_bounds : entry.data_file.upper_bounds
  if (!map) return undefined
  const e = Array.isArray(map) ? map.find(x => Number(x.key) === fieldId) : undefined
  const bytes = e ? e.value : /** @type {any} */ (map)[fieldId]
  return bytes ? deserializeValue(bytes, 'long') : undefined
}

/**
 * Create a sorted, multi-file table and return its committed metadata.
 * @param {object} [opts]
 * @param {SortOrder} [opts.sortOrder]
 * @returns {Promise<{ tableUrl: string, resolver: import('../../src/types.js').Resolver, metadata: TableMetadata }>}
 */
async function makeMultiFileTable({ sortOrder } = {}) {
  vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
  const tableUrl = 'mem://rewrite'
  const { resolver } = memResolver()
  let metadata = await icebergCreate({ tableUrl, resolver, schema, sortOrder })
  const batches = [
    [{ id: 5n, name: 'e' }, { id: 2n, name: 'b' }],
    [{ id: 1n, name: 'a' }, { id: 6n, name: 'f' }],
    [{ id: 4n, name: 'd' }, { id: 3n, name: 'c' }],
  ]
  for (const records of batches) {
    const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  return { tableUrl, resolver, metadata }
}

describe('icebergRewrite — round-trip and file consolidation', () => {
  it('merges files, preserves rows (modulo order), and globally sorts output', async () => {
    const { tableUrl, resolver, metadata } = await makeMultiFileTable({ sortOrder: sortById })

    const before = await icebergRead({ tableUrl, metadata, resolver })
    const beforeManifests = await icebergManifests({ metadata, resolver })
    expect(splitManifestEntries(beforeManifests).dataEntries.length).toBe(3)

    const staged = await icebergStageRewrite({ tableUrl, metadata, resolver })
    const after = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })

    const rows = await icebergRead({ tableUrl, metadata: after, resolver })
    // Same multiset.
    expect(byId(rows)).toEqual(byId(before))
    // Output is globally sorted by id and consolidated into a single file.
    expect(rows.map(r => r.id)).toEqual([1n, 2n, 3n, 4n, 5n, 6n])
    const afterEntries = splitManifestEntries(await icebergManifests({ metadata: after, resolver })).dataEntries
    expect(afterEntries.length).toBe(1)
    expect(afterEntries[0].data_file.sort_order_id).toBe(1)
  })

  it('produces non-overlapping sort-key bounds across split output files', async () => {
    const { tableUrl, resolver, metadata } = await makeMultiFileTable({ sortOrder: sortById })

    const staged = await icebergStageRewrite({ tableUrl, metadata, resolver, targetFileRows: 2 })
    const after = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })

    const entries = splitManifestEntries(await icebergManifests({ metadata: after, resolver })).dataEntries
    expect(entries.length).toBe(3)
    const ranges = entries
      .map(e => ({ lo: longBound(e, 1, 'lower'), hi: longBound(e, 1, 'upper') }))
      .sort((a, b) => Number(a.lo - b.lo))
    expect(ranges).toEqual([
      { lo: 1n, hi: 2n }, { lo: 3n, hi: 4n }, { lo: 5n, hi: 6n },
    ])
    // Strictly non-overlapping: each file's max < the next file's min.
    for (let i = 0; i + 1 < ranges.length; i++) {
      expect(ranges[i].hi < ranges[i + 1].lo).toBe(true)
    }
  })
})

describe('icebergRewrite — deletes consumed', () => {
  it('applies deletes during rewrite and leaves no delete files', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://rewrite-del'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    const records = [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }, { id: 3n, name: 'c' }]
    const appended = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })
    const dataPath = appended.writtenFiles[0]

    const delStaged = await icebergStagePositionDelete({
      tableUrl, metadata: afterAppend, deletes: [{ file_path: dataPath, pos: 1n }], resolver,
    })
    const afterDelete = await fileCatalogCommit({ tableUrl, metadata: afterAppend, staged: delStaged, resolver })
    // Sanity: there is a delete file before rewrite.
    expect(splitManifestEntries(await icebergManifests({ metadata: afterDelete, resolver })).deleteEntries.length).toBe(1)

    const staged = await icebergStageRewrite({ tableUrl, metadata: afterDelete, resolver })
    const afterRewrite = await fileCatalogCommit({ tableUrl, metadata: afterDelete, staged, resolver })

    const rows = await icebergRead({ tableUrl, metadata: afterRewrite, resolver })
    expect(rows).toEqual([{ id: 1n, name: 'a' }, { id: 3n, name: 'c' }])
    const { dataEntries, deleteEntries } = splitManifestEntries(await icebergManifests({ metadata: afterRewrite, resolver }))
    expect(deleteEntries.length).toBe(0)
    expect(dataEntries.length).toBe(1)
  })
})

describe('icebergRewrite — partition spec evolution', () => {
  it('rewrites unpartitioned data under a new identity spec', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://rewrite-evolve'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    const records = [
      { id: 1n, name: 'x' }, { id: 2n, name: 'y' }, { id: 3n, name: 'x' }, { id: 4n, name: 'y' },
    ]
    const appended = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })
    expect(splitManifestEntries(await icebergManifests({ metadata: afterAppend, resolver })).dataEntries.length).toBe(1)

    // Add a second partition spec: identity(name).
    /** @type {TableMetadata} */
    const evolved = {
      ...afterAppend,
      'partition-specs': [
        ...afterAppend['partition-specs'],
        { 'spec-id': 1, fields: [{ 'source-id': 2, 'field-id': 1000, name: 'name', transform: 'identity' }] },
      ],
      'last-partition-id': 1000,
    }

    const staged = await icebergStageRewrite({ tableUrl, metadata: evolved, resolver, partitionSpecId: 1 })
    const after = await fileCatalogCommit({ tableUrl, metadata: evolved, staged, resolver })

    const entries = splitManifestEntries(await icebergManifests({ metadata: after, resolver })).dataEntries
    // One file per distinct name value.
    expect(entries.length).toBe(2)
    expect(entries.every(e => e.partition_spec_id === 1)).toBe(true)
    expect(entries.map(e => e.data_file.partition.name).sort()).toEqual(['x', 'y'])
    const rows = await icebergRead({ tableUrl, metadata: after, resolver })
    expect([...rows].sort((a, b) => Number(a.id - b.id))).toEqual(records)
  })
})

describe('icebergRewrite — safety', () => {
  it('throws on a concurrent commit rather than dropping rows', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://rewrite-conflict'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    const a1 = await icebergStageAppend({ tableUrl, metadata: created, records: [{ id: 1n, name: 'a' }], resolver })
    const m1 = await fileCatalogCommit({ tableUrl, metadata: created, staged: a1, resolver })

    // Stage a rewrite against m1.
    const rewriteStaged = await icebergStageRewrite({ tableUrl, metadata: m1, resolver })

    // A concurrent append commits first, advancing main.
    const a2 = await icebergStageAppend({ tableUrl, metadata: m1, records: [{ id: 2n, name: 'b' }], resolver })
    const m2 = await fileCatalogCommit({ tableUrl, metadata: m1, staged: a2, resolver })

    // Committing the stale rewrite must fail (CAS on main), not clobber m2.
    await expect(fileCatalogCommit({ tableUrl, metadata: m2, staged: rewriteStaged, resolver }))
      .rejects.toThrow(/ref main expected snapshot/)
    // The concurrently-appended row is still present.
    const rows = await icebergRead({ tableUrl, metadata: m2, resolver })
    expect(rows.map(r => r.id).sort()).toEqual([1n, 2n])
  })

  it('rejects format-version 3 (row lineage not yet handled)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://rewrite-v3'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })
    const appended = await icebergStageAppend({ tableUrl, metadata: created, records: [{ id: 1n, name: 'a' }], resolver })
    const afterAppend = await fileCatalogCommit({ tableUrl, metadata: created, staged: appended, resolver })

    await expect(() => icebergStageRewrite({ tableUrl, metadata: afterAppend, resolver }))
      .rejects.toThrow(/format-version 2 only/)
  })
})

describe('icebergRewrite — one-call API', () => {
  it('loads, rewrites, and commits through a file catalog', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://rewrite-onecall'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreate({ tableUrl, resolver, schema, sortOrder: sortById })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 3n, name: 'c' }, { id: 1n, name: 'a' }] })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] })

    const committed = await icebergRewrite({ catalog, tableUrl })
    const rows = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(rows.map(r => r.id)).toEqual([1n, 2n, 3n])
    const entries = splitManifestEntries(await icebergManifests({ metadata: committed, resolver })).dataEntries
    expect(entries.length).toBe(1)
  })
})
