import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { fetchAvroRecords } from '../../src/fetch.js'
import { icebergManifests } from '../../src/manifest.js'
import { loadLatestFileCatalogMetadata } from '../../src/metadata.js'
import { icebergRead } from '../../src/read.js'
import { deserializeValue } from '../../src/write/serde.js'
import { prepareRewriteManifests, stageSnapshotForRewriteManifests } from '../../src/write/rewrite-manifests.js'
import { icebergAppend, icebergCreateTable, icebergDelete, icebergRewriteManifests } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Manifest, PartitionSpec, Schema, TableMetadata} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'created', required: false, type: 'timestamptz' },
  ],
}

/** @type {PartitionSpec} */
const daySpec = {
  'spec-id': 0,
  fields: [{ 'source-id': 2, 'field-id': 1000, name: 'created_day', transform: 'day' }],
}

const DAY = 86400000

/**
 * @param {TableMetadata} metadata
 * @param {ReturnType<typeof memResolver>['resolver']} resolver
 * @returns {Promise<Manifest[]>}
 */
async function manifestList(metadata, resolver) {
  const snap = metadata.snapshots?.find(s => s['snapshot-id'] === metadata['current-snapshot-id'])
  if (!snap?.['manifest-list']) throw new Error('no manifest list')
  return /** @type {Manifest[]} */ (await fetchAvroRecords(snap['manifest-list'], resolver))
}

/**
 * @param {Record<string, any>[]} rows
 * @returns {Record<string, any>[]}
 */
function byId(rows) {
  return [...rows].sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0)
}

/**
 * A table with one single-row manifest per commit, appended in
 * scrambled day order so manifests are not clustered by partition.
 *
 * @param {number} commits
 * @param {number} days
 * @param {Record<string, string>} [properties]
 * @returns {Promise<{ resolver: ReturnType<typeof memResolver>['resolver'], lister: ReturnType<typeof memResolver>['lister'], catalog: ReturnType<typeof fileCatalog>, tableUrl: string, metadata: TableMetadata }>}
 */
async function scrambledTable(commits, days, properties) {
  const { resolver, lister } = memResolver()
  const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
  const tableUrl = `http://test/rm-${Math.random().toString(36).slice(2)}`
  await icebergCreateTable({
    catalog, tableUrl, schema, partitionSpec: daySpec,
    properties,
  })
  let metadata
  for (let i = 0; i < commits; i++) {
    const day = i * 37 % days
    metadata = await icebergAppend({
      catalog, tableUrl,
      records: [{ id: BigInt(i), created: new Date(day * DAY + i * 1000) }],
    })
  }
  return { resolver, lister, catalog, tableUrl, metadata: /** @type {TableMetadata} */ (metadata) }
}

describe('icebergRewriteManifests', () => {
  it('preserves UUID partition bounds when rewriting manifests', async () => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/uuid-rewrite'
    await icebergCreateTable({
      catalog, tableUrl,
      schema: {
        type: 'struct',
        'schema-id': 0,
        fields: [{ id: 1, name: 'id', required: true, type: 'uuid' }],
      },
      partitionSpec: {
        'spec-id': 0,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'id', transform: 'identity' }],
      },
    })
    const lower = '0a000000-0000-0000-0000-000000000000'
    const upper = '0b000000-0000-0000-0000-000000000000'
    await icebergAppend({ catalog, tableUrl, records: [{ id: upper }] })
    await icebergAppend({ catalog, tableUrl, records: [{ id: lower }] })
    const metadata = await icebergRewriteManifests({ catalog, tableUrl })
    const manifests = await icebergManifests({ metadata, resolver })
    expect(manifests).toHaveLength(1)
    expect(manifests[0].entries.map(e => e.data_file.partition.id)).toEqual([lower, upper])
    const list = await manifestList(metadata, resolver)
    expect(list).toHaveLength(1)
    const lowerBytes = new Uint8Array(16)
    lowerBytes[0] = 0x0a
    const upperBytes = new Uint8Array(16)
    upperBytes[0] = 0x0b
    expect(list[0].partitions).toEqual([{
      contains_null: false,
      contains_nan: undefined,
      lower_bound: lowerBytes,
      upper_bound: upperBytes,
    }])
  })

  it('rewrites ~1000 single-file manifests into target-size manifests sorted by partition', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { resolver, lister, catalog, tableUrl, metadata: before } = await scrambledTable(1000, 100)
    const beforeList = await manifestList(before, resolver)
    expect(beforeList).toHaveLength(1000)
    const beforeEntries = new Map((await icebergManifests({ metadata: before, resolver }))
      .flatMap(m => m.entries).map(e => [e.data_file.file_path, e]))
    const beforeRows = await icebergRead({ tableUrl, metadata: before, resolver })

    const totalBytes = beforeList.reduce((sum, m) => sum + Number(m.manifest_length), 0)
    const targetSizeBytes = Math.ceil(totalBytes / 7.5)
    const after = await icebergRewriteManifests({ catalog, tableUrl, targetSizeBytes })
    const list = await manifestList(after, resolver)
    expect(list).toHaveLength(Math.ceil(totalBytes / targetSizeBytes))

    const snap = after.snapshots?.find(s => s['snapshot-id'] === after['current-snapshot-id'])
    expect(snap?.summary).toMatchObject({
      operation: 'replace',
      'manifests-created': '8',
      'manifests-replaced': '1000',
      'manifests-kept': '0',
      'entries-processed': '1000',
      'total-records': '1000',
      'total-data-files': '1000',
    })

    // Partition ranges ascend across manifests and summaries are exact.
    const manifests = await icebergManifests({ metadata: after, resolver })
    let prevUpper = -Infinity
    for (let i = 0; i < list.length; i++) {
      const manifest = list[i]
      const { entries } = manifests[i]
      const days = entries.map(e => Number(e.data_file.partition.created_day))
      const [summary] = manifest.partitions ?? []
      const lo = Number(deserializeValue(/** @type {Uint8Array} */ (summary.lower_bound), 'int'))
      const hi = Number(deserializeValue(/** @type {Uint8Array} */ (summary.upper_bound), 'int'))
      expect(lo).toBe(Math.min(...days))
      expect(hi).toBe(Math.max(...days))
      expect(summary.contains_null).toBe(false)
      expect(lo).toBeGreaterThanOrEqual(prevUpper)
      prevUpper = hi
      expect(manifest.existing_files_count).toBe(entries.length)
      expect(manifest.added_files_count).toBe(0)
      for (const entry of entries) {
        const original = beforeEntries.get(entry.data_file.file_path)
        expect(entry.status).toBe(0)
        expect(entry.snapshot_id).toBe(original?.snapshot_id)
        expect(entry.sequence_number).toBe(original?.sequence_number)
        expect(entry.file_sequence_number).toBe(original?.file_sequence_number)
      }
    }

    const { metadata: latest } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(byId(await icebergRead({ tableUrl, metadata: latest, resolver }))).toEqual(byId(beforeRows))
  }, 60000)

  it('is a no-op on a table with a single manifest', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { catalog, tableUrl, metadata } = await scrambledTable(1, 1)
    const out = await icebergRewriteManifests({ catalog, tableUrl })
    expect(out['current-snapshot-id']).toBe(metadata['current-snapshot-id'])
  })

  it('keeps position deletes applicable', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { resolver, lister, catalog, tableUrl } = await scrambledTable(20, 5)
    await icebergAppend({
      catalog, tableUrl,
      records: [{ id: 100n, created: new Date(0) }, { id: 101n, created: new Date(1000) }],
    })
    let { metadata } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const entries = (await icebergManifests({ metadata, resolver })).flatMap(m => m.entries)
    const twoRow = entries.find(e => e.data_file.record_count === 2n)
    if (!twoRow) throw new Error('two-row file not found')
    await icebergDelete({ catalog, tableUrl, deletes: [{ file_path: twoRow.data_file.file_path, pos: 0n }] })
    ;({ metadata } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister }))
    const beforeRows = await icebergRead({ tableUrl, metadata, resolver })
    expect(beforeRows.some(r => r.id === 100n)).toBe(false)

    const after = await icebergRewriteManifests({ catalog, tableUrl, targetSizeBytes: 4096 })
    const list = await manifestList(after, resolver)
    expect(list.filter(m => m.content === 1)).toHaveLength(1)
    expect(byId(await icebergRead({ tableUrl, metadata: after, resolver }))).toEqual(byId(beforeRows))
  })

  it('keeps v3 row ids and next-row-id', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/rm-v3'
    await icebergCreateTable({
      catalog, tableUrl, schema, partitionSpec: daySpec, formatVersion: 3,
    })
    for (let i = 0; i < 6; i++) {
      await icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i), created: new Date((5 - i) * DAY) }] })
    }
    const { metadata: before } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const beforeRows = await icebergRead({ tableUrl, metadata: before, resolver })
    const after = await icebergRewriteManifests({ catalog, tableUrl })
    expect(await manifestList(after, resolver)).toHaveLength(1)
    expect(after['next-row-id']).toBe(before['next-row-id'])
    expect(byId(await icebergRead({ tableUrl, metadata: after, resolver }))).toEqual(byId(beforeRows))
  })

  it('carries manifests committed concurrently', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { resolver, lister, catalog, tableUrl } = await scrambledTable(10, 3, {
      'commit.retry.min-wait-ms': '0',
      'commit.retry.max-wait-ms': '0',
    })
    await Promise.all([
      icebergRewriteManifests({ catalog, tableUrl }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 50n, created: new Date(0) }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 51n, created: new Date(DAY) }] }),
    ])
    const { metadata } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const rows = await icebergRead({ tableUrl, metadata, resolver })
    expect(rows.map(r => r.id).sort((a, b) => Number(a - b))).toEqual([...Array.from({ length: 10 }, (_, i) => BigInt(i)), 50n, 51n])
  })

  it('re-plans when a replaced manifest disappears', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { resolver, lister, catalog, tableUrl, metadata } = await scrambledTable(4, 2)
    const prepared = await prepareRewriteManifests({ tableUrl, metadata, resolver })
    if (!prepared) throw new Error('expected a rewrite')
    // Another rewrite lands first, replacing every manifest this one replaces.
    await icebergRewriteManifests({ catalog, tableUrl })
    const { metadata: fresh } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(await stageSnapshotForRewriteManifests({ tableUrl, metadata: fresh, prepared, resolver })).toBeUndefined()
  })

  it('rejects specs it cannot rewrite losslessly', async () => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister })
    const tableUrl = 'http://test/rm-identity-ts'
    await icebergCreateTable({
      catalog, tableUrl, schema,
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 2, 'field-id': 1000, name: 'created', transform: 'identity' }] },
    })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, created: new Date(0) }] })
    await expect(icebergRewriteManifests({ catalog, tableUrl })).rejects.toThrow(/losslessly/)
  })
})
