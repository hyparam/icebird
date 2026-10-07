import { describe, expect, it, vi } from 'vitest'
import { avroRead } from '../../src/avro/avro.read.js'
import { avroWrite } from '../../src/avro/avro.write.js'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { fileCatalog } from '../../src/catalog/file.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { fetchAvroRecords } from '../../src/fetch.js'
import { icebergManifests } from '../../src/manifest.js'
import { icebergRead } from '../../src/read.js'
import { loadLatestFileCatalogMetadata } from '../../src/metadata.js'
import { writeCarriedManifest } from '../../src/write/manifest.js'
import { manifestMergeConfig, mergeManifests, packManifests } from '../../src/write/merge.js'
import { icebergAppend, icebergCreateTable, icebergDelete, icebergRewrite, icebergUpdateSchema } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {AvroRecord} from '../../src/avro/types.js'
 * @import {Manifest, ManifestEntry, PartitionSpec, Resolver, Schema, TableMetadata} from '../../src/types.js'
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
 * @param {TableMetadata} metadata
 * @param {Resolver} resolver
 * @returns {Promise<Manifest[]>}
 */
async function manifestList(metadata, resolver) {
  const snap = metadata.snapshots?.find(s => s['snapshot-id'] === metadata['current-snapshot-id'])
  if (!snap?.['manifest-list']) throw new Error('no manifest list')
  return /** @type {Manifest[]} */ (await fetchAvroRecords(snap['manifest-list'], resolver))
}

/**
 * @param {Record<string, string>} [properties]
 * @param {2|3} [formatVersion]
 * @returns {Promise<ReturnType<typeof memResolver> & { catalog: ReturnType<typeof fileCatalog>, tableUrl: string }>}
 */
async function setup(properties, formatVersion) {
  const { resolver, files, lister } = memResolver()
  const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
  const tableUrl = `http://test/merge-${Math.random().toString(36).slice(2)}`
  await icebergCreateTable({ catalog, tableUrl, schema, properties, formatVersion })
  return { resolver, files, lister, catalog, tableUrl }
}

/**
 * @param {Record<string, any>[]} rows
 * @returns {bigint[]}
 */
function ids(rows) {
  return rows.map(r => r.id).sort((a, b) => a < b ? -1 : a > b ? 1 : 0)
}

/**
 * Replace a single-manifest snapshot with a differently encoded manifest.
 * Inline locations let the reader recover its new length from the file.
 *
 * @param {TableMetadata} metadata
 * @param {Resolver} resolver
 * @param {(schema: AvroRecord, entries: any[]) => any[]} transform
 * @returns {Promise<TableMetadata>}
 */
async function replaceSnapshotManifest(metadata, resolver, transform) {
  const [manifest] = await manifestList(metadata, resolver)
  const file = await resolver.reader(manifest.manifest_path)
  const reader = { view: new DataView(await file.slice(0, file.byteLength)), offset: 0 }
  const header = avroMetadata(reader)
  const entries = await avroRead({ reader, ...header })
  const avroSchema = header.metadata['avro.schema']
  const records = transform(avroSchema, entries)
  const path = `${metadata.location}/metadata/reencoded.avro`
  const writer = resolver.writer?.(path)
  if (!writer) throw new Error('writer required')
  await avroWrite({ writer, schema: avroSchema, records, metadata: { 'partition-spec-id': '0' } })
  const snapshot = metadata.snapshots?.find(s => s['snapshot-id'] === metadata['current-snapshot-id'])
  if (!snapshot) throw new Error('snapshot required')
  return await fileCatalogCommit({
    tableUrl: metadata.location, resolver,
    metadata: { ...metadata, snapshots: [{ ...snapshot, 'manifest-list': '', manifests: [path] }] },
    staged: { requirements: [], updates: [], writtenFiles: [] },
  })
}

describe('manifestMergeConfig', () => {
  it('defaults to Java: enabled, 100 manifests, 8 MB', () => {
    expect(manifestMergeConfig(undefined)).toEqual({ enabled: true, minCountToMerge: 100, targetSizeBytes: 8388608 })
  })

  it('reads the commit.manifest* properties', () => {
    expect(manifestMergeConfig({
      'commit.manifest-merge.enabled': 'false',
      'commit.manifest.min-count-to-merge': '5',
      'commit.manifest.target-size-bytes': '1024',
    })).toEqual({ enabled: false, minCountToMerge: 5, targetSizeBytes: 1024 })
  })

  it('ignores garbage values', () => {
    expect(manifestMergeConfig({
      'commit.manifest.min-count-to-merge': 'lots',
      'commit.manifest.target-size-bytes': '-1',
    })).toEqual({ enabled: true, minCountToMerge: 100, targetSizeBytes: 8388608 })
  })
})

describe('packManifests', () => {
  /**
   * @param {number} len
   * @returns {Manifest}
   */
  function m(len) {
    return /** @type {Manifest} */ ({ manifest_length: BigInt(len) })
  }

  it('packs greedily in order, leaving the newest bin under-filled', () => {
    const list = [m(4), m(4), m(4), m(4), m(1)]
    const bins = packManifests(list, 10)
    expect(bins.map(b => b.length)).toEqual([2, 3])
    expect(bins[1][2]).toBe(list[4])
  })

  it('gives an oversized manifest its own bin', () => {
    expect(packManifests([m(3), m(20), m(3)], 10).map(b => b.length)).toEqual([1, 1, 1])
  })
})

describe('merge on commit', () => {
  it.each([true, false])('preserves position deletes after int-to-long partition promotion (merge=%s)', async merge => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/merge-promoted-partition'
    /** @type {Schema} */
    const originalSchema = {
      ...schema,
      fields: [...schema.fields, { id: 3, name: 'category', required: false, type: 'int' }],
    }
    await icebergCreateTable({
      catalog, tableUrl, schema: originalSchema,
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 3, 'field-id': 1000, name: 'category', transform: 'identity' }] },
      properties: {
        'commit.manifest-merge.enabled': String(merge),
        'commit.manifest.min-count-to-merge': '2',
      },
    })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'deleted', category: 7 }] })
    const [{ entries }] = await icebergManifests({ metadata: before, resolver })
    const deleted = await icebergDelete({
      catalog, tableUrl, deletes: [{ file_path: entries[0].data_file.file_path, pos: 0n }],
    })
    expect(await icebergRead({ tableUrl, metadata: deleted, resolver })).toEqual([])
    const [deleteManifest] = (await manifestList(deleted, resolver)).filter(m => m.content === 1)
    await icebergUpdateSchema({
      catalog, tableUrl,
      schema: { ...originalSchema, fields: [...schema.fields, { id: 3, name: 'category', required: false, type: 'long' }] },
    })
    const after = await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'kept', category: 7n }] })
    const list = await manifestList(after, resolver)
    expect(list.filter(m => m.content === 0)).toHaveLength(merge ? 1 : 2)
    expect(list.filter(m => m.content === 1)).toEqual([deleteManifest])
    const afterEntries = (await icebergManifests({ metadata: after, resolver })).flatMap(m => m.entries)
    expect(afterEntries.find(e => e.data_file.file_path === entries[0].data_file.file_path)?.data_file.partition.category)
      .toBe(merge ? 7n : 7)
    expect(afterEntries.find(e => e.data_file.content === 1)?.data_file.partition.category).toBe(7)
    expect(await icebergRead({ tableUrl, metadata: after, resolver })).toEqual([{ id: 2n, name: 'kept', category: 7n }])
  })

  it.each([-1, 0, 20000, null])('merges date-encoded day partitions (%s)', async day => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/date-encoded-merge'
    await icebergCreateTable({
      catalog, tableUrl,
      schema: { ...schema, fields: [schema.fields[0], { id: 2, name: 'created', type: 'timestamp', required: false }] },
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 2, 'field-id': 1000, name: 'created_day', transform: 'day' }] },
      properties: { 'commit.manifest.min-count-to-merge': '2' },
    })
    const records = [{ id: 1n, created: day === null ? null : new Date(day * 86400000) }]
    const before = await icebergAppend({ catalog, tableUrl, records })
    await replaceSnapshotManifest(before, resolver, (avroSchema, entries) => {
      const dataFile = /** @type {AvroRecord} */ (avroSchema.fields.find(f => f.name === 'data_file')?.type)
      const partition = /** @type {AvroRecord} */ (dataFile.fields.find(f => f.name === 'partition')?.type)
      partition.fields[0].type = ['null', { type: 'int', logicalType: 'date' }]
      return entries
    })
    const after = await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, created: new Date(86400000) }] })
    const list = await manifestList(after, resolver)
    expect(list).toHaveLength(1)
    const [{ entries }] = await icebergManifests({ metadata: after, resolver })
    expect(entries.map(e => e.data_file.partition.created_day)).toEqual([day ?? undefined, 1])
    const [summary] = list[0].partitions ?? []
    expect(summary.contains_null).toBe(day === null)
    if (!summary.lower_bound || !summary.upper_bound) throw new Error('partition bounds required')
    expect(new DataView(summary.lower_bound.buffer, summary.lower_bound.byteOffset).getInt32(0, true)).toBe(Math.min(day ?? 1, 1))
    expect(new DataView(summary.upper_bound.buffer, summary.upper_bound.byteOffset).getInt32(0, true)).toBe(Math.max(day ?? 1, 1))
    expect(ids(await icebergRead({ tableUrl, metadata: after, resolver }))).toEqual([1n, 2n])
  })

  it.each([0, 1, 2])('merges upgraded v1 manifests with entry status %s', async status => {
    const { catalog, tableUrl, resolver } = await setup({ 'commit.manifest.min-count-to-merge': '2' })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'old' }] })
    await replaceSnapshotManifest(before, resolver, (avroSchema, entries) => {
      avroSchema.fields = avroSchema.fields.filter(f => !['sequence_number', 'file_sequence_number'].includes(f.name))
      const dataFile = /** @type {AvroRecord} */ (avroSchema.fields.find(f => f.name === 'data_file')?.type)
      dataFile.fields = dataFile.fields.filter(f => f.name !== 'content')
      return entries.map(entry => ({ ...entry, status }))
    })
    const after = await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'new' }] })
    const list = await manifestList(after, resolver)
    expect(list).toHaveLength(1)
    expect(list[0].min_sequence_number).toBe(status === 2 ? 2n : 0n)
    const [{ entries }] = await icebergManifests({ metadata: after, resolver })
    expect(entries).toHaveLength(status === 2 ? 1 : 2)
    if (status !== 2) {
      expect(entries[0]).toMatchObject({
        status: 0, snapshot_id: BigInt(before['current-snapshot-id'] ?? -1),
        sequence_number: 0n, file_sequence_number: 0n,
        data_file: { content: 0 },
      })
    }
    expect(entries.at(-1)).toMatchObject({ status: 1, sequence_number: 2n, file_sequence_number: 2n })
    expect(ids(await icebergRead({ tableUrl, metadata: after, resolver }))).toEqual(status === 2 ? [2n] : [1n, 2n])
  })

  it('bounds manifest count at the Java defaults and reads like fast append', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const merged = await setup()
    const fast = await setup({ 'commit.manifest-merge.enabled': 'false' })
    const N = 105
    let maxManifests = 0
    for (let i = 0; i < N; i++) {
      const records = [{ id: BigInt(i), name: `r${i}` }]
      const md = await icebergAppend({ catalog: merged.catalog, tableUrl: merged.tableUrl, records })
      await icebergAppend({ catalog: fast.catalog, tableUrl: fast.tableUrl, records })
      const count = (await manifestList(md, merged.resolver)).length
      maxManifests = Math.max(maxManifests, count)
      // The 100th commit finds 100 manifests in the newest bin and merges.
      if (i === 99) expect(count).toBe(1)
    }
    expect(maxManifests).toBe(99)

    const mergedMd = (await loadLatestFileCatalogMetadata({ tableUrl: merged.tableUrl, resolver: merged.resolver, lister: merged.lister })).metadata
    const fastMd = (await loadLatestFileCatalogMetadata({ tableUrl: fast.tableUrl, resolver: fast.resolver, lister: fast.lister })).metadata
    expect((await manifestList(mergedMd, merged.resolver)).length).toBe(6)
    expect((await manifestList(fastMd, fast.resolver)).length).toBe(N)

    const mergedRows = await icebergRead({ tableUrl: merged.tableUrl, metadata: mergedMd, resolver: merged.resolver })
    const fastRows = await icebergRead({ tableUrl: fast.tableUrl, metadata: fastMd, resolver: fast.resolver })
    expect(mergedRows).toEqual(fastRows)
    expect(ids(mergedRows)).toEqual(Array.from({ length: N }, (_, i) => BigInt(i)))
  })

  it('keeps snapshot ids and sequence numbers, marking only own adds as ADDED', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { catalog, tableUrl, resolver } = await setup({ 'commit.manifest.min-count-to-merge': '3' })
    /** @type {TableMetadata[]} */
    const history = []
    for (let i = 0; i < 3; i++) {
      history.push(await icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i), name: `r${i}` }] }))
    }
    const md = history[2]
    const list = await manifestList(md, resolver)
    expect(list).toHaveLength(1)
    const [merged] = list
    expect(merged.added_snapshot_id).toBe(BigInt(md['current-snapshot-id'] ?? -1))
    expect(merged.sequence_number).toBe(3n)
    expect(merged.min_sequence_number).toBe(1n)
    expect(merged.added_files_count).toBe(1)
    expect(merged.existing_files_count).toBe(2)
    expect(merged.added_rows_count).toBe(1n)
    expect(merged.existing_rows_count).toBe(2n)

    const [{ entries }] = await icebergManifests({ metadata: md, resolver })
    const byId = entries.map(e => ({
      status: e.status,
      snapshot: e.snapshot_id,
      seq: e.sequence_number,
      fileSeq: e.file_sequence_number,
    }))
    expect(byId).toEqual(history.map((h, i) => ({
      status: i === 2 ? 1 : 0,
      snapshot: BigInt(h['current-snapshot-id'] ?? -1),
      seq: BigInt(i + 1),
      fileSeq: BigInt(i + 1),
    })))
  })

  it('does not merge when disabled', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { catalog, tableUrl, resolver } = await setup({
      'commit.manifest-merge.enabled': 'false',
      'commit.manifest.min-count-to-merge': '2',
    })
    let md
    for (let i = 0; i < 4; i++) {
      md = await icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i), name: 'x' }] })
    }
    expect(await manifestList(/** @type {TableMetadata} */ (md), resolver)).toHaveLength(4)
  })

  it('merges full bins even below min count, keeping the newest bin', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    // Tiny target: every manifest exceeds half the target, so bins hold one or
    // two manifests and only the newest bin is exempt from merging.
    const { catalog, tableUrl, resolver, files } = await setup({})
    let md
    for (let i = 0; i < 3; i++) {
      md = await icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i), name: 'x' }] })
    }
    const list = await manifestList(/** @type {TableMetadata} */ (md), resolver)
    const size = Number(list[0].manifest_length)
    const { metadata } = await loadLatestFileCatalogMetadata({ tableUrl, resolver })
    metadata.properties = { ...metadata.properties, 'commit.manifest.target-size-bytes': String(size * 2 + 1) }
    files.set(`${tableUrl}/metadata/v4.metadata.json`, new TextEncoder().encode(JSON.stringify(metadata)))
    // [m1 m2] [m3 m4]: the first bin merges, the newest bin stays.
    md = await icebergAppend({ catalog, tableUrl, records: [{ id: 3n, name: 'x' }] })
    const after = await manifestList(md, resolver)
    expect(after.map(m => m.existing_files_count + m.added_files_count)).toEqual([2, 1, 1])
    expect(ids(await icebergRead({ tableUrl, metadata: md, resolver }))).toEqual([0n, 1n, 2n, 3n])
  })

  it('merges a historical spec whose source column was dropped', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/merge-dropped-source'
    /** @type {Schema} */
    const originalSchema = {
      ...schema,
      fields: [schema.fields[0], { id: 3, name: 'category', required: false, type: 'string' }],
    }
    await icebergCreateTable({
      catalog, tableUrl, schema: originalSchema,
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 3, 'field-id': 1000, name: 'category', transform: 'identity' }] },
    })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, category: 'zebra' }] })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, category: 'apple' }] })
    await fileCatalogCommit({
      tableUrl, metadata: before, resolver, conditionalCommits: true,
      staged: {
        requirements: [], writtenFiles: [],
        updates: [
          { action: 'add-spec', spec: { 'spec-id': 1, fields: [] } },
          { action: 'set-default-spec', 'spec-id': 1 },
        ],
      },
    })
    await icebergUpdateSchema({ catalog, tableUrl, schema: { ...schema, fields: [schema.fields[0]] } })
    // The newest manifest is in spec 1, so spec 0's two manifests merge now.
    const after = await icebergAppend({ catalog, tableUrl, records: [{ id: 3n }] })
    const list = await manifestList(after, resolver)
    expect(list.map(m => [m.partition_spec_id, m.existing_files_count + m.added_files_count])).toEqual([[0, 2], [1, 1]])
    const file = await resolver.reader(list[0].manifest_path)
    const header = avroMetadata({ view: new DataView(await file.slice(0, file.byteLength)), offset: 0 })
    expect(header.metadata.schema).toEqual(originalSchema)
    const entries = (await icebergManifests({ metadata: after, resolver })).flatMap(m => m.entries)
    expect(entries.map(e => e.data_file.partition.category)).toEqual(['zebra', 'apple', undefined])
    expect(ids(await icebergRead({ tableUrl, metadata: after, resolver }))).toEqual([1n, 2n, 3n])
  })

  it('re-merges against the refreshed base when a concurrent commit wins', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { catalog, tableUrl, resolver, lister } = await setup({
      'commit.manifest.min-count-to-merge': '2',
      'commit.retry.min-wait-ms': '0',
      'commit.retry.max-wait-ms': '0',
    })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 0n, name: 'seed' }] })
    // Both writers stage a merge of [seed, own] against the same base. The
    // loser must re-merge against the winner's list, not reuse its own merge.
    const N = 8
    await Promise.all(Array.from({ length: N }, (_, i) =>
      icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i + 1), name: `w${i}` }] })))
    const { metadata } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const rows = await icebergRead({ tableUrl, metadata, resolver })
    expect(ids(rows)).toEqual(Array.from({ length: N + 1 }, (_, i) => BigInt(i)))
    expect(metadata.snapshots).toHaveLength(N + 1)
    expect(await manifestList(metadata, resolver)).toHaveLength(1)
  })

  it('keeps deletes applicable across merges (v2 position deletes)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const props = { 'commit.manifest.min-count-to-merge': '2' }
    const merged = await setup(props)
    const fast = await setup({ 'commit.manifest-merge.enabled': 'false' })
    for (const t of [merged, fast]) {
      for (let i = 0; i < 3; i++) {
        await icebergAppend({ catalog: t.catalog, tableUrl: t.tableUrl, records: [{ id: BigInt(2 * i), name: 'a' }, { id: BigInt(2 * i + 1), name: 'b' }] })
      }
    }
    for (const t of [merged, fast]) {
      const { metadata } = await loadLatestFileCatalogMetadata({ tableUrl: t.tableUrl, resolver: t.resolver, lister: t.lister })
      const entries = (await icebergManifests({ metadata, resolver: t.resolver })).flatMap(m => m.entries)
      const path = entries.find(e => e.sequence_number === 1n)?.data_file.file_path
      if (!path) throw new Error('file not found')
      // Delete row 0 of one file, twice so delete manifests merge too.
      await icebergDelete({ catalog: t.catalog, tableUrl: t.tableUrl, deletes: [{ file_path: path, pos: 0n }] })
      await icebergAppend({ catalog: t.catalog, tableUrl: t.tableUrl, records: [{ id: 100n, name: 'late' }] })
      await icebergDelete({ catalog: t.catalog, tableUrl: t.tableUrl, deletes: [{ file_path: path, pos: 1n }] })
      await icebergAppend({ catalog: t.catalog, tableUrl: t.tableUrl, records: [{ id: 101n, name: 'later' }] })
    }
    const mergedMd = (await loadLatestFileCatalogMetadata({ tableUrl: merged.tableUrl, resolver: merged.resolver, lister: merged.lister })).metadata
    const fastMd = (await loadLatestFileCatalogMetadata({ tableUrl: fast.tableUrl, resolver: fast.resolver, lister: fast.lister })).metadata
    const mergedList = await manifestList(mergedMd, merged.resolver)
    expect(mergedList.filter(m => m.content === 1)).toHaveLength(1)
    expect(mergedList.length).toBeLessThan((await manifestList(fastMd, fast.resolver)).length)
    const mergedRows = await icebergRead({ tableUrl: merged.tableUrl, metadata: mergedMd, resolver: merged.resolver })
    const fastRows = await icebergRead({ tableUrl: fast.tableUrl, metadata: fastMd, resolver: fast.resolver })
    expect(ids(mergedRows)).toEqual(ids(fastRows))
    expect(mergedRows).toHaveLength(6)
  })

  it('keeps v3 deletion vectors and row ids stable across merges', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const merged = await setup({ 'commit.manifest.min-count-to-merge': '2' }, 3)
    const fast = await setup({ 'commit.manifest-merge.enabled': 'false' }, 3)
    for (const t of [merged, fast]) {
      for (let i = 0; i < 3; i++) {
        await icebergAppend({ catalog: t.catalog, tableUrl: t.tableUrl, records: [{ id: BigInt(2 * i), name: 'a' }, { id: BigInt(2 * i + 1), name: 'b' }] })
      }
      const { metadata } = await loadLatestFileCatalogMetadata({ tableUrl: t.tableUrl, resolver: t.resolver, lister: t.lister })
      const entries = (await icebergManifests({ metadata, resolver: t.resolver })).flatMap(m => m.entries)
      const path = entries.find(e => e.sequence_number === 2n)?.data_file.file_path
      if (!path) throw new Error('file not found')
      await icebergDelete({ catalog: t.catalog, tableUrl: t.tableUrl, deletes: [{ file_path: path, pos: 1n }] })
      await icebergAppend({ catalog: t.catalog, tableUrl: t.tableUrl, records: [{ id: 100n, name: 'late' }] })
    }
    const mergedMd = (await loadLatestFileCatalogMetadata({ tableUrl: merged.tableUrl, resolver: merged.resolver, lister: merged.lister })).metadata
    const fastMd = (await loadLatestFileCatalogMetadata({ tableUrl: fast.tableUrl, resolver: fast.resolver, lister: fast.lister })).metadata
    expect(mergedMd['next-row-id']).toBe(fastMd['next-row-id'])
    const mergedRows = await icebergRead({ tableUrl: merged.tableUrl, metadata: mergedMd, resolver: merged.resolver })
    const fastRows = await icebergRead({ tableUrl: fast.tableUrl, metadata: fastMd, resolver: fast.resolver })
    expect(mergedRows).toEqual(fastRows)
    expect(ids(mergedRows)).toEqual([0n, 1n, 2n, 4n, 5n, 100n])
  })

  it('merges after a subset rewrite and reads the same rows', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { catalog, tableUrl, resolver, lister } = await setup({ 'commit.manifest.min-count-to-merge': '3' })
    for (let i = 0; i < 2; i++) {
      await icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i), name: 'x' }] })
    }
    let { metadata } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const [{ entries }] = await icebergManifests({ metadata, resolver })
    await icebergRewrite({ catalog, tableUrl, files: [entries[0].data_file.file_path] })
    ;({ metadata } = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister }))
    expect(ids(await icebergRead({ tableUrl, metadata, resolver }))).toEqual([0n, 1n])
  })
})

describe('mergeManifests', () => {
  it('drops DELETED tombstones of earlier snapshots and keeps its own', async () => {
    const { resolver } = memResolver()
    /** @type {PartitionSpec} */
    const spec = { 'spec-id': 0, fields: [] }
    /** @type {any} */
    const rawMetadata = {
      'format-version': 2,
      'current-schema-id': 0,
      schemas: [schema],
      'partition-specs': [spec],
    }
    /** @type {TableMetadata} */
    const metadata = rawMetadata
    /**
     * @param {string} path
     * @param {0|1|2} status
     * @param {bigint} snapshotId
     * @returns {ManifestEntry}
     */
    function entry(path, status, snapshotId) {
      return {
        status,
        snapshot_id: snapshotId,
        sequence_number: 1n,
        file_sequence_number: 1n,
        data_file: {
          content: 0, file_path: path, file_format: 'parquet', partition: {},
          record_count: 10n, file_size_in_bytes: 100n,
        },
      }
    }
    /**
     * @param {string} path
     * @param {ManifestEntry[]} entries
     * @param {bigint} snapshotId
     * @returns {Promise<Manifest>}
     */
    async function manifest(path, entries, snapshotId) {
      const writer = /** @type {NonNullable<Resolver['writer']>} */ (resolver.writer)(path)
      await writeCarriedManifest({ writer, schema, partitionSpec: spec, snapshotId, entries, content: 0 })
      return {
        manifest_path: path, manifest_length: BigInt(writer.offset), partition_spec_id: 0, content: 0,
        sequence_number: 1n, min_sequence_number: 1n, added_snapshot_id: snapshotId,
        added_files_count: 0, existing_files_count: 0, deleted_files_count: 0,
        added_rows_count: 0n, existing_rows_count: 0n, deleted_rows_count: 0n,
      }
    }
    const a = await manifest('mem://a.avro', [entry('old-deleted', 2, 1n), entry('kept', 0, 1n)], 1n)
    const b = await manifest('mem://b.avro', [entry('own-deleted', 2, 9n)], 9n)
    const { manifests, writtenFiles } = await mergeManifests({
      tableUrl: 'mem://t', metadata, resolver, manifests: [a, b],
      snapshotId: 9n, sequenceNumber: 2n, formatVersion: 2,
      config: { minCountToMerge: 1, targetSizeBytes: 1 << 20 },
    })
    expect(writtenFiles).toHaveLength(1)
    expect(manifests).toHaveLength(1)
    expect(manifests[0]).toMatchObject({
      existing_files_count: 1, deleted_files_count: 1, added_files_count: 0,
      existing_rows_count: 10n, deleted_rows_count: 10n, min_sequence_number: 1n,
    })
    const records = await fetchAvroRecords(manifests[0].manifest_path, resolver)
    expect(records.map(r => [r.status, r.data_file.file_path])).toEqual([[0, 'kept'], [2, 'own-deleted']])
  })
})
