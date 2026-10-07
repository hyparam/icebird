import { describe, expect, it, vi } from 'vitest'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { avroWrite } from '../../src/avro/avro.write.js'
import { fileCatalog } from '../../src/catalog/file.js'
import { fetchAvroRecords } from '../../src/fetch.js'
import { icebergManifests } from '../../src/manifest.js'
import { loadLatestFileCatalogMetadata } from '../../src/metadata.js'
import { icebergRead } from '../../src/read.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { deserializeValue } from '../../src/write/serde.js'
import { prepareRewriteManifests, stageSnapshotForRewriteManifests } from '../../src/write/rewrite-manifests.js'
import { icebergAppend, icebergCreateTable, icebergDelete, icebergRewriteManifests, icebergUpdateSchema } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Manifest, PartitionSpec, Resolver, Schema, TableMetadata} from '../../src/types.js'
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
  it.each(['identity', 'truncate[2]'])('rewrites a historical %s spec after its source column is dropped', async transform => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/rm-dropped-source'
    /** @type {Schema} */
    const originalSchema = {
      ...schema,
      fields: [schema.fields[0], { id: 2, name: 'category', required: false, type: 'string' }],
    }
    await icebergCreateTable({
      catalog, tableUrl, schema: originalSchema,
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 2, 'field-id': 1000, name: 'category', transform }] },
    })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, category: 'zebra' }] })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, category: 'apple' }] })
    const originalEntries = (await icebergManifests({ metadata: before, resolver })).flatMap(m => m.entries)
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
    const evolved = await icebergUpdateSchema({
      catalog, tableUrl,
      // Reusing the name with a new ID must not change the historical type.
      schema: { ...schema, fields: [schema.fields[0], { id: 3, name: 'category', required: false, type: 'int' }] },
    })
    const beforeRows = byId(await icebergRead({ tableUrl, metadata: evolved, resolver }))
    const after = await icebergRewriteManifests({ catalog, tableUrl, specId: 0 })
    expect(after['current-schema-id']).toBe(evolved['current-schema-id'])
    expect(after.schemas).toEqual(evolved.schemas)
    const manifests = await manifestList(after, resolver)
    expect(manifests).toHaveLength(1)
    const [manifest] = manifests
    expect(manifest.partition_spec_id).toBe(0)
    const file = await resolver.reader(manifest.manifest_path)
    const buffer = await file.slice(0, file.byteLength)
    const header = avroMetadata({ view: new DataView(buffer), offset: 0 })
    expect(header.metadata.schema).toEqual(originalSchema)
    const entries = (await icebergManifests({ metadata: after, resolver })).flatMap(m => m.entries)
    const sorted = [...originalEntries].sort((a, b) => String(a.data_file.partition.category).localeCompare(String(b.data_file.partition.category)))
    expect(entries).toEqual(sorted.map(entry => ({ ...entry, status: 0 })))
    expect(deserializeValue(/** @type {Uint8Array} */ (manifest.partitions?.[0].lower_bound), 'string'))
      .toBe(transform === 'identity' ? 'apple' : 'ap')
    expect(deserializeValue(/** @type {Uint8Array} */ (manifest.partitions?.[0].upper_bound), 'string'))
      .toBe(transform === 'identity' ? 'zebra' : 'ze')
    expect(byId(await icebergRead({ tableUrl, metadata: after, resolver }))).toEqual(beforeRows)
    await expect(prepareRewriteManifests({
      tableUrl, resolver, specId: 0,
      metadata: { ...evolved, schemas: evolved.schemas.filter(s => s['schema-id'] === evolved['current-schema-id']) },
    })).rejects.toThrow(/source fields not found in retained schemas/)
  })

  it('rewrites inherited v1 data manifests without a content column', async () => {
    const { resolver, catalog, tableUrl, metadata } = await scrambledTable(2, 2)
    const originalEntries = (await icebergManifests({ metadata, resolver })).flatMap(m => m.entries)
    const originalRows = byId(await icebergRead({ tableUrl, metadata, resolver }))
    const paths = []
    for (const [i, entry] of originalEntries.entries()) {
      const path = `${tableUrl}/metadata/v1-${i}.avro`
      const writer = resolver.writer?.(path)
      if (!writer) throw new Error('writer required')
      // V1 omits content and sequence columns entirely, rather than storing null.
      await avroWrite({
        writer,
        schema: {
          type: 'record', name: 'manifest_entry', fields: [
            { name: 'status', type: 'int', 'field-id': 0 },
            { name: 'snapshot_id', type: 'long', 'field-id': 1 },
            { name: 'data_file', 'field-id': 2, type: {
              type: 'record', name: 'r2', fields: [
                { name: 'file_path', type: 'string', 'field-id': 100 },
                { name: 'file_format', type: 'string', 'field-id': 101 },
                { name: 'partition', 'field-id': 102, type: {
                  type: 'record', name: 'r102', fields: [
                    { name: 'created_day', type: ['null', 'int'], 'field-id': 1000 },
                  ],
                } },
                { name: 'record_count', type: 'long', 'field-id': 103 },
                { name: 'file_size_in_bytes', type: 'long', 'field-id': 104 },
                { name: 'block_size_in_bytes', type: 'long', 'field-id': 105 },
              ],
            } },
          ],
        },
        records: [{ ...entry, data_file: { ...entry.data_file, block_size_in_bytes: 0n } }],
        metadata: { 'format-version': '1', 'partition-spec-id': '0' },
      })
      paths.push(path)
    }
    const snapshot = metadata.snapshots?.find(s => s['snapshot-id'] === metadata['current-snapshot-id'])
    if (!snapshot) throw new Error('snapshot required')
    const inherited = await fileCatalogCommit({
      tableUrl, resolver,
      metadata: { ...metadata, snapshots: [{ ...snapshot, 'manifest-list': '', manifests: paths }] },
      staged: { requirements: [], updates: [], writtenFiles: [] },
    })
    expect(inherited['format-version']).toBe(2)
    const inheritedEntries = (await icebergManifests({ metadata: inherited, resolver })).flatMap(m => m.entries)
    expect(inheritedEntries.map(e => e.data_file.content)).toEqual([undefined, undefined])

    const after = await icebergRewriteManifests({ catalog, tableUrl })
    const manifests = await icebergManifests({ metadata: after, resolver })
    expect(manifests).toHaveLength(1)
    expect(manifests[0].entries).toHaveLength(2)
    for (const entry of manifests[0].entries) {
      const original = originalEntries.find(e => e.data_file.file_path === entry.data_file.file_path)
      expect(entry).toMatchObject({
        status: 0,
        snapshot_id: original?.snapshot_id,
        sequence_number: 0n,
        file_sequence_number: 0n,
        data_file: { content: 0 },
      })
    }
    expect(byId(await icebergRead({ tableUrl, metadata: after, resolver }))).toEqual(originalRows)
  })

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

  it('preserves row ids assigned by a concurrent append after a v3 upgrade', async () => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/rm-upgrade-retry'
    await icebergCreateTable({
      catalog, tableUrl, schema, partitionSpec: daySpec,
      properties: { 'commit.retry.min-wait-ms': '0', 'commit.retry.max-wait-ms': '0' },
    })
    // The rewrite reverses these files, so stale inheritance would swap IDs.
    await icebergAppend({ catalog, tableUrl, records: [{ id: 10n, created: new Date(DAY) }] })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 20n, created: new Date(0) }] })
    const upgraded = await fileCatalogCommit({
      tableUrl, resolver, conditionalCommits: true,
      metadata: { ...before, 'format-version': 3, 'next-row-id': 0 },
      staged: { requirements: [], updates: [], writtenFiles: [] },
    })
    const originalManifests = await manifestList(upgraded, resolver)
    expect(originalManifests.map(m => m.first_row_id)).toEqual([undefined, undefined])

    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    /** @type {TableMetadata | undefined} */
    let appended
    let attempts = 0
    /** @type {Resolver} */
    const racingResolver = {
      ...resolver,
      writer(path, options) {
        const writer = realWriter(path, options)
        if (options?.ifNoneMatch === '*') {
          const finish = writer.finish.bind(writer)
          writer.finish = async () => {
            attempts++
            if (attempts === 1) {
              // Assign inherited row IDs after the rewrite has been staged,
              // then let its metadata commit encounter a real conflict.
              appended = await icebergAppend({
                catalog, tableUrl, records: [{ id: 30n, created: new Date(2 * DAY) }],
              })
            }
            await finish()
          }
        }
        return writer
      },
    }
    const after = await icebergRewriteManifests({
      catalog: fileCatalog({ resolver: racingResolver, lister, conditionalCommits: true }),
      tableUrl,
    })
    expect(attempts).toBe(2)
    if (!appended) throw new Error('expected concurrent append')
    const appendedManifests = await manifestList(appended, resolver)
    expect(appendedManifests.slice(0, 2).map(m => m.manifest_path))
      .toEqual(originalManifests.map(m => m.manifest_path))
    expect(appendedManifests.slice(0, 2).map(m => m.first_row_id)).toEqual([0n, 1n])
    const committedRows = byId(await icebergRead({ tableUrl, metadata: appended, resolver }))
    expect(committedRows.map(r => [r.id, r._row_id])).toEqual([[10n, 0n], [20n, 1n], [30n, 2n]])
    expect(byId(await icebergRead({ tableUrl, metadata: after, resolver }))).toEqual(committedRows)
    expect(after['next-row-id']).toBe(appended['next-row-id'])
  })

  it('reserves row ids again after a metadata-only conflict on an upgraded v3 table', async () => {
    const { resolver, lister, catalog, tableUrl, metadata } = await scrambledTable(2, 2, {
      'commit.retry.min-wait-ms': '0', 'commit.retry.max-wait-ms': '0',
    })
    await fileCatalogCommit({
      tableUrl, resolver, conditionalCommits: true,
      metadata: { ...metadata, 'format-version': 3, 'next-row-id': 0 },
      staged: { requirements: [], updates: [], writtenFiles: [] },
    })
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    let attempts = 0
    /** @type {string[]} */
    const manifestWrites = []
    /** @type {Resolver} */
    const racingResolver = {
      ...resolver,
      writer(path, options) {
        if (/-m\d+\.avro$/.test(path)) manifestWrites.push(path)
        const writer = realWriter(path, options)
        if (options?.ifNoneMatch === '*') {
          const finish = writer.finish.bind(writer)
          writer.finish = async () => {
            if (++attempts === 1) {
              await icebergUpdateSchema({
                catalog, tableUrl,
                schema: { ...schema, fields: [...schema.fields, { id: 3, name: 'extra', required: false, type: 'string' }] },
              })
            }
            await finish()
          }
        }
        return writer
      },
    }
    const after = await icebergRewriteManifests({
      catalog: fileCatalog({ resolver: racingResolver, lister, conditionalCommits: true }), tableUrl,
    })
    expect(attempts).toBe(2)
    expect(manifestWrites).toHaveLength(1)
    expect(after['current-schema-id']).toBe(1)
    expect(after['next-row-id']).toBe(2)
    const snapshot = after.snapshots?.find(s => s['snapshot-id'] === after['current-snapshot-id'])
    expect(snapshot).toMatchObject({ 'first-row-id': 0, 'added-rows': 2 })
    expect(byId(await icebergRead({ tableUrl, metadata: after, resolver })).map(r => r._row_id)).toEqual([0n, 1n])
    const appended = await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, created: new Date(2 * DAY) }] })
    expect(appended['next-row-id']).toBe(3)
    expect(byId(await icebergRead({ tableUrl, metadata: appended, resolver })).map(r => r._row_id)).toEqual([0n, 1n, 2n])
  })

  it('reprepares v3 manifests when a concurrent format upgrade causes a retry', async () => {
    const { resolver, lister, tableUrl, metadata } = await scrambledTable(2, 2, {
      'commit.retry.min-wait-ms': '0', 'commit.retry.max-wait-ms': '0',
    })
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    let attempts = 0
    /** @type {string[]} */
    const manifestWrites = []
    /** @type {Resolver} */
    const racingResolver = {
      ...resolver,
      writer(path, options) {
        if (/-m\d+\.avro$/.test(path)) manifestWrites.push(path)
        const writer = realWriter(path, options)
        if (options?.ifNoneMatch === '*') {
          const finish = writer.finish.bind(writer)
          writer.finish = async () => {
            if (++attempts === 1) {
              await fileCatalogCommit({
                tableUrl, resolver, conditionalCommits: true,
                metadata: { ...metadata, 'format-version': 3, 'next-row-id': 0 },
                staged: { requirements: [], updates: [], writtenFiles: [] },
              })
            }
            await finish()
          }
        }
        return writer
      },
    }
    const after = await icebergRewriteManifests({
      catalog: fileCatalog({ resolver: racingResolver, lister, conditionalCommits: true }), tableUrl,
    })
    expect(attempts).toBe(2)
    expect(after['format-version']).toBe(3)
    const snapshot = after.snapshots?.find(s => s['snapshot-id'] === after['current-snapshot-id'])
    expect(snapshot).toMatchObject({ 'first-row-id': 0, 'added-rows': 2 })
    expect(after['next-row-id']).toBe(2)
    expect(manifestWrites).toHaveLength(2)
    expect(manifestWrites[0]).not.toBe(manifestWrites[1])
    const list = await manifestList(after, resolver)
    expect(list.map(m => m.manifest_path)).toEqual([manifestWrites[1]])
    if (!snapshot) throw new Error('snapshot required')
    for (const path of [snapshot['manifest-list'], list[0].manifest_path]) {
      const file = await resolver.reader(path)
      const buffer = await file.slice(0, file.byteLength)
      const header = avroMetadata({ view: new DataView(buffer), offset: 0 })
      expect(header.metadata['format-version']).toBe('3')
    }
    const rows = byId(await icebergRead({ tableUrl, metadata: after, resolver }))
    expect(rows.map(r => r._row_id)).toEqual([0n, 1n])
    expect(rows.map(r => r._last_updated_sequence_number)).toEqual([1n, 2n])
  })

  it('reuses prepared v3 manifests when a concurrent append leaves inherited row ids unchanged', async () => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/rm-v3-reuse'
    await icebergCreateTable({ catalog, tableUrl, schema, partitionSpec: daySpec, formatVersion: 3 })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 10n, created: new Date(DAY) }] })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 20n, created: new Date(0) }] })
    const prepared = await prepareRewriteManifests({ tableUrl, metadata: before, resolver })
    if (!prepared) throw new Error('expected a rewrite')
    await stageSnapshotForRewriteManifests({ tableUrl, metadata: before, prepared, resolver })
    const appended = await icebergAppend({ catalog, tableUrl, records: [{ id: 30n, created: new Date(2 * DAY) }] })
    expect(appended['next-row-id']).toBe(3)
    const staged = await stageSnapshotForRewriteManifests({ tableUrl, metadata: appended, prepared, resolver })
    if (!staged) throw new Error('expected prepared manifests to remain reusable')
    const after = await fileCatalogCommit({ tableUrl, metadata: appended, staged, resolver, conditionalCommits: true })
    const list = await manifestList(after, resolver)
    expect(list.map(m => m.manifest_path)).toContain(prepared.manifests[0].manifest_path)
    expect(after['next-row-id']).toBe(3)
    expect(byId(await icebergRead({ tableUrl, metadata: after, resolver })))
      .toEqual(byId(await icebergRead({ tableUrl, metadata: appended, resolver })))
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

  it.each([false, true])('rejects specs it cannot rewrite losslessly (dropped source: %s)', async dropped => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister })
    const tableUrl = 'http://test/rm-identity-ts'
    await icebergCreateTable({
      catalog, tableUrl, schema,
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 2, 'field-id': 1000, name: 'created', transform: 'identity' }] },
    })
    const metadata = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, created: new Date(0) }] })
    if (dropped) {
      await fileCatalogCommit({
        tableUrl, metadata, resolver,
        staged: {
          requirements: [], writtenFiles: [],
          updates: [
            { action: 'add-spec', spec: { 'spec-id': 1, fields: [] } },
            { action: 'set-default-spec', 'spec-id': 1 },
          ],
        },
      })
      await icebergUpdateSchema({ catalog, tableUrl, schema: { ...schema, fields: [schema.fields[0]] } })
    }
    await expect(icebergRewriteManifests({ catalog, tableUrl, specId: 0 })).rejects.toThrow(/losslessly/)
  })
})
