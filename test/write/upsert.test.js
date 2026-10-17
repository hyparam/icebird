import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergManifests } from '../../src/manifest.js'
import { icebergRead } from '../../src/read.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { writeParquet } from '../../src/write/parquet.js'
import { writeManifestFile } from '../../src/write/rewrite-manifests.js'
import { buildSnapshotUpdate } from '../../src/write/snapshot.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergStageUpsert } from '../../src/write/upsert.js'
import { icebergAppend, icebergCreateTable, icebergUpdateSchema, icebergUpsert } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Resolver, Schema, TableMetadata} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  'identifier-field-ids': [1],
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
}

/**
 * @param {Record<string, any>[]} rows
 * @returns {Record<string, any>[]}
 */
function byId(rows) {
  return [...rows].sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0)
}

/**
 * Commit an equality delete on `id`, as another engine would.
 *
 * @param {TableMetadata} metadata
 * @param {Resolver} resolver
 * @param {Record<string, any>} partition
 * @param {Record<string, any>[]} rows
 * @returns {Promise<TableMetadata>}
 */
async function commitEqualityDelete(metadata, resolver, partition, rows) {
  const tableUrl = metadata.location
  const schema = metadata.schemas[0]
  const sequenceNumber = BigInt(metadata['last-sequence-number']) + 1n
  const snapshotId = BigInt(metadata['current-snapshot-id'] ?? 0) + 1n
  const filePath = `${tableUrl}/data/eq.parquet`
  const { writer } = resolver
  if (!writer) throw new Error('writer required')
  const w = writer(filePath)
  await writeParquet({ writer: w, schema: { ...schema, fields: schema.fields.filter(f => f.name === 'id') }, records: rows })
  const manifestPath = `${tableUrl}/metadata/eq.avro`
  const manifest = await writeManifestFile({
    resolver, manifestPath, schema, spec: metadata['partition-specs'][0], content: 1, snapshotId, sequenceNumber, formatVersion: 2,
    entries: [{
      status: 1, snapshot_id: snapshotId, sequence_number: sequenceNumber, file_sequence_number: sequenceNumber,
      data_file: {
        content: 2, file_path: filePath, file_format: 'parquet', partition, equality_ids: [2],
        record_count: BigInt(rows.length), file_size_in_bytes: BigInt(w.offset),
      },
    }],
  })
  const staged = await buildSnapshotUpdate({
    tableUrl, metadata, resolver, snapshotId, sequenceNumber, manifestUuid: 'eq',
    timestampMs: Date.now(), formatVersion: 2, newManifests: [manifest],
    summary: { operation: 'delete' }, writtenFiles: [filePath, manifestPath],
  })
  return await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
}

describe('icebergUpsert', () => {
  it('replaces matching rows and inserts the rest in one commit', async () => {
    const tableUrl = 'http://test/upsert'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }, { id: 3n, name: 'c' }] })

    // joinCols defaults to the identifier fields.
    const first = await icebergUpsert({ catalog, tableUrl, records: [{ id: 2n, name: 'B' }, { id: 4n, name: 'd' }] })
    expect(first.snapshots?.map(s => s.summary.operation)).toEqual(['append', 'overwrite'])
    expect(first.snapshots?.[1].summary['added-position-deletes']).toBe('1')
    expect(first.snapshots?.[1].summary['changed-partition-count']).toBe('1')

    // id 3 is at physical position 2 of the first file but logical index 1.
    const second = await icebergUpsert({ catalog, tableUrl, joinCols: ['id'], records: [{ id: 3n, name: 'C' }] })
    expect(byId(await icebergRead({ tableUrl, metadata: second, resolver }))).toEqual([
      { id: 1n, name: 'a' },
      { id: 2n, name: 'B' },
      { id: 3n, name: 'C' },
      { id: 4n, name: 'd' },
    ])
  })

  it('matches composite keys by field id across partitions', async () => {
    const tableUrl = 'http://test/upsert-composite'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    /** @type {Schema} */
    const composite = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'region', required: true, type: 'string' },
        { id: 2, name: 'id', required: true, type: 'int' },
        { id: 3, name: 'name', required: false, type: 'string' },
      ],
    }
    await icebergCreateTable({
      catalog, tableUrl, schema: composite,
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' }] },
    })
    const joinCols = ['region', 'id']
    await icebergUpsert({
      catalog, tableUrl, joinCols,
      records: [{ region: 'eu', id: 1, name: 'eu1' }, { region: 'us', id: 1, name: 'us1' }],
    })
    const update = [{ region: 'eu', id: 1, name: 'eu2' }]
    await icebergUpsert({ catalog, tableUrl, joinCols, records: update })
    const redelivered = await icebergUpsert({ catalog, tableUrl, joinCols, records: update })
    expect(await icebergRead({ tableUrl, metadata: redelivered, resolver })).toEqual([
      { region: 'us', id: 1, name: 'us1' },
      { region: 'eu', id: 1, name: 'eu2' },
    ])

    await icebergUpdateSchema({
      catalog, tableUrl,
      schema: { ...composite, fields: composite.fields.map(f => f.id === 2 ? { ...f, name: 'key' } : f) },
    })
    const renamed = await icebergUpsert({
      catalog, tableUrl, joinCols: ['region', 'key'], records: [{ region: 'us', key: 1, name: 'us2' }],
    })
    expect(await icebergRead({ tableUrl, metadata: renamed, resolver })).toEqual([
      { region: 'eu', key: 1, name: 'eu2' },
      { region: 'us', key: 1, name: 'us2' },
    ])
  })

  it('matches keys stored only in partition metadata or defaults, and skips equality-deleted rows', async () => {
    const tableUrl = 'http://test/upsert-read-semantics'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    /** @type {Schema} */
    const base = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'region', required: true, type: 'string' },
        { id: 2, name: 'id', required: true, type: 'long' },
      ],
    }
    await icebergCreateTable({
      catalog, tableUrl, schema: base,
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' }] },
    })
    const seeded = await icebergAppend({ catalog, tableUrl, records: [{ region: 'eu', id: 1n }, { region: 'eu', id: 2n }] })
    // Rewrite the data file without the identity-partitioned column, as
    // migrated Hive tables do, so `region` comes from partition metadata.
    const [dataPath] = (await icebergManifests({ metadata: seeded, resolver })).flatMap(m => m.entries.map(e => e.data_file.file_path))
    const { writer } = resolver
    if (!writer) throw new Error('writer required')
    const w = writer(dataPath)
    await writeParquet({ writer: w, schema: { ...base, fields: [base.fields[1]] }, records: [{ id: 1n }, { id: 2n }] })
    // Equality-delete id 2 like another engine would.
    const deleted = await commitEqualityDelete(seeded, resolver, { region: 'eu' }, [{ id: 2n }])
    expect(await icebergRead({ tableUrl, metadata: deleted, resolver })).toEqual([{ region: 'eu', id: 1n }])

    // A column added later reads as its default in the older file.
    await icebergUpdateSchema({
      catalog, tableUrl,
      schema: { ...base, fields: [...base.fields, { id: 3, name: 'tag', required: false, type: 'string' }] },
    })
    const upserted = await icebergUpsert({
      catalog, tableUrl, joinCols: ['region', 'id', 'tag'],
      records: [{ region: 'eu', id: 1n, tag: 'x' }, { region: 'eu', id: 2n, tag: 'y' }],
    })
    // Neither key matches: tag is null in the old file, and id 2 is already deleted.
    expect(upserted.snapshots?.at(-1)?.summary['added-position-deletes']).toBe('0')
    const keyed = await icebergUpsert({
      catalog, tableUrl, joinCols: ['region', 'id'], records: [{ region: 'eu', id: 1n, tag: 'z' }, { region: 'eu', id: 2n, tag: 'z' }],
    })
    expect(keyed.snapshots?.at(-1)?.summary['added-position-deletes']).toBe('3')
    // Data and delete files all land in region=eu.
    expect(keyed.snapshots?.at(-1)?.summary['changed-partition-count']).toBe('1')
    expect(await icebergRead({ tableUrl, metadata: keyed, resolver })).toEqual([
      { region: 'eu', id: 1n, tag: 'z' },
      { region: 'eu', id: 2n, tag: 'z' },
    ])
  })

  it('rejects invalid input before writing any file', async () => {
    const tableUrl = 'http://test/upsert-invalid'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })
    const metadata = await icebergCreateTable({
      catalog, tableUrl,
      schema: {
        ...schema,
        'identifier-field-ids': [],
        fields: [...schema.fields, { id: 3, name: 'score', required: false, type: 'double' }, { id: 4, name: 'n', required: false, type: 'int' }],
      },
    })
    const before = new Set(files.keys())

    /** @type {[any, RegExp][]} */
    const cases = [
      [{ records: [{ id: 1n }] }, /joinCols is required/],
      [{ joinCols: ['missing'], records: [{ id: 1n }] }, /join column not found in current schema: missing/],
      [{ joinCols: ['score'], records: [{ score: 1 }] }, /unsupported join column type for score/],
      [{ joinCols: ['id', 'name'], records: [{ id: 1n, name: null }] }, /missing join column name/],
      [{ joinCols: ['id'], records: [{ id: 1 }] }, /invalid long value for join column id/],
      [{ joinCols: ['n'], records: [{ n: 2 ** 40 }] }, /invalid int value for join column n/],
      [{ joinCols: ['id'], records: [{ id: 5n, name: 'x' }, { id: 5n, name: 'y' }] }, /duplicate join key/],
    ]
    for (const [options, error] of cases) {
      await expect(icebergUpsert({ catalog, tableUrl, ...options })).rejects.toThrow(error)
    }
    await expect(icebergStageUpsert({
      tableUrl, metadata: { ...metadata, 'format-version': 3 }, resolver, joinCols: ['id'], records: [],
    })).rejects.toThrow(/format-version 2 only/)
    expect(new Set(files.keys())).toEqual(before)

    // A record the writer rejects after a key matched leaves no delete files.
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const appended = new Set(files.keys())
    await expect(icebergUpsert({ catalog, tableUrl, joinCols: ['id'], records: [{ id: 1n, name: 123 }] }))
      .rejects.toThrow(/expected Uint8Array/)
    expect([...files.keys()].filter(path => !appended.has(path) && path.includes('-deletes'))).toEqual([])
  })

  it('throws on a concurrent commit rather than duplicating rows', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/upsert-conflict'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({ catalog, tableUrl, schema })
    const m1 = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })

    // Stage an upsert against m1, then let a concurrent append commit first.
    const upsert = await icebergStageUpsert({ tableUrl, metadata: m1, resolver, joinCols: ['id'], records: [{ id: 1n, name: 'b' }] })
    const a2 = await icebergStageAppend({ tableUrl, metadata: m1, records: [{ id: 2n, name: 'c' }], resolver })
    const m2 = await fileCatalogCommit({ tableUrl, metadata: m1, staged: a2, resolver })

    await expect(fileCatalogCommit({ tableUrl, metadata: m2, staged: upsert, resolver }))
      .rejects.toThrow(/ref main expected snapshot/)
    expect(await icebergRead({ tableUrl, metadata: m2, resolver })).toEqual([{ id: 1n, name: 'a' }, { id: 2n, name: 'c' }])
  })

  it('keeps staged files when the commit outcome is unknown', async () => {
    const tableUrl = 'http://test/upsert-ambiguous'
    const { resolver, files, lister } = memResolver()
    const deleter = vi.fn(resolver.deleter)
    const { writer } = resolver
    if (!writer) throw new Error('writer required')
    let lost = false
    const catalog = fileCatalog({
      lister,
      conditionalCommits: true,
      resolver: {
        ...resolver,
        deleter,
        // Persist the metadata file, then lose the response.
        writer(path, options) {
          const w = writer(path, options)
          if (!lost || !path.endsWith('.metadata.json')) return w
          const finish = w.finish.bind(w)
          w.finish = async () => {
            await finish()
            throw new Error('connection lost')
          }
          return w
        },
      },
    })
    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const before = new Set(files.keys())

    lost = true
    await expect(icebergUpsert({ catalog, tableUrl, records: [{ id: 1n, name: 'b' }] })).rejects.toThrow('connection lost')
    expect(deleter).not.toHaveBeenCalled()
    lost = false
    const metadata = await icebergAppend({ catalog, tableUrl, records: [] })
    expect(metadata.snapshots?.map(s => s.summary.operation)).toEqual(['append', 'overwrite', 'append'])
    expect([...before].every(path => files.has(path))).toBe(true)
    expect(await icebergRead({ tableUrl, metadata, resolver })).toEqual([{ id: 1n, name: 'b' }])
  })
})
