import { parquetMetadata } from 'hyparquet'
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
 * @import {Resolver, Schema, TableMetadata, UpsertWhenMatched} from '../../src/types.js'
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

  it('gives the same result at any concurrency, with at most that many candidate reads in flight', async () => {
    /**
     * @param {number | undefined} concurrency
     * @returns {Promise<{calls: any[], maxInFlight: number, summary: Record<string, any>, rows: Record<string, any>[]}>}
     */
    async function run(concurrency) {
      const tableUrl = 'http://test/upsert-concurrency'
      const { resolver } = memResolver()
      // Candidate data files with a range read in flight.
      /** @type {Map<string, number>} */
      const inFlight = new Map()
      let maxInFlight = 0
      /**
       * @param {string} path
       * @param {() => ArrayBuffer | Promise<ArrayBuffer>} fn
       * @returns {Promise<ArrayBuffer>}
       */
      async function track(path, fn) {
        inFlight.set(path, (inFlight.get(path) ?? 0) + 1)
        maxInFlight = Math.max(maxInFlight, inFlight.size)
        await new Promise(resolve => setTimeout(resolve, 1))
        const bytes = await fn()
        const count = (inFlight.get(path) ?? 1) - 1
        if (count) inFlight.set(path, count)
        else inFlight.delete(path)
        return bytes
      }
      /** @type {Resolver} */
      const instrumented = {
        ...resolver,
        async reader(path, byteLength) {
          const file = await resolver.reader(path, byteLength)
          if (!path.includes('/data/') || path.includes('-deletes')) return file
          return { byteLength: file.byteLength, slice: (start, end) => track(path, () => file.slice(start, end)) }
        },
      }
      const catalog = fileCatalog({ resolver: instrumented })
      await icebergCreateTable({ catalog, tableUrl, schema: { ...schema, 'identifier-field-ids': [] } })
      await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'a' }] })
      await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'b' }, { id: 3n, name: 'b' }] })
      await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'c' }, { id: 3n, name: 'c' }] })
      maxInFlight = 0

      /** @type {any[]} */
      const calls = []
      const metadata = await icebergUpsert({
        catalog, tableUrl, joinCols: ['id'], matchCols: ['name'], concurrency,
        records: [{ id: 1n, name: 'x' }, { id: 2n, name: 'x' }, { id: 3n, name: 'x' }, { id: 4n, name: 'x' }],
        whenMatched(existingRows, incoming) {
          calls.push([existingRows, incoming])
          return incoming.id !== 3n
        },
      })
      // File sizes vary with the random file names stored in delete files.
      const summary = { ...metadata.snapshots?.at(-1)?.summary }
      delete summary['added-files-size']
      delete summary['total-files-size']
      return {
        calls,
        maxInFlight,
        summary,
        rows: byId(await icebergRead({ tableUrl, metadata, resolver })),
      }
    }

    const sequential = await run(1)
    const unlimited = await run(undefined)
    expect(sequential.maxInFlight).toBe(1)
    expect(unlimited.maxInFlight).toBeGreaterThan(1)
    expect(sequential.calls).toEqual(unlimited.calls)
    expect(sequential.summary).toEqual(unlimited.summary)
    expect(sequential.rows).toEqual(unlimited.rows)
    expect(sequential.rows.map(r => `${r.id}${r.name}`)).toEqual(['1x', '2x', '3b', '3c', '4x'])

    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    const tableUrl = 'http://test/upsert-concurrency-invalid'
    await icebergCreateTable({ catalog, tableUrl, schema })
    for (const concurrency of [0, -1, 1.5, Infinity, NaN, '2']) {
      await expect(icebergUpsert({ catalog, tableUrl, records: [{ id: 1n }], concurrency: /** @type {any} */ (concurrency) }))
        .rejects.toThrow(/concurrency must be a positive integer/)
    }
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
    if (!upsert) throw new Error('staged upsert required')
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

describe('icebergUpsert whenMatched', () => {
  /** @type {Schema} */
  const events = {
    type: 'struct',
    'schema-id': 0,
    'identifier-field-ids': [1],
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: false, type: 'string' },
      { id: 3, name: 'updated_at', required: false, type: 'timestamptz' },
    ],
  }

  it('skips both the delete and the append when whenMatched returns false', async () => {
    const tableUrl = 'http://test/upsert-when-matched'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }] })

    const whenMatched = vi.fn(/** @type {UpsertWhenMatched} */ ((_rows, incoming) => incoming.id !== 1n))
    const skipped = await icebergUpsert({
      catalog, tableUrl, whenMatched, records: [{ id: 1n, name: 'A' }, { id: 2n, name: 'B' }, { id: 3n, name: 'c' }],
    })
    // Called only for matched records, with the join columns of each live row.
    expect(whenMatched.mock.calls).toEqual([
      [[{ id: 1n }], { id: 1n, name: 'A' }],
      [[{ id: 2n }], { id: 2n, name: 'B' }],
    ])
    expect(byId(await icebergRead({ tableUrl, metadata: skipped, resolver }))).toEqual([
      { id: 1n, name: 'a' },
      { id: 2n, name: 'B' },
      { id: 3n, name: 'c' },
    ])

    const replaced = await icebergUpsert({ catalog, tableUrl, whenMatched: () => true, records: [{ id: 1n, name: 'A' }] })
    expect(replaced.snapshots?.at(-1)?.summary['added-position-deletes']).toBe('1')
    expect(byId(await icebergRead({ tableUrl, metadata: replaced, resolver }))).toEqual([
      { id: 1n, name: 'A' },
      { id: 2n, name: 'B' },
      { id: 3n, name: 'c' },
    ])
  })

  it('keeps the newer row: skips older, replaces newer, inserts unmatched in one commit', async () => {
    const tableUrl = 'http://test/upsert-newer-wins'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({ catalog, tableUrl, schema: events })
    const seeded = await icebergAppend({
      catalog, tableUrl, records: [
        { id: 1n, name: 'a', updated_at: new Date('2026-01-01T10:00:00Z') },
        { id: 2n, name: 'b', updated_at: new Date('2026-01-01T10:00:00Z') },
      ],
    })

    const upserted = await icebergUpsert({
      catalog, tableUrl,
      matchCols: ['updated_at'],
      whenMatched: (existingRows, incoming) => existingRows.every(row => incoming.updated_at > row.updated_at),
      records: [
        { id: 1n, name: 'old', updated_at: new Date('2026-01-01T09:00:00Z') },
        { id: 2n, name: 'new', updated_at: new Date('2026-01-01T11:00:00Z') },
        { id: 3n, name: 'c', updated_at: new Date('2026-01-01T08:00:00Z') },
      ],
    })
    expect(upserted.snapshots?.length).toBe((seeded.snapshots?.length ?? 0) + 1)
    expect(byId(await icebergRead({ tableUrl, metadata: upserted, resolver }))).toEqual([
      { id: 1n, name: 'a', updated_at: new Date('2026-01-01T10:00:00Z') },
      { id: 2n, name: 'new', updated_at: new Date('2026-01-01T11:00:00Z') },
      { id: 3n, name: 'c', updated_at: new Date('2026-01-01T08:00:00Z') },
    ])
    // Counts reflect the two accepted records and the one delete written.
    expect(upserted.snapshots?.at(-1)?.summary).toMatchObject({
      operation: 'overwrite',
      'added-data-files': '1',
      'added-records': '2',
      'added-delete-files': '1',
      'added-position-deletes': '1',
      'total-records': '4',
      'total-data-files': '2',
      'total-delete-files': '1',
      'total-position-deletes': '1',
    })
  })

  it('passes every live matched row, and none that are already deleted', async () => {
    const tableUrl = 'http://test/upsert-when-matched-rows'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({ catalog, tableUrl, schema: { ...schema, 'identifier-field-ids': [] } })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }] })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'x' }] })

    /** @type {Record<string, any>[][]} */
    const seen = []
    /**
     * @param {Record<string, any>[]} existingRows
     * @returns {boolean}
     */
    function whenMatched(existingRows) {
      seen.push(existingRows)
      return true
    }
    await icebergUpsert({ catalog, tableUrl, joinCols: ['id'], matchCols: ['name'], whenMatched, records: [{ id: 1n, name: 'c' }] })
    const second = await icebergUpsert({
      catalog, tableUrl, joinCols: ['id'], matchCols: ['name'], whenMatched, records: [{ id: 1n, name: 'd' }],
    })
    expect(seen.map(rows => [...rows].sort((a, b) => a.name < b.name ? -1 : 1))).toEqual([
      [{ id: 1n, name: 'a' }, { id: 1n, name: 'x' }],
      [{ id: 1n, name: 'c' }],
    ])
    expect(byId(await icebergRead({ tableUrl, metadata: second, resolver }))).toEqual([
      { id: 1n, name: 'd' },
      { id: 2n, name: 'b' },
    ])
  })

  it('reads match columns missing from older files as null, and promoted ints as bigint', async () => {
    const tableUrl = 'http://test/upsert-when-matched-evolution'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    /** @type {Schema} */
    const base = {
      type: 'struct',
      'schema-id': 0,
      'identifier-field-ids': [1],
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'version', required: false, type: 'int' },
      ],
    }
    await icebergCreateTable({ catalog, tableUrl, schema: base })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, version: 5 }] })
    await icebergUpdateSchema({
      catalog, tableUrl,
      schema: {
        ...base,
        fields: [
          base.fields[0],
          { ...base.fields[1], type: 'long' },
          { id: 3, name: 'updated_at', required: false, type: 'timestamptz' },
        ],
      },
    })

    const whenMatched = vi.fn(() => false)
    const metadata = await icebergUpsert({
      catalog, tableUrl, matchCols: ['version', 'updated_at'], whenMatched,
      records: [{ id: 1n, version: 6n, updated_at: new Date('2026-01-01T00:00:00Z') }],
    })
    expect(whenMatched).toHaveBeenCalledWith([{ id: 1n, version: 5n, updated_at: null }], expect.anything())
    expect(await icebergRead({ tableUrl, metadata, resolver })).toEqual([{ id: 1n, version: 5, updated_at: null }])
  })

  it('makes no commit when every record is skipped', async () => {
    const tableUrl = 'http://test/upsert-all-skipped'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({ catalog, tableUrl, schema })
    const loaded = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }] })
    const before = new Set(files.keys())

    const records = [{ id: 1n, name: 'A' }, { id: 2n, name: 'B' }]
    /** @type {Record<string, any>[]} */
    const skipped = []
    const metadata = await icebergUpsert({
      catalog, tableUrl, records,
      whenMatched: (_rows, incoming) => {
        skipped.push(incoming)
        return false
      },
    })
    expect(skipped).toEqual(records)
    expect(metadata).toEqual(loaded)
    expect(metadata['current-snapshot-id']).toBe(loaded['current-snapshot-id'])
    // No data, delete, manifest, or metadata file was written.
    expect(new Set(files.keys())).toEqual(before)
    expect(await icebergStageUpsert({ tableUrl, metadata, resolver, records, whenMatched: () => false })).toBeUndefined()
    expect(new Set(files.keys())).toEqual(before)
  })

  it('rejects invalid matchCols and whenMatched before writing any file', async () => {
    const tableUrl = 'http://test/upsert-when-matched-invalid'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({
      catalog, tableUrl,
      schema: {
        ...schema,
        fields: [...schema.fields, {
          id: 3, name: 'tags', required: false, type: { type: 'list', 'element-id': 4, 'element-required': false, element: 'string' },
        }],
      },
    })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a', tags: null }] })
    const before = new Set(files.keys())

    /** @type {[any, RegExp][]} */
    const cases = [
      [{ matchCols: ['missing'] }, /match column not found in current schema: missing/],
      [{ matchCols: ['tags'] }, /unsupported match column type for tags/],
      [{ matchCols: 'name' }, /matchCols must be an array/],
      [{ whenMatched: 'newer' }, /whenMatched must be a function/],
      [{ whenMatched: () => undefined }, /whenMatched must return a boolean, got undefined/],
    ]
    for (const [options, error] of cases) {
      await expect(icebergUpsert({ catalog, tableUrl, records: [{ id: 1n, name: 'b' }], ...options })).rejects.toThrow(error)
    }
    expect(new Set(files.keys())).toEqual(before)
  })

  it('reads matchCols only for row groups that hold a matching key', async () => {
    const tableUrl = 'http://test/upsert-when-matched-late'
    const { resolver, files } = memResolver()
    /** @type {Map<string, [number, number][]>} */
    const slices = new Map()
    const catalog = fileCatalog({
      resolver: {
        ...resolver,
        reader(path, byteLength) {
          const file = resolver.reader(path, byteLength)
          if (file instanceof Promise) throw new Error('expected a sync reader')
          return {
            byteLength: file.byteLength,
            slice(start, end) {
              slices.get(path)?.push([start, end ?? file.byteLength])
              return file.slice(start, end)
            },
          }
        },
      },
    })
    /** @type {Schema} */
    const versioned = {
      type: 'struct',
      'schema-id': 0,
      'identifier-field-ids': [],
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'version', required: false, type: 'long' },
      ],
    }
    /**
     * Incompressible, so the files outgrow whole-file caching (512 KB).
     *
     * @param {number} i
     * @returns {bigint}
     */
    function version(i) {
      return BigInt(Math.imul(i + 1, 2654435761) >>> 1) << 32n | BigInt(Math.imul(i + 7, 40503) >>> 0)
    }
    await icebergCreateTable({ catalog, tableUrl, schema: versioned })
    // The writer's first row group holds 1000 rows, the second the rest. Ids
    // are permuted so neither row group can be pruned by its bounds.
    await icebergAppend({
      catalog, tableUrl, records: Array.from({ length: 60000 }, (_, i) => ({ id: BigInt(i * 7919 % 60000), version: version(i) })),
    })
    // Candidate by bounds for id 100001, but no row matches it.
    await icebergAppend({
      catalog, tableUrl, records: Array.from({ length: 50000 }, (_, i) => ({ id: BigInt(100000 + 2 * i), version: version(i) })),
    })
    const metadata = await icebergAppend({ catalog, tableUrl, records: [{ id: 23757n, version: 99n }] })
    const dataFiles = (await icebergManifests({ metadata, resolver })).flatMap(m => m.entries.map(e => e.data_file))
    const [large, noMatch] = [60000n, 50000n].map(count => {
      const file = dataFiles.find(f => f.record_count === count)
      if (!file) throw new Error('data file not found')
      return file.file_path
    })
    for (const path of [large, noMatch]) {
      expect(files.get(path)?.byteLength).toBeGreaterThan(512 * 1024)
      slices.set(path, [])
    }

    /**
     * @param {string} path
     * @returns {[number, number][]} byte range of the `version` column chunk in each row group
     */
    function versionChunks(path) {
      const bytes = files.get(path)
      if (!bytes) throw new Error(`no such file: ${path}`)
      const buffer = /** @type {ArrayBuffer} */ (bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength))
      return parquetMetadata(buffer).row_groups.map(rg => {
        const meta = rg.columns.find(c => c.meta_data?.path_in_schema[0] === 'version')?.meta_data
        if (!meta) throw new Error('version column not found')
        const start = Number(meta.dictionary_page_offset ?? meta.data_page_offset)
        return [start, start + Number(meta.total_compressed_size)]
      })
    }
    /**
     * Range reads beyond the footer prefetch, which ends at the end of the file.
     *
     * @param {string} path
     * @returns {[number, number][]}
     */
    function rangeReads(path) {
      const size = files.get(path)?.byteLength
      return (slices.get(path) ?? []).filter(([, end]) => end !== size)
    }
    /**
     * @param {string} path
     * @param {[number, number]} chunk
     * @returns {boolean}
     */
    function chunkRead(path, [start, end]) {
      return rangeReads(path).some(([s, e]) => s < end && e > start)
    }
    const largeChunks = versionChunks(large)
    expect(largeChunks.length).toBe(2)

    /** @type {any[]} */
    const calls = []
    await icebergUpsert({
      catalog, tableUrl, joinCols: ['id'], matchCols: ['version'],
      records: [{ id: 23757n, version: 50n }, { id: 100001n, version: 1n }, { id: 200000n, version: 1n }],
      whenMatched(existingRows, incoming) {
        calls.push([existingRows, incoming])
        return false
      },
    })
    // Same rows as a full read of the table.
    const expected = (await icebergRead({ tableUrl, metadata, resolver })).filter(row => row.id === 23757n)
    expect(expected).toHaveLength(2)
    expect(calls).toEqual([[expect.arrayContaining(expected), { id: 23757n, version: 50n }]])
    expect(calls[0][0]).toHaveLength(2)
    // The version column is read for the row group holding the match (row 3) only.
    expect(chunkRead(large, largeChunks[0])).toBe(true)
    expect(chunkRead(large, largeChunks[1])).toBe(false)
    // The file without a match reads its join column but no version bytes.
    expect(rangeReads(noMatch).length).toBeGreaterThan(0)
    expect(versionChunks(noMatch).some(chunk => chunkRead(noMatch, chunk))).toBe(false)
  })
})
