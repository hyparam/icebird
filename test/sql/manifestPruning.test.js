import { collect } from 'squirreling'
import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergManifests } from '../../src/manifest.js'
import { manifestMightMatch } from '../../src/prune.js'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { icebergQuery } from '../../src/sql/icebergQuery.js'
import { serializeValue } from '../../src/write/serde.js'
import { icebergAppend, icebergCreateTable, icebergRewriteManifests, icebergUpdateSchema } from '../../src/write/write.js'
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
    { id: 3, name: 'kind', required: false, type: 'string' },
  ],
}

const DAY = 86400000

/**
 * Count manifest file reads (not manifest lists, not data files).
 *
 * @param {Resolver} inner
 * @returns {{ resolver: Resolver, manifestsRead: () => number, reset: () => void }}
 */
function countingResolver(inner) {
  let count = 0
  return {
    resolver: {
      reader(url, byteLength) {
        if (/\/metadata\/[^/]*\.avro$/.test(url) && !/\/snap-[^/]*\.avro$/.test(url)) count++
        return inner.reader(url, byteLength)
      },
    },
    manifestsRead: () => count,
    reset() { count = 0 },
  }
}

/**
 * A day-partitioned table of 200 single-row commits over 50 days, appended in
 * scrambled day order, then rewritten into manifests clustered by day.
 *
 * @param {PartitionSpec} partitionSpec
 * @returns {Promise<{ resolver: Resolver, tableUrl: string, metadata: TableMetadata }>}
 */
async function clusteredTable(partitionSpec) {
  const { resolver, lister } = memResolver()
  const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
  const tableUrl = `http://test/prune-${Math.random().toString(36).slice(2)}`
  // Fast append, so manifests start out unclustered.
  await icebergCreateTable({
    catalog, tableUrl, schema, partitionSpec,
    properties: { 'commit.manifest-merge.enabled': 'false' },
  })
  for (let i = 0; i < 200; i++) {
    const day = i * 37 % 50
    await icebergAppend({
      catalog, tableUrl,
      records: [{ id: BigInt(i), created: new Date(day * DAY + i * 1000), kind: `k${i % 3}` }],
    })
  }
  // About four entries per manifest, so each covers one or two days.
  const metadata = await icebergRewriteManifests({ catalog, tableUrl, targetSizeBytes: 4 * 1024 })
  return { resolver, tableUrl, metadata }
}

/**
 * @param {Resolver} resolver
 * @param {string} tableUrl
 * @param {string} where
 * @returns {Promise<bigint[]>}
 */
async function query(resolver, tableUrl, where) {
  const results = await icebergQuery({ query: `SELECT id FROM t WHERE ${where}`, tables: { t: tableUrl }, resolver })
  const rows = await collect(results)
  return rows.map(r => /** @type {bigint} */ (r.id)).sort((a, b) => Number(a) - Number(b))
}

/**
 * Two independently prunable manifests for source-lifetime tests.
 *
 * @returns {Promise<{ resolver: Resolver, catalog: ReturnType<typeof fileCatalog>, tableUrl: string }>}
 */
async function smallPartitionedTable() {
  const { resolver, lister } = memResolver()
  const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
  const tableUrl = 'http://test/source-manifests'
  await icebergCreateTable({
    catalog, tableUrl, schema,
    partitionSpec: {
      'spec-id': 0,
      fields: [{ 'source-id': 3, 'field-id': 1000, name: 'kind', transform: 'identity' }],
    },
  })
  for (let i = 0; i < 2; i++) {
    await icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i), kind: `k${i}` }] })
  }
  return { resolver, catalog, tableUrl }
}

describe('manifest list pruning', () => {
  it('loads manifests lazily and shares them across concurrent and repeated scans', async () => {
    const { resolver, tableUrl } = await smallPartitionedTable()
    const counting = countingResolver(resolver)
    const source = await icebergDataSource({ tableUrl, resolver: counting.resolver })
    expect(counting.manifestsRead()).toBe(0)
    /**
     * @param {string} where
     * @returns {Promise<Record<string, import('squirreling').SqlPrimitive>[]>}
     */
    async function scan(where) {
      return collect(await icebergQuery({ query: `SELECT id FROM t ${where} ORDER BY id`, tables: { t: source } }))
    }
    const samePartition = await Promise.all([scan('WHERE kind = \'k0\''), scan('WHERE kind = \'k0\'')])
    expect(samePartition).toEqual([[{ id: 0n }], [{ id: 0n }]])
    expect(counting.manifestsRead()).toBe(1)
    expect(await scan('WHERE kind = \'k0\'')).toEqual([{ id: 0n }])
    expect(counting.manifestsRead()).toBe(1)
    expect(await scan('')).toEqual([{ id: 0n }, { id: 1n }])
    expect(counting.manifestsRead()).toBe(2)
    expect(await scan('WHERE kind = \'k1\'')).toEqual([{ id: 1n }])
    expect(counting.manifestsRead()).toBe(2)
  })

  it('keeps retained manifests private to each source and its snapshot', async () => {
    const { resolver, catalog, tableUrl } = await smallPartitionedTable()
    const counting = countingResolver(resolver)
    const old = await icebergDataSource({ tableUrl, resolver: counting.resolver })
    const sql = 'SELECT id FROM t ORDER BY id'
    expect(await collect(await icebergQuery({ query: sql, tables: { t: old } }))).toEqual([{ id: 0n }, { id: 1n }])
    expect(counting.manifestsRead()).toBe(2)
    await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, kind: 'k2' }] })
    const fresh = await icebergDataSource({ tableUrl, resolver: counting.resolver })
    expect(await collect(await icebergQuery({ query: sql, tables: { t: fresh } }))).toEqual([{ id: 0n }, { id: 1n }, { id: 2n }])
    expect(counting.manifestsRead()).toBe(5)
    expect(await collect(await icebergQuery({ query: sql, tables: { t: old } }))).toEqual([{ id: 0n }, { id: 1n }])
    expect(counting.manifestsRead()).toBe(5)
  })

  it('retries a failed manifest read on the same source', async () => {
    const { resolver, tableUrl } = await smallPartitionedTable()
    let attempts = 0
    /** @type {Resolver} */
    const flaky = {
      reader(url, length) {
        if (url.endsWith('.avro') && !url.includes('/snap-') && ++attempts === 1) {
          throw new Error('temporary manifest failure')
        }
        return resolver.reader(url, length)
      },
    }
    const source = await icebergDataSource({ tableUrl, resolver: flaky })
    /** @returns {Promise<Record<string, import('squirreling').SqlPrimitive>[]>} */
    async function scan() {
      return collect(await icebergQuery({ query: 'SELECT id FROM t WHERE kind = \'k0\'', tables: { t: source } }))
    }
    await expect(scan()).rejects.toThrow('temporary manifest failure')
    expect(await scan()).toEqual([{ id: 0n }])
    expect(attempts).toBe(2)
    expect(await scan()).toEqual([{ id: 0n }])
    expect(attempts).toBe(2)
  })

  it('keeps identity dates before a timestamp later on the same day', async () => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/prune-date-timestamp'
    await icebergCreateTable({
      catalog, tableUrl,
      schema: {
        type: 'struct', 'schema-id': 0,
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'd', required: true, type: 'date' },
        ],
      },
      partitionSpec: {
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'd', transform: 'identity' }],
      },
    })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, d: new Date('2026-01-01') }] })
    const where = 'd < TIMESTAMP \'2026-01-01T12:00:00Z\''
    expect(await query(resolver, tableUrl, `${where} OR id + 0 < 0`)).toEqual([1n])
    expect(await query(resolver, tableUrl, where)).toEqual([1n])
  })

  it.each([false, true])('binds filters by field id after a schema-only name swap (pinned: %s)', async pinned => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = 'http://test/prune-renamed'
    /** @type {Schema} */
    const originalSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'a', required: true, type: 'int' },
        { id: 2, name: 'b', required: true, type: 'int' },
      ],
    }
    await icebergCreateTable({
      catalog, tableUrl, schema: originalSchema,
      partitionSpec: {
        'spec-id': 0,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'a', transform: 'identity' }],
      },
    })
    const appended = await icebergAppend({ catalog, tableUrl, records: [{ a: 1, b: 2 }] })
    const metadata = await icebergUpdateSchema({
      catalog, tableUrl,
      schema: {
        ...originalSchema,
        fields: originalSchema.fields.map(f => ({ ...f, name: f.name === 'a' ? 'b' : 'a' })),
      },
    })
    expect(metadata['current-snapshot-id']).toBe(appended['current-snapshot-id'])
    expect(metadata.snapshots?.[0]['schema-id']).toBe(0)
    expect(metadata['current-schema-id']).toBe(1)

    const snapshotId = pinned ? metadata['current-snapshot-id'] : undefined
    const matching = await icebergManifests({ metadata, resolver, snapshotId, filter: { a: { $eq: 2 } } })
    expect(matching).toHaveLength(pinned ? 0 : 1)
    const other = await icebergManifests({ metadata, resolver, snapshotId, filter: { b: { $eq: 2 } } })
    expect(other).toHaveLength(pinned ? 1 : 0)
  })

  it('reads only manifests whose day range matches a one-day predicate', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { resolver, tableUrl, metadata } = await clusteredTable({
      'spec-id': 0,
      fields: [{ 'source-id': 2, 'field-id': 1000, name: 'created_day', transform: 'day' }],
    })
    const totalManifests = (await icebergManifests({ metadata, resolver })).length
    expect(totalManifests).toBeGreaterThan(30)

    const counting = countingResolver(resolver)
    const where = 'created >= TIMESTAMP \'1970-01-11T00:00:00Z\' AND created < TIMESTAMP \'1970-01-12T00:00:00Z\''
    const pruned = await query(counting.resolver, tableUrl, where)
    // Include the conservative boundary day for the strict timestamp cutoff.
    expect(counting.manifestsRead()).toBeLessThanOrEqual(4)

    // Same rows as a scan that cannot prune manifests (`+ 0` stays in the engine).
    counting.reset()
    const unpruned = await query(counting.resolver, tableUrl, `${where} OR id + 0 < 0`)
    expect(counting.manifestsRead()).toBe(totalManifests)
    expect(pruned).toEqual(unpruned)
    const expected = Array.from({ length: 200 }, (_, i) => i).filter(i => i * 37 % 50 === 10)
    expect(pruned.map(Number)).toEqual(expected)

    // icebergManifests takes the same filter.
    const filter = { created: { $gte: new Date(10 * DAY), $lt: new Date(11 * DAY) } }
    const filtered = await icebergManifests({ metadata, resolver, filter })
    expect(filtered.length).toBeLessThanOrEqual(4)
    const days = filtered.flatMap(m => m.entries).map(e => Number(e.data_file.partition.created_day))
    expect(days).toContain(10)
  }, 60000)

  it('prunes on identity partitions and keeps manifests a predicate cannot exclude', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const { resolver, tableUrl, metadata } = await clusteredTable({
      'spec-id': 0,
      fields: [{ 'source-id': 3, 'field-id': 1000, name: 'kind', transform: 'identity' }],
    })
    const counting = countingResolver(resolver)
    const rows = await query(counting.resolver, tableUrl, 'kind = \'k1\'')
    const totalManifests = (await icebergManifests({ metadata, resolver })).length
    expect(counting.manifestsRead()).toBeLessThan(totalManifests / 2)
    expect(rows.map(Number)).toEqual(Array.from({ length: 200 }, (_, i) => i).filter(i => i % 3 === 1))

    // A predicate on a non-partition column reads every manifest.
    counting.reset()
    await query(counting.resolver, tableUrl, 'id = 5')
    expect(counting.manifestsRead()).toBe(totalManifests)
  }, 60000)
})

describe('manifestMightMatch', () => {
  /** @type {any} */
  const raw = {
    'partition-specs': [{
      'spec-id': 0,
      fields: [
        { 'source-id': 2, 'field-id': 1000, name: 'created_day', transform: 'day' },
        { 'source-id': 1, 'field-id': 1001, name: 'id_bucket', transform: 'bucket[4]' },
      ],
    }],
  }
  /** @type {TableMetadata} */
  const metadata = raw

  /**
   * @param {number} value
   * @returns {Uint8Array}
   */
  function int(value) {
    const bytes = new Uint8Array(4)
    new DataView(bytes.buffer).setInt32(0, value, true)
    return bytes
  }

  /** @type {Manifest} */
  const manifest = /** @type {any} */ ({
    partition_spec_id: 0,
    partitions: [
      { contains_null: false, lower_bound: int(10), upper_bound: int(12) },
      { contains_null: false, lower_bound: int(1), upper_bound: int(1) },
    ],
  })

  it('evaluates monotonic transforms against the range', () => {
    expect(manifestMightMatch({ created: { $gte: new Date(13 * DAY) } }, manifest, schema, metadata)).toBe(false)
    expect(manifestMightMatch({ created: { $gte: new Date(12 * DAY + 5) } }, manifest, schema, metadata)).toBe(true)
    expect(manifestMightMatch({ created: { $lt: new Date(10 * DAY + 1) } }, manifest, schema, metadata)).toBe(true)
    // Date literals keep the boundary partition conservatively.
    expect(manifestMightMatch({ created: { $lt: new Date(10 * DAY) } }, manifest, schema, metadata)).toBe(true)
    expect(manifestMightMatch({ created: { $lte: new Date(10 * DAY) } }, manifest, schema, metadata)).toBe(true)
    expect(manifestMightMatch({ created: { $gt: new Date(13 * DAY - 1) } }, manifest, schema, metadata)).toBe(true)
    expect(manifestMightMatch({ created: { $gt: new Date(13 * DAY - 2) } }, manifest, schema, metadata)).toBe(true)
    expect(manifestMightMatch({ created: { $eq: new Date(11 * DAY) } }, manifest, schema, metadata)).toBe(true)
    expect(manifestMightMatch({ created: { $in: [new Date(1 * DAY), new Date(20 * DAY)] } }, manifest, schema, metadata)).toBe(false)
    expect(manifestMightMatch({ created: { $ne: new Date(11 * DAY) } }, manifest, schema, metadata)).toBe(true)
  })

  it('evaluates bucket equality', () => {
    // bucket[4] of long 1 is 0; ids 2..4 land elsewhere, keep only matches.
    const results = [0n, 1n, 2n, 3n, 4n, 5n, 6n, 7n].map(id => manifestMightMatch({ id: { $eq: id } }, manifest, schema, metadata))
    expect(results).toContain(true)
    expect(results).toContain(false)
  })

  it.each(/** @type {const} */ (['timestamp', 'timestamptz', 'timestamp_ns', 'timestamptz_ns']))(
    'keeps submillisecond %s matches at temporal partition boundaries', type => {
      const temporalSchema = { ...schema, fields: [{ ...schema.fields[1], type }] }
      for (const transform of ['year', 'month', 'day', 'hour']) {
        const temporalMetadata = {
          ...metadata,
          'partition-specs': [{
            'spec-id': 0,
            fields: [{ 'source-id': 2, 'field-id': 1000, name: 'created_partition', transform }],
          }],
        }
        // -500 microseconds (or nanoseconds) belongs to partition -1 and is > -1 ms.
        const temporalManifest = {
          ...manifest,
          partitions: [{ contains_null: false, lower_bound: int(-1), upper_bound: int(-1) }],
        }
        expect(manifestMightMatch({ created: { $gt: new Date(-1) } }, temporalManifest, temporalSchema, temporalMetadata)).toBe(true)
        expect(manifestMightMatch({ created: { $gt: new Date(0) } }, temporalManifest, temporalSchema, temporalMetadata)).toBe(false)
        const unitsPerMillis = type.endsWith('_ns') ? 1000000n : 1000n
        for (const value of [-unitsPerMillis, -unitsPerMillis + 1n]) {
          expect(manifestMightMatch({ created: { $gt: value } }, temporalManifest, temporalSchema, temporalMetadata)).toBe(true)
          expect(manifestMightMatch({ created: { $gt: Number(value) } }, temporalManifest, temporalSchema, temporalMetadata)).toBe(true)
        }
        expect(manifestMightMatch({ created: { $eq: -1n } }, temporalManifest, temporalSchema, temporalMetadata)).toBe(true)
        expect(manifestMightMatch({ created: { $lt: 0n } }, temporalManifest, temporalSchema, temporalMetadata)).toBe(true)
        expect(manifestMightMatch({ created: { $gt: -1n } }, temporalManifest, temporalSchema, temporalMetadata)).toBe(false)
      }
    }
  )

  it('compares identity date summaries against the full timestamp literal', () => {
    /** @type {Schema} */
    const dateSchema = { ...schema, fields: [{ ...schema.fields[1], type: 'date' }] }
    const dateMetadata = {
      ...metadata,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'created', transform: 'identity' }],
      }],
    }
    const day = Date.parse('2026-01-01') / DAY
    const dateManifest = {
      ...manifest,
      partitions: [{ contains_null: false, lower_bound: int(day), upper_bound: int(day) }],
    }
    expect(manifestMightMatch({ created: { $lt: new Date('2026-01-01T12:00:00Z') } }, dateManifest, dateSchema, dateMetadata)).toBe(true)
    expect(manifestMightMatch({ created: { $lt: new Date('2026-01-01') } }, dateManifest, dateSchema, dateMetadata)).toBe(false)
  })

  it.each(/** @type {const} */ (['float', 'double']))('treats signed zeros equally in identity %s summaries', type => {
    const zeroSchema = { ...schema, fields: [{ ...schema.fields[0], type }] }
    const zeroMetadata = {
      ...metadata,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'id', transform: 'identity' }],
      }],
    }
    for (const zero of [-0, 0]) {
      const bound = serializeValue(zero, type)
      const zeroManifest = {
        ...manifest,
        partitions: [{ contains_null: false, lower_bound: bound, upper_bound: bound }],
      }
      const literal = -zero
      for (const condition of [{ $eq: literal }, { $in: [literal] }, { $lte: literal }, { $gte: literal }]) {
        expect(manifestMightMatch({ id: condition }, zeroManifest, zeroSchema, zeroMetadata)).toBe(true)
      }
      for (const condition of [{ $lt: literal }, { $gt: literal }, { $eq: 1 }, { $eq: -1 }]) {
        expect(manifestMightMatch({ id: condition }, zeroManifest, zeroSchema, zeroMetadata)).toBe(false)
      }
    }
  })

  it('handles AND / OR and keeps on missing summaries', () => {
    expect(manifestMightMatch({ $or: [{ created: { $lt: new Date(0) } }, { created: { $gt: new Date(30 * DAY) } }] }, manifest, schema, metadata)).toBe(false)
    expect(manifestMightMatch({ $or: [{ created: { $lt: new Date(0) } }, { kind: { $eq: 'x' } }] }, manifest, schema, metadata)).toBe(true)
    const bare = /** @type {Manifest} */ ({ ...manifest, partitions: undefined })
    expect(manifestMightMatch({ created: { $lt: new Date(0) } }, bare, schema, metadata)).toBe(true)
  })
})
