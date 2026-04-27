import { describe, expect, it, vi } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { fetchAvroRecords } from '../../src/fetch.js'
import { applyUpdates, checkRequirements, fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'
import { icebergStageAppend } from '../../src/write/stage.js'

/**
 * @import {Resolver, Schema, TableMetadata} from '../../src/types.js'
 */

/**
 * @returns {{ resolver: Resolver, files: Map<string, Uint8Array> }}
 */
function memResolver() {
  /** @type {Map<string, Uint8Array>} */
  const files = new Map()
  /** @type {Resolver} */
  const resolver = {
    reader(path) {
      const bytes = files.get(path)
      if (!bytes) throw new Error(`no such file: ${path}`)
      const ab = /** @type {ArrayBuffer} */ (
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
      )
      return {
        byteLength: bytes.byteLength,
        slice: (/** @type {number} */ s, /** @type {number} */ e) => ab.slice(s, e),
      }
    },
    writer(path) {
      const w = new ByteWriter()
      const origFinish = w.finish.bind(w)
      w.finish = () => {
        origFinish()
        files.set(path, w.getBytes())
      }
      return w
    },
  }
  return { resolver, files }
}

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
}

describe('icebergStageAppend', () => {
  it('returns the StagedUpdate payload without writing metadata.json', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage1'
    const { resolver, files } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const beforeFiles = new Set(files.keys())

    const staged = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })

    // requirements use the live table-uuid and a null parent ref (first append)
    expect(staged.requirements).toEqual([
      { type: 'assert-table-uuid', uuid: created['table-uuid'] },
      { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': null },
    ])

    expect(staged.updates).toHaveLength(2)
    expect(staged.updates[0]).toMatchObject({ action: 'add-snapshot' })
    expect(staged.updates[1]).toMatchObject({
      action: 'set-snapshot-ref',
      'ref-name': 'main',
      type: 'branch',
      'snapshot-id': staged.snapshot['snapshot-id'],
    })

    // data + manifest + manifest-list were written; metadata.json was not
    expect(staged.writtenFiles).toHaveLength(3)
    for (const path of staged.writtenFiles) expect(files.has(path)).toBe(true)
    const newFiles = [...files.keys()].filter(k => !beforeFiles.has(k))
    expect(newFiles.sort()).toEqual([...staged.writtenFiles].sort())
    expect([...files.keys()].some(k => k.endsWith('v2.metadata.json'))).toBe(false)
  })

  it('round-trips through fileCatalogCommit + icebergRead', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage2'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const records = [{ id: 1n, name: 'alice' }, { id: 2n, name: 'bob' }]

    const staged = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const committed = await fileCatalogCommit({ tableUrl, metadata: created, staged, resolver })

    expect(committed['current-snapshot-id']).toBe(staged.snapshot['snapshot-id'])
    expect(committed.refs?.main).toEqual({ 'snapshot-id': staged.snapshot['snapshot-id'], type: 'branch' })
    expect(committed['snapshot-log']).toHaveLength(1)
    expect(committed['metadata-log']).toHaveLength(1)

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('assigns row lineage for v3 appends', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage-v3'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const v3Metadata = { ...created, 'format-version': 3, 'next-row-id': 100 }
    const records = [{ id: 1n, name: 'alice' }, { id: 2n, name: 'bob' }]

    const staged = await icebergStageAppend({ tableUrl, metadata: v3Metadata, records, resolver })
    expect(staged.requirements).toContainEqual({ type: 'assert-next-row-id', 'next-row-id': 100 })
    expect(staged.snapshot['first-row-id']).toBe(100)
    expect(staged.snapshot['added-rows']).toBe(2)

    const committed = await fileCatalogCommit({ tableUrl, metadata: v3Metadata, staged, resolver })
    expect(committed['next-row-id']).toBe(102)

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual([
      {
        id: 1n,
        name: 'alice',
        _row_id: 100n,
        _last_updated_sequence_number: 1n,
      },
      {
        id: 2n,
        name: 'bob',
        _row_id: 101n,
        _last_updated_sequence_number: 1n,
      },
    ])
  })

  it('writes one parquet per identity-partition group and round-trips through read', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage-partitioned'
    const { resolver, files } = memResolver()

    /** @type {Schema} */
    const partitionedSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'country', required: true, type: 'string' },
      ],
    }
    const created = await icebergCreate({ tableUrl, resolver, schema: partitionedSchema })
    const partitioned = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'country', transform: /** @type {const} */ ('identity') }],
      }],
      'last-partition-id': 1000,
    }

    const records = [
      { id: 1n, country: 'us' },
      { id: 2n, country: 'fr' },
      { id: 3n, country: 'us' },
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: partitioned, records, resolver })

    // two partition groups => two parquet files + 1 manifest + 1 manifest list
    const writtenParquet = staged.writtenFiles.filter(p => p.endsWith('.parquet'))
    expect(writtenParquet).toHaveLength(2)
    for (const p of staged.writtenFiles) expect(files.has(p)).toBe(true)
    expect(staged.snapshot.summary['added-data-files']).toBe('2')
    expect(staged.snapshot.summary['changed-partition-count']).toBe('2')
    expect(staged.snapshot.summary['added-records']).toBe('3')

    const committed = await fileCatalogCommit({ tableUrl, metadata: partitioned, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })

    expect(read).toHaveLength(3)
    expect([...read].sort((a, b) => Number(a.id - b.id))).toEqual(records)
  })

  it('emits manifest-list partition FieldSummary for an identity spec', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage-summary'
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
    const created = await icebergCreate({ tableUrl, resolver, schema: partitionedSchema })
    const partitioned = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'country', transform: /** @type {const} */ ('identity') }],
      }],
      'last-partition-id': 1000,
    }

    const records = [
      { id: 1n, country: 'us' },
      { id: 2n, country: 'fr' },
      { id: 3n, country: 'us' },
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: partitioned, records, resolver })
    const manifestListPath = staged.snapshot['manifest-list']
    if (!manifestListPath) throw new Error('manifest-list path missing')
    const list = await fetchAvroRecords(manifestListPath, resolver)

    expect(list).toHaveLength(1)
    expect(list[0].partitions).toHaveLength(1)
    const summary = list[0].partitions[0]
    expect(summary.contains_null).toBe(false)
    // string lower/upper bounds are UTF-8 of 'fr' / 'us'
    expect(summary.lower_bound).toEqual(new TextEncoder().encode('fr'))
    expect(summary.upper_bound).toEqual(new TextEncoder().encode('us'))
  })

  it('groups by bucket[N] and round-trips through read', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage-bucket'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const bucketed = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{
          'source-id': 1, 'field-id': 1000, name: 'id_bucket',
          transform: /** @type {const} */ ('bucket[4]'),
        }],
      }],
      'last-partition-id': 1000,
    }

    const records = [
      { id: 1n, name: 'alice' },
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
      { id: 4n, name: 'dan' },
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: bucketed, records, resolver })

    const writtenParquet = staged.writtenFiles.filter(p => p.endsWith('.parquet'))
    // bucket[4] over 4 distinct longs typically produces 2-4 groups; verify at least one and at most four
    expect(writtenParquet.length).toBeGreaterThanOrEqual(1)
    expect(writtenParquet.length).toBeLessThanOrEqual(4)
    expect(staged.snapshot.summary['added-records']).toBe('4')

    const committed = await fileCatalogCommit({ tableUrl, metadata: bucketed, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect([...read].sort((a, b) => Number(a.id - b.id))).toEqual(records)
  })

  it('groups by day(timestamp) and round-trips through read', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage-day'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const tsSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'ts', required: true, type: 'timestamp' },
      ],
    }
    const created = await icebergCreate({ tableUrl, resolver, schema: tsSchema })
    const dayed = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{
          'source-id': 2, 'field-id': 1000, name: 'ts_day',
          transform: /** @type {const} */ ('day'),
        }],
      }],
      'last-partition-id': 1000,
    }

    const records = [
      { id: 1n, ts: new Date('2024-01-01T08:00:00Z') },
      { id: 2n, ts: new Date('2024-01-01T20:00:00Z') },
      { id: 3n, ts: new Date('2024-01-02T08:00:00Z') },
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: dayed, records, resolver })

    // two distinct days => two parquet files
    const writtenParquet = staged.writtenFiles.filter(p => p.endsWith('.parquet'))
    expect(writtenParquet).toHaveLength(2)
    expect(staged.snapshot.summary['changed-partition-count']).toBe('2')

    const committed = await fileCatalogCommit({ tableUrl, metadata: dayed, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect([...read].sort((a, b) => Number(a.id - b.id))).toEqual(records)
  })

  it('groups by truncate[W] on a string column and round-trips through read', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage-truncate'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const truncated = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{
          'source-id': 2, 'field-id': 1000, name: 'name_pre',
          transform: /** @type {const} */ ('truncate[2]'),
        }],
      }],
      'last-partition-id': 1000,
    }

    const records = [
      { id: 1n, name: 'alice' },
      { id: 2n, name: 'alex' }, // shares 'al' prefix with alice
      { id: 3n, name: 'bob' },
      { id: 4n, name: 'bart' }, // shares 'ba' with bob? no — 'ba' vs 'bo'; distinct
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: truncated, records, resolver })

    // expected groups: 'al' (alice, alex), 'bo' (bob), 'ba' (bart) => 3 files
    const writtenParquet = staged.writtenFiles.filter(p => p.endsWith('.parquet'))
    expect(writtenParquet).toHaveLength(3)

    const committed = await fileCatalogCommit({ tableUrl, metadata: truncated, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect([...read].sort((a, b) => Number(a.id - b.id))).toEqual(records)
  })

  it('collapses records into one group for a void-transform partition spec', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage-void'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const voidSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'country', required: true, type: 'string' },
      ],
    }
    const created = await icebergCreate({ tableUrl, resolver, schema: voidSchema })
    const voided = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'country_void', transform: /** @type {const} */ ('void') }],
      }],
      'last-partition-id': 1000,
    }

    const records = [
      { id: 1n, country: 'us' },
      { id: 2n, country: 'fr' },
      { id: 3n, country: 'us' },
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: voided, records, resolver })

    // void collapses every record into a single group => one parquet file
    const writtenParquet = staged.writtenFiles.filter(p => p.endsWith('.parquet'))
    expect(writtenParquet).toHaveLength(1)
    expect(staged.snapshot.summary['added-data-files']).toBe('1')
    expect(staged.snapshot.summary['changed-partition-count']).toBe('1')
    expect(staged.snapshot.summary['added-records']).toBe('3')

    const committed = await fileCatalogCommit({ tableUrl, metadata: voided, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })

    expect([...read].sort((a, b) => Number(a.id - b.id))).toEqual(records)
  })

  it('carries forward prior manifests across two sequential commits', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stage3'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })

    const stagedA = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })
    const v2 = await fileCatalogCommit({ tableUrl, metadata: created, staged: stagedA, resolver })

    const stagedB = await icebergStageAppend({
      tableUrl, metadata: v2, resolver,
      records: [{ id: 2n, name: 'bob' }, { id: 3n, name: 'carol' }],
    })
    const v3 = await fileCatalogCommit({ tableUrl, metadata: v2, staged: stagedB, resolver })

    expect(v3.snapshots).toHaveLength(2)
    expect(v3['last-sequence-number']).toBe(2)
    expect(v3.snapshots?.[1].summary['total-records']).toBe('3')
    expect(v3.snapshots?.[1]['parent-snapshot-id']).toBe(v2['current-snapshot-id'])

    const read = await icebergRead({ tableUrl, metadata: v3, resolver })
    expect(read).toEqual([
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
      { id: 1n, name: 'alice' },
    ])
  })

  it('honors write.parquet.compression-codec=none', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/codec-none'
    const { resolver, files } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const uncompressed = {
      ...created,
      properties: { ...created.properties, 'write.parquet.compression-codec': 'none' },
    }
    const records = [{ id: 1n, name: 'alice' }, { id: 2n, name: 'bob' }]

    const staged = await icebergStageAppend({ tableUrl, metadata: uncompressed, records, resolver })
    const committed = await fileCatalogCommit({ tableUrl, metadata: uncompressed, staged, resolver })

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)

    // PAR1 magic bytes plus utf8-encoded "alice"/"bob" appear verbatim in an uncompressed file.
    const dataPath = staged.writtenFiles.find(p => p.endsWith('.parquet'))
    if (!dataPath) throw new Error('no parquet file written')
    const bytes = files.get(dataPath)
    if (!bytes) throw new Error(`no bytes for ${dataPath}`)
    const text = new TextDecoder('utf-8', { fatal: false }).decode(bytes)
    expect(text).toContain('alice')
    expect(text).toContain('bob')
  })

  it('accepts write.format.default=parquet', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/format-parquet'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const withFormat = {
      ...created,
      properties: { ...created.properties, 'write.format.default': 'parquet' },
    }
    const records = [{ id: 1n, name: 'alice' }]

    const staged = await icebergStageAppend({ tableUrl, metadata: withFormat, records, resolver })
    const committed = await fileCatalogCommit({ tableUrl, metadata: withFormat, staged, resolver })

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('rejects an unsupported write.format.default', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/format-bad'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const avroFormat = {
      ...created,
      properties: { ...created.properties, 'write.format.default': 'avro' },
    }

    await expect(icebergStageAppend({
      tableUrl, metadata: avroFormat, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })).rejects.toThrow(/unsupported write\.format\.default: avro/)
  })

  it('rejects an unsupported write.parquet.compression-codec', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/codec-bad'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const gzipped = {
      ...created,
      properties: { ...created.properties, 'write.parquet.compression-codec': 'gzip' },
    }

    await expect(icebergStageAppend({
      tableUrl, metadata: gzipped, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })).rejects.toThrow(/unsupported write\.parquet\.compression-codec: gzip/)
  })

  it('rejects appending to a v2 table whose schema uses a v3-only type', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/v3-type-in-v2'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const tampered = {
      ...created,
      schemas: [{
        type: 'struct',
        'schema-id': 0,
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          // forged: a v3-only type smuggled into a v2 metadata
          { id: 2, name: 'doc', required: false, type: /** @type {any} */ ('variant') },
        ],
      }],
    }

    await expect(icebergStageAppend({
      tableUrl, metadata: tampered, resolver,
      records: [{ id: 1n, doc: { x: 1 } }],
    })).rejects.toThrow(/type variant requires format-version 3/)
  })
})

describe('fileCatalogCommit', () => {
  it('rejects a stale CAS (assert-ref-snapshot-id mismatch)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cas'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })

    // writer A stages against the empty table
    const stagedA = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })

    // writer B commits first, advancing the table
    const stagedB = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 2n, name: 'bob' }],
    })
    const after = await fileCatalogCommit({ tableUrl, metadata: created, staged: stagedB, resolver })

    // writer A's commit must now fail — its requirement still asserts null parent
    await expect(fileCatalogCommit({
      tableUrl, metadata: after, staged: stagedA, resolver,
    })).rejects.toThrow(/ref main expected snapshot null/)
  })

  it('caps metadata-log via write.metadata.previous-versions-max', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/log-cap'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const capped = {
      ...created,
      properties: { ...created.properties, 'write.metadata.previous-versions-max': '2' },
    }

    let metadata = capped
    for (let i = 0; i < 4; i++) {
      const staged = await icebergStageAppend({
        tableUrl, metadata, resolver,
        records: [{ id: BigInt(i), name: `r${i}` }],
      })
      metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
    }

    // 4 commits → log entries point at v1..v4; capped at 2 keeps the last two
    const log = metadata['metadata-log'] ?? []
    expect(log).toHaveLength(2)
    expect(log[0]['metadata-file']).toMatch(/v3\.metadata\.json$/)
    expect(log[1]['metadata-file']).toMatch(/v4\.metadata\.json$/)

    // version derivation must survive truncation: a fifth commit writes v6
    const staged = await icebergStageAppend({
      tableUrl, metadata, resolver,
      records: [{ id: 99n, name: 'tail' }],
    })
    const next = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
    const nextLog = next['metadata-log'] ?? []
    expect(nextLog).toHaveLength(2)
    expect(nextLog[1]['metadata-file']).toMatch(/v5\.metadata\.json$/)
  })

  it('applies set-properties and remove-properties updates', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/props'
    const { resolver } = memResolver()

    const created = await icebergCreate({
      tableUrl, resolver, schema,
      properties: { 'write.format.default': 'parquet', 'owner': 'alice' },
    })

    const setStaged = {
      snapshot: /** @type {any} */ (null),
      requirements: [{ type: /** @type {const} */ ('assert-table-uuid'), uuid: created['table-uuid'] }],
      updates: [
        { action: /** @type {const} */ ('set-properties'), updates: { 'owner': 'bob', 'comment': 'hello' } },
      ],
      writtenFiles: [],
    }
    const afterSet = await fileCatalogCommit({ tableUrl, metadata: created, staged: setStaged, resolver })
    expect(afterSet.properties).toEqual({
      'write.format.default': 'parquet',
      'owner': 'bob',
      'comment': 'hello',
    })

    const removeStaged = {
      snapshot: /** @type {any} */ (null),
      requirements: [{ type: /** @type {const} */ ('assert-table-uuid'), uuid: created['table-uuid'] }],
      updates: [
        { action: /** @type {const} */ ('remove-properties'), removals: ['owner', 'missing'] },
      ],
      writtenFiles: [],
    }
    const afterRemove = await fileCatalogCommit({ tableUrl, metadata: afterSet, staged: removeStaged, resolver })
    expect(afterRemove.properties).toEqual({
      'write.format.default': 'parquet',
      'comment': 'hello',
    })
  })

  it('applies add-schema and set-current-schema updates', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/schema'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })

    /** @type {Schema} */
    const v2 = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...schema.fields,
        { id: 99, name: 'tag', required: false, type: 'string', 'write-default': 'unknown' },
      ],
    }
    const staged = {
      snapshot: /** @type {any} */ (null),
      requirements: [{ type: /** @type {const} */ ('assert-table-uuid'), uuid: created['table-uuid'] }],
      updates: [
        { action: /** @type {const} */ ('add-schema'), schema: v2 },
        { action: /** @type {const} */ ('set-current-schema'), 'schema-id': -1 },
      ],
      writtenFiles: [],
    }
    const after = await fileCatalogCommit({ tableUrl, metadata: created, staged, resolver })

    expect(after.schemas).toHaveLength(2)
    expect(after.schemas[1]['schema-id']).toBe(1) // -1 → next id after the seeded schema
    expect(after.schemas[1].fields.find(f => f.name === 'tag')?.['write-default']).toBe('unknown')
    expect(after['current-schema-id']).toBe(1)
    expect(after['last-column-id']).toBe(99)
  })

  it('rejects add-schema with write-default on a v2 table', () => {
    /** @type {TableMetadata} */
    const meta = {
      'format-version': 2,
      'table-uuid': 'u',
      location: 'http://test',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
    }
    /** @type {Schema} */
    const schemaWithDefault = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        { id: 2, name: 'tag', required: false, type: 'string', 'write-default': 'unknown' },
      ],
    }
    expect(() => applyUpdates(meta, [
      { action: 'add-schema', schema: schemaWithDefault },
    ])).toThrow(/write-default requires format-version 3/)
  })

  it('rejects add-schema when a new required field lacks initial-default', () => {
    /** @type {TableMetadata} */
    const meta = {
      'format-version': 3,
      'table-uuid': 'u',
      location: 'http://test',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{
        type: 'struct',
        'schema-id': 0,
        fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
      }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
      'next-row-id': 0,
    }
    /** @type {Schema} */
    const newRequiredNoDefault = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'tag', required: true, type: 'string' },
      ],
    }
    expect(() => applyUpdates(meta, [
      { action: 'add-schema', schema: newRequiredNoDefault },
    ])).toThrow(/required field tag .* needs an initial-default/)

    // carrying over an existing required field (id <= last-column-id) is fine
    /** @type {Schema} */
    const sameRequired = {
      type: 'struct',
      'schema-id': -1,
      fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
    }
    expect(() => applyUpdates(meta, [
      { action: 'add-schema', schema: sameRequired },
    ])).not.toThrow()

    // adding a new required field with initial-default is allowed
    /** @type {Schema} */
    const newRequiredWithDefault = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'tag', required: true, type: 'string', 'initial-default': 'unknown' },
      ],
    }
    expect(() => applyUpdates(meta, [
      { action: 'add-schema', schema: newRequiredWithDefault },
    ])).not.toThrow()
  })

  it('rejects add-schema with a duplicate schema-id', () => {
    /** @type {TableMetadata} */
    const meta = {
      'format-version': 2,
      'table-uuid': 'u',
      location: 'http://test',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
    }
    expect(() => applyUpdates(meta, [
      { action: 'add-schema', schema: { type: 'struct', 'schema-id': 0, fields: [] } },
    ])).toThrow(/schema-id 0 already exists/)
  })

  it('checks assert-current-schema-id and assert-last-assigned-field-id', () => {
    /** @type {TableMetadata} */
    const meta = {
      'format-version': 2,
      'table-uuid': 'u',
      location: 'http://test',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 5,
      'current-schema-id': 2,
      schemas: [{ type: 'struct', 'schema-id': 2, fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
    }
    // matching values pass
    expect(() => checkRequirements(meta, [
      { type: 'assert-current-schema-id', 'current-schema-id': 2 },
      { type: 'assert-last-assigned-field-id', 'last-assigned-field-id': 5 },
    ])).not.toThrow()
    // mismatches throw
    expect(() => checkRequirements(meta, [
      { type: 'assert-current-schema-id', 'current-schema-id': 1 },
    ])).toThrow(/current-schema-id expected 1, got 2/)
    expect(() => checkRequirements(meta, [
      { type: 'assert-last-assigned-field-id', 'last-assigned-field-id': 4 },
    ])).toThrow(/last-assigned-field-id expected 4, got 5/)
  })

  it('checks assert-default-spec-id, assert-default-sort-order-id, assert-last-assigned-partition-id', () => {
    /** @type {TableMetadata} */
    const meta = {
      'format-version': 2,
      'table-uuid': 'u',
      location: 'http://test',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 5,
      'current-schema-id': 0,
      schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
      'default-spec-id': 3,
      'partition-specs': [{ 'spec-id': 3, fields: [] }],
      'last-partition-id': 1007,
      'sort-orders': [{ 'order-id': 4, fields: [] }],
      'default-sort-order-id': 4,
    }
    // matching values pass
    expect(() => checkRequirements(meta, [
      { type: 'assert-default-spec-id', 'default-spec-id': 3 },
      { type: 'assert-default-sort-order-id', 'default-sort-order-id': 4 },
      { type: 'assert-last-assigned-partition-id', 'last-assigned-partition-id': 1007 },
    ])).not.toThrow()
    // mismatches throw
    expect(() => checkRequirements(meta, [
      { type: 'assert-default-spec-id', 'default-spec-id': 0 },
    ])).toThrow(/default-spec-id expected 0, got 3/)
    expect(() => checkRequirements(meta, [
      { type: 'assert-default-sort-order-id', 'default-sort-order-id': 0 },
    ])).toThrow(/default-sort-order-id expected 0, got 4/)
    expect(() => checkRequirements(meta, [
      { type: 'assert-last-assigned-partition-id', 'last-assigned-partition-id': 1000 },
    ])).toThrow(/last-assigned-partition-id expected 1000, got 1007/)
  })

  it('assert-create always fails against existing metadata', () => {
    /** @type {TableMetadata} */
    const meta = {
      'format-version': 2,
      'table-uuid': 'u',
      location: 'http://test',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 0,
      'current-schema-id': 0,
      schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
    }
    expect(() => checkRequirements(meta, [
      { type: 'assert-create' },
    ])).toThrow(/assert-create against an existing table/)
  })

  it('rejects set-current-schema with an unknown schema-id', () => {
    /** @type {TableMetadata} */
    const meta = {
      'format-version': 2,
      'table-uuid': 'u',
      location: 'http://test',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
    }
    expect(() => applyUpdates(meta, [
      { action: 'set-current-schema', 'schema-id': 7 },
    ])).toThrow(/schema-id 7 not found/)
  })

  it('rejects a table-uuid mismatch', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/uuid'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const staged = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })

    const wrongUuid = { ...created, 'table-uuid': '00000000-0000-0000-0000-000000000000' }
    await expect(fileCatalogCommit({
      tableUrl, metadata: wrongUuid, staged, resolver,
    })).rejects.toThrow(/table-uuid expected/)
  })
})
