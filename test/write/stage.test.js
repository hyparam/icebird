import { describe, expect, it, vi } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'
import { icebergStageAppend } from '../../src/write/stage.js'

/**
 * @import {Resolver, Schema} from '../../src/types.js'
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
