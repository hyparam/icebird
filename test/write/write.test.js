import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'
import { icebergAppend, icebergDelete, icebergExpireSnapshots, icebergSetRef } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema} from '../../src/types.js'
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

describe('fileCatalog', () => {
  it('returns a frozen, type-discriminated catalog object', () => {
    const { resolver } = memResolver()
    const cat = fileCatalog({ resolver })
    expect(cat).toEqual({ type: 'file', resolver })
    expect(Object.isFrozen(cat)).toBe(true)
  })

  it('throws when resolver is missing', () => {
    // @ts-expect-error
    expect(() => fileCatalog({})).toThrow(/resolver is required/)
  })
})

describe('icebergAppend', () => {
  it('appends rows and round-trips through icebergRead', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/append1'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const records = [{ id: 1n, name: 'alice' }, { id: 2n, name: 'bob' }]
    const committed = await icebergAppend({ catalog, tableUrl, records })

    expect(committed['current-snapshot-id']).toBeDefined()
    expect(committed.snapshots).toHaveLength(1)
    expect(committed.snapshots?.[0].summary.operation).toBe('append')

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('chains multiple appends', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/append2'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'alice' }] })
    const committed = await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'bob' }] })

    expect(committed.snapshots).toHaveLength(2)
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual([{ id: 1n, name: 'alice' }, { id: 2n, name: 'bob' }])
  })

  it('throws when tableUrl is missing on a file catalog', async () => {
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await expect(icebergAppend({ catalog, records: [] }))
      .rejects.toThrow(/tableUrl is required/)
  })
})

describe('icebergDelete', () => {
  it('parquet mode (v2): removes targeted rows', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/delete-v2'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const records = [
      { id: 1n, name: 'alice' },
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
    ]
    await icebergAppend({ catalog, tableUrl, records })
    const dataPath = [...findDataFiles({ files })][0]
    const committed = await icebergDelete({
      catalog, tableUrl,
      deletes: [{ file_path: dataPath, pos: 1 }],
    })

    expect(committed.snapshots).toHaveLength(2)
    expect(committed.snapshots?.[1].summary.operation).toBe('delete')
    expect(committed.snapshots?.[1].summary['added-position-deletes']).toBe('1')

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual([
      { id: 1n, name: 'alice' },
      { id: 3n, name: 'carol' },
    ])
  })

  it('puffin mode (v3): writes a deletion vector and removes targeted rows', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/delete-v3'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })
    const records = [
      { id: 1n, name: 'alice' },
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
    ]
    await icebergAppend({ catalog, tableUrl, records })
    const dataPath = [...findDataFiles({ files })][0]

    const committed = await icebergDelete({
      catalog, tableUrl,
      deletes: [{ file_path: dataPath, pos: 0 }, { file_path: dataPath, pos: 2 }],
    })

    expect(committed['format-version']).toBe(3)
    expect([...files.keys()].some(k => k.endsWith('.puffin'))).toBe(true)

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read.map(r => ({ id: r.id, name: r.name }))).toEqual([{ id: 2n, name: 'bob' }])
  })

  it('mode override forces parquet on v3', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/delete-override'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }] })
    const dataPath = [...findDataFiles({ files })][0]

    await icebergDelete({
      catalog, tableUrl,
      deletes: [{ file_path: dataPath, pos: 0 }],
      mode: 'parquet',
    })

    expect([...files.keys()].some(k => k.endsWith('-deletes.parquet'))).toBe(true)
    expect([...files.keys()].some(k => k.endsWith('.puffin'))).toBe(false)
  })

  it('rejects unknown delete mode', async () => {
    const tableUrl = 'http://test/delete-bad-mode'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreate({ tableUrl, resolver, schema })

    await expect(icebergDelete({
      catalog, tableUrl,
      deletes: [{ file_path: 'x', pos: 0 }],
      // @ts-expect-error
      mode: 'bogus',
    })).rejects.toThrow(/unknown delete mode/)
  })
})

describe('icebergSetRef', () => {
  it('rolls main back to a prior snapshot', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/setref'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const afterFirst = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const firstSnap = /** @type {number} */ (afterFirst['current-snapshot-id'])
    await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] })

    const rolled = await icebergSetRef({ catalog, tableUrl, ref: 'main', snapshotId: firstSnap })
    expect(rolled['current-snapshot-id']).toBe(firstSnap)
    expect(rolled.refs?.main['snapshot-id']).toBe(firstSnap)

    const read = await icebergRead({ tableUrl, metadata: rolled, resolver })
    expect(read).toEqual([{ id: 1n, name: 'a' }])
  })

  it('creates a tag pointing at a snapshot', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/setref-tag'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const afterFirst = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const firstSnap = /** @type {number} */ (afterFirst['current-snapshot-id'])

    const tagged = await icebergSetRef({
      catalog, tableUrl,
      ref: 'v1', snapshotId: firstSnap, type: 'tag',
    })
    expect(tagged.refs?.v1).toEqual({ 'snapshot-id': firstSnap, type: 'tag' })
  })
})

describe('icebergExpireSnapshots', () => {
  it('removes the named snapshot from metadata', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/expire'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const afterFirst = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const firstSnap = /** @type {number} */ (afterFirst['current-snapshot-id'])
    await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] })
    // roll main off the first snapshot so it's expirable
    const rolled = await icebergAppend({ catalog, tableUrl, records: [{ id: 3n, name: 'c' }] })
    expect(rolled.snapshots).toHaveLength(3)

    const expired = await icebergExpireSnapshots({
      catalog, tableUrl, snapshotIds: [firstSnap],
    })
    expect(expired.snapshots?.some(s => s['snapshot-id'] === firstSnap)).toBe(false)
    expect(expired['snapshot-log']?.some(e => e['snapshot-id'] === firstSnap)).toBe(false)
  })

  it('rejects expiring the current snapshot', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/expire-current'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const after = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const tip = /** @type {number} */ (after['current-snapshot-id'])

    await expect(icebergExpireSnapshots({ catalog, tableUrl, snapshotIds: [tip] }))
      .rejects.toThrow(/referenced by branch main/)
  })
})

/**
 * Pull the data parquet file paths out of the in-memory file map, keyed by
 * the `/data/<uuid>.parquet` shape used by the writers.
 *
 * @param {{files: Map<string, Uint8Array>}} mem
 * @returns {string[]}
 */
function findDataFiles({ files }) {
  return [...files.keys()].filter(k => /\/data\/[^/]+\.parquet$/.test(k) && !k.endsWith('-deletes.parquet'))
}
