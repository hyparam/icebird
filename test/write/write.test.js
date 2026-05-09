import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'
import { icebergAppend, icebergCreateTable, icebergDelete, icebergDropTable, icebergExpireSnapshots, icebergSetRef } from '../../src/write/write.js'
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

  it('exposes conditionalCommits when set', () => {
    const { resolver } = memResolver()
    const cat = fileCatalog({ resolver, conditionalCommits: true })
    expect(cat.conditionalCommits).toBe(true)
    expect(Object.isFrozen(cat)).toBe(true)
  })

  it('omits conditionalCommits when not set', () => {
    const { resolver } = memResolver()
    const cat = fileCatalog({ resolver })
    expect('conditionalCommits' in cat).toBe(false)
  })

  it('throws when resolver is missing', () => {
    // @ts-expect-error
    expect(() => fileCatalog({})).toThrow(/resolver is required/)
  })
})

describe('icebergCreateTable', () => {
  it('writes v1.metadata.json and version-hint.text via the file catalog resolver', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/create-file'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    const metadata = await icebergCreateTable({ catalog, tableUrl, schema })

    expect(metadata['format-version']).toBe(2)
    expect(metadata.location).toBe(tableUrl)
    expect(files.has(`${tableUrl}/metadata/v1.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/version-hint.text`)).toBe(true)
  })

  it('throws when tableUrl is missing on a file catalog', async () => {
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await expect(icebergCreateTable({ catalog, schema }))
      .rejects.toThrow(/tableUrl is required/)
  })

  it('rest catalog: POSTs name + schema and returns metadata', async () => {
    const created = {
      'format-version': 2,
      'table-uuid': 'abc',
      location: 's3://bucket/orders',
      schemas: [schema],
    }
    /** @type {{url: string, init?: RequestInit}[]} */
    const calls = []
    vi.stubGlobal('fetch', async (/** @type {string} */ url, /** @type {RequestInit | undefined} */ init) => {
      calls.push({ url, init })
      if (url === 'https://cat/v1/config') return new Response(JSON.stringify({}), { status: 200 })
      if (url === 'https://cat/v1/namespaces/db/tables') {
        return new Response(JSON.stringify({
          'metadata-location': 's3://bucket/orders/metadata/v1.metadata.json',
          metadata: created,
        }), { status: 200 })
      }
      throw new Error(`unexpected url: ${url}`)
    })

    const { restCatalogConnect } = await import('../../src/catalog/rest.js')
    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const metadata = await icebergCreateTable({ catalog: ctx, namespace: 'db', table: 'orders', schema })

    expect(metadata).toEqual(created)
    const post = calls.find(c => c.url === 'https://cat/v1/namespaces/db/tables')
    expect(post?.init?.method).toBe('POST')
    expect(JSON.parse(/** @type {string} */ (post?.init?.body))).toEqual({ name: 'orders', schema })
    vi.unstubAllGlobals()
  })

  it('rest catalog: requires namespace, table, and schema', async () => {
    const ctx = /** @type {any} */ ({ type: 'rest', url: 'https://cat', prefix: '', defaults: {}, overrides: {} })
    await expect(icebergCreateTable({ catalog: ctx, table: 'orders', schema }))
      .rejects.toThrow(/namespace and table are required/)
    await expect(icebergCreateTable({ catalog: ctx, namespace: 'db', table: 'orders' }))
      .rejects.toThrow(/schema is required/)
  })
})

describe('icebergDropTable', () => {
  it('file catalog: lists metadata/ and deletes every file', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/drop-file'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    expect(files.has(`${tableUrl}/metadata/version-hint.text`)).toBe(true)

    await icebergDropTable({ catalog, tableUrl, lister })

    expect([...files.keys()].some(k => k.startsWith(`${tableUrl}/metadata/`))).toBe(false)
    // data/ untouched without purgeRequested
    expect([...files.keys()].some(k => k.startsWith(`${tableUrl}/data/`))).toBe(true)
  })

  it('file catalog with purgeRequested: also deletes data/', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/drop-file-purge'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })

    await icebergDropTable({ catalog, tableUrl, lister, purgeRequested: true })

    expect([...files.keys()].some(k => k.startsWith(`${tableUrl}/`))).toBe(false)
  })

  it('file catalog: throws when lister is missing', async () => {
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await expect(icebergDropTable({ catalog, tableUrl: 'http://test/drop-no-lister' }))
      .rejects.toThrow(/lister is required/)
  })

  it('file catalog: throws when tableUrl is missing', async () => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver })
    await expect(icebergDropTable({ catalog, lister }))
      .rejects.toThrow(/tableUrl is required/)
  })

  it('rest catalog: issues DELETE and forwards purgeRequested', async () => {
    /** @type {{url: string, init?: RequestInit}[]} */
    const calls = []
    vi.stubGlobal('fetch', async (/** @type {string} */ url, /** @type {RequestInit | undefined} */ init) => {
      calls.push({ url, init })
      if (url === 'https://cat/v1/config') return new Response(JSON.stringify({}), { status: 200 })
      return new Response(null, { status: 204 })
    })

    const { restCatalogConnect } = await import('../../src/catalog/rest.js')
    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await icebergDropTable({ catalog: ctx, namespace: 'db', table: 'orders', purgeRequested: true })

    expect(calls.some(c =>
      c.url === 'https://cat/v1/namespaces/db/tables/orders?purgeRequested=true' &&
      c.init?.method === 'DELETE'
    )).toBe(true)
    vi.unstubAllGlobals()
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

  it('records the prior metadata-file using its real on-disk name (java/rust/python style)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/append-extwriter'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister })

    // Bootstrap with Icebird, then rename the resulting metadata file to
    // mimic iceberg-java/rust/python's `NNNNN-<uuid>.metadata.json` naming
    // and update version-hint accordingly. Drop `v1.metadata.json` so the
    // commit path can't accidentally fall back to a synthesized filename
    // that happens to exist.
    await icebergCreate({ tableUrl, resolver, schema })
    const original = `${tableUrl}/metadata/v1.metadata.json`
    const renamed = '00000-aa56fadd-dad7-4958-840f-9198034d74f0.metadata.json'
    const renamedPath = `${tableUrl}/metadata/${renamed}`
    files.set(renamedPath, /** @type {Uint8Array} */ (files.get(original)))
    files.delete(original)
    files.set(`${tableUrl}/metadata/version-hint.text`, new TextEncoder().encode('0'))

    const committed = await icebergAppend({
      catalog, tableUrl,
      records: [{ id: 1n, name: 'alice' }],
    })

    const log = committed['metadata-log'] ?? []
    expect(log).toHaveLength(1)
    expect(log[0]['metadata-file']).toBe(renamedPath)
    // Sanity: the entry should point at a file that actually exists on disk.
    expect(files.has(log[0]['metadata-file'])).toBe(true)
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

  it('reads rowStart/rowEnd in post-delete coordinates for v3 deletion vectors', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/delete-v3-range'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })
    const records = [
      { id: 1n, name: 'alice' },
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
      { id: 4n, name: 'dan' },
    ]
    await icebergAppend({ catalog, tableUrl, records })
    const dataPath = [...findDataFiles({ files })][0]
    const committed = await icebergDelete({
      catalog, tableUrl,
      deletes: [{ file_path: dataPath, pos: 1 }],
    })

    const full = await icebergRead({ tableUrl, metadata: committed, resolver })
    const ranged = await icebergRead({ tableUrl, metadata: committed, resolver, rowStart: 1, rowEnd: 3 })

    expect(full.map(r => r.id)).toEqual([1n, 3n, 4n])
    expect(ranged.map(r => r.id)).toEqual(full.slice(1, 3).map(r => r.id))
  })

  it('rejects parquet delete mode on v3', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/delete-override'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }] })
    const dataPath = [...findDataFiles({ files })][0]

    await expect(icebergDelete({
      catalog, tableUrl,
      deletes: [{ file_path: dataPath, pos: 0 }],
      mode: 'parquet',
    })).rejects.toThrow(/deletion vectors/)
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
