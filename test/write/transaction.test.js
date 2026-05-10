import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergCreate } from '../../src/create.js'
import { icebergMetadata } from '../../src/metadata.js'
import { icebergRead } from '../../src/read.js'
import { icebergAppend, icebergTransaction } from '../../src/write/write.js'
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

describe('icebergTransaction', () => {
  it('chains two appends in one commit and produces both snapshots', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/tx-append-append'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const baseline = before['current-snapshot-id']

    const committed = await icebergTransaction({ catalog, tableUrl }, async tx => {
      await tx.append({ records: [{ id: 2n, name: 'b' }] })
      await tx.append({ records: [{ id: 3n, name: 'c' }] })
    })

    expect(committed.snapshots).toHaveLength(3)
    const tip = committed.snapshots?.[2]
    expect(tip?.['parent-snapshot-id']).toBe(committed.snapshots?.[1]['snapshot-id'])
    expect(committed.snapshots?.[1]['parent-snapshot-id']).toBe(baseline)
    expect(committed['current-snapshot-id']).toBe(tip?.['snapshot-id'])

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual([
      { id: 1n, name: 'a' },
      { id: 2n, name: 'b' },
      { id: 3n, name: 'c' },
    ])
  })

  it('mixes append + delete + setRef in one transaction', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/tx-mixed'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const seeded = await icebergAppend({
      catalog, tableUrl,
      records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }, { id: 3n, name: 'c' }],
    })
    const seededTip = /** @type {number} */ (seeded['current-snapshot-id'])
    const dataPath = [...files.keys()].find(k => /\/data\/[^/]+\.parquet$/.test(k) && !k.endsWith('-deletes.parquet'))
    if (!dataPath) throw new Error('no data file found')

    const committed = await icebergTransaction({ catalog, tableUrl }, async tx => {
      await tx.delete({ deletes: [{ file_path: dataPath, pos: 1 }] })
      await tx.append({ records: [{ id: 4n, name: 'd' }] })
      tx.setRef({ ref: 'before-tx', snapshotId: seededTip, type: 'tag' })
    })

    expect(committed.snapshots).toHaveLength(3)
    expect(committed.snapshots?.[1].summary.operation).toBe('delete')
    expect(committed.snapshots?.[2].summary.operation).toBe('append')
    expect(committed.refs?.['before-tx']).toEqual({ 'snapshot-id': seededTip, type: 'tag' })

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual([
      { id: 1n, name: 'a' },
      { id: 3n, name: 'c' },
      { id: 4n, name: 'd' },
    ])
  })

  it('returns the original metadata when the callback stages nothing', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/tx-empty'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const sizeBefore = files.size

    const after = await icebergTransaction({ catalog, tableUrl }, () => {
      // no ops
    })

    expect(after['current-snapshot-id']).toBe(before['current-snapshot-id'])
    expect(files.size).toBe(sizeBefore)
  })

  it('does not commit when the callback throws', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/tx-throws'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    const before = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const beforeTip = before['current-snapshot-id']

    await expect(icebergTransaction({ catalog, tableUrl }, async tx => {
      await tx.append({ records: [{ id: 2n, name: 'b' }] })
      throw new Error('rollback')
    })).rejects.toThrow(/rollback/)

    const reread = await icebergMetadata({ tableUrl, resolver })
    expect(reread['current-snapshot-id']).toBe(beforeTip)
    expect(reread.snapshots).toHaveLength(1)
  })

  it('rest catalog: chains snapshots through a single commit POST with one CAS', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/tx-rest'
    const { resolver } = memResolver()
    const fileCat = fileCatalog({ resolver })

    // Seed a real on-disk table; the fake REST catalog returns its metadata
    // on load and accepts the (single) commit POST.
    await icebergCreate({ tableUrl, resolver, schema })
    const seeded = await icebergAppend({ catalog: fileCat, tableUrl, records: [{ id: 1n, name: 'a' }] })

    let commitCalls = 0
    /** @type {any} */
    let lastBody
    vi.stubGlobal('fetch', async (/** @type {string} */ url, /** @type {RequestInit | undefined} */ init) => {
      if (url === 'https://cat/v1/config') return new Response(JSON.stringify({}), { status: 200 })
      if (url === 'https://cat/v1/namespaces/db/tables/t' && (!init || !init.method || init.method === 'GET')) {
        return new Response(JSON.stringify({
          'metadata-location': `${tableUrl}/metadata/v2.metadata.json`,
          metadata: seeded,
        }), { status: 200 })
      }
      if (url === 'https://cat/v1/namespaces/db/tables/t' && init?.method === 'POST') {
        commitCalls++
        lastBody = JSON.parse(/** @type {string} */ (init.body))
        return new Response(JSON.stringify({
          'metadata-location': `${tableUrl}/metadata/v3.metadata.json`,
          metadata: seeded,
        }), { status: 200 })
      }
      throw new Error(`unexpected url: ${url} (${init?.method ?? 'GET'})`)
    })

    const { restCatalogConnect } = await import('../../src/catalog/rest.js')
    const ctx = await restCatalogConnect({ url: 'https://cat' })

    await icebergTransaction({ catalog: ctx, namespace: 'db', table: 't', resolver }, async tx => {
      await tx.append({ records: [{ id: 2n, name: 'b' }] })
      await tx.append({ records: [{ id: 3n, name: 'c' }] })
    })

    expect(commitCalls).toBe(1)
    expect(lastBody.updates.filter((/** @type {any} */ u) => u.action === 'add-snapshot')).toHaveLength(2)
    const mainCas = lastBody.requirements.filter((/** @type {any} */ r) => r.type === 'assert-ref-snapshot-id' && r.ref === 'main')
    expect(mainCas).toHaveLength(1)
    expect(mainCas[0]['snapshot-id']).toBe(seeded['current-snapshot-id'])

    vi.unstubAllGlobals()
  })

  // Transactions deliberately opt out of the conditional-commit retry loop:
  // re-running the user's callback could repeat side effects. Two concurrent
  // transactions therefore race for the same vN+1, and the loser surfaces
  // the resolver's raw 412. Callers that want retry semantics around a
  // transaction must implement it themselves and ensure callback idempotence.
  it('does not retry on conflict under conditionalCommits (caller wraps)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/tx-noretry'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    await icebergCreate({ tableUrl, resolver, schema })

    const results = await Promise.allSettled([
      icebergTransaction({ catalog, tableUrl }, async tx => {
        await tx.append({ records: [{ id: 1n, name: 'a' }] })
      }),
      icebergTransaction({ catalog, tableUrl }, async tx => {
        await tx.append({ records: [{ id: 2n, name: 'b' }] })
      }),
    ])
    const failed = results.filter(r => r.status === 'rejected')
    expect(failed).toHaveLength(1)
    /** @type {any} */
    const err = /** @type {PromiseRejectedResult} */ (failed[0]).reason
    expect(err.status).toBe(412)
  })
})
