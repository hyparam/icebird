import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergCreate } from '../../src/create.js'
import { icebergAppend, icebergCreateTable } from '../../src/write/write.js'
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

describe('fileCatalogCommit conditional create', () => {
  it('passes ifNoneMatch on the new metadata file when enabled', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-flag'
    const { resolver } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')

    /** @type {{ path: string, ifNoneMatch?: string }[]} */
    const calls = []
    resolver.writer = (p, options) => {
      calls.push({ path: p, ifNoneMatch: options?.ifNoneMatch })
      return realWriter(p, options)
    }
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    await icebergCreate({ tableUrl, resolver, schema })
    calls.length = 0
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })

    const v2 = calls.find(c => c.path.endsWith('/metadata/v2.metadata.json'))
    const hint = calls.find(c => c.path.endsWith('/metadata/version-hint.text'))
    expect(v2?.ifNoneMatch).toBe('*')
    expect(hint?.ifNoneMatch).toBeUndefined()
  })

  it('writes without ifNoneMatch by default', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-default'
    const { resolver } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')

    /** @type {{ path: string, ifNoneMatch?: string }[]} */
    const calls = []
    resolver.writer = (p, options) => {
      calls.push({ path: p, ifNoneMatch: options?.ifNoneMatch })
      return realWriter(p, options)
    }
    const catalog = fileCatalog({ resolver })

    await icebergCreate({ tableUrl, resolver, schema })
    calls.length = 0
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })

    expect(calls.every(c => c.ifNoneMatch === undefined)).toBe(true)
  })

  it('retries on 412 and commits the next free version', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-retry'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    // v1: create. v2: a real "concurrent" append we use as the planted commit.
    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    const v2Bytes = files.get(`${tableUrl}/metadata/v2.metadata.json`)
    if (!v2Bytes) throw new Error('v2 missing')

    // Set up a second writer that staged against v1 (stale ctx). Easiest
    // simulation: roll the catalog back to v1 by capturing v1 metadata and
    // injecting a wrapper resolver that plants v3 right before the staged
    // writer hits finish() on v3 — forcing a 412, then a retry against the
    // real latest (still v2).
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    let plantedOnce = false
    /** @type {import('../../src/types.js').Resolver} */
    const racing = {
      ...resolver,
      writer(p, options) {
        if (p.endsWith('/metadata/v3.metadata.json') && options?.ifNoneMatch === '*' && !plantedOnce) {
          plantedOnce = true
          // Plant a foreign v3 *before* our writer reaches the conditional
          // PUT. memResolver's writer() rejects with 412 on collision.
          files.set(p, v2Bytes)
        }
        return realWriter(p, options)
      },
    }
    const racingCatalog = fileCatalog({ resolver: racing, conditionalCommits: true })

    const committed = await icebergAppend({
      catalog: racingCatalog, tableUrl,
      records: [{ id: 2n, name: 'b' }],
    })

    expect(plantedOnce).toBe(true)
    // After the 412 on v3 + retry, the writer commits v4 (the real "next
    // free version" once the planted v3 is observed as latest).
    expect(files.has(`${tableUrl}/metadata/v4.metadata.json`)).toBe(true)
    expect(committed['current-snapshot-id']).toBeDefined()
  })

  it('overwrites v2 by default (backwards compatible)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-overwrite-default'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreateTable({ catalog, tableUrl, schema })
    files.set(`${tableUrl}/metadata/v2.metadata.json`, new TextEncoder().encode('{}'))

    const committed = await icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })
    expect(committed.snapshots).toHaveLength(1)
    // overwrite happened
    const v2 = files.get(`${tableUrl}/metadata/v2.metadata.json`)
    expect(v2 && v2.byteLength).toBeGreaterThan(2)
  })

  it('hint failure does not fail the commit when conditionalCommits is on', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-hint-fail'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    // First, create the table successfully (hint write must work for create
    // because we install the failure-injecting writer afterward).
    await icebergCreateTable({ catalog, tableUrl, schema })

    // Now make subsequent hint writes fail.
    resolver.writer = (p, options) => {
      const w = realWriter(p, options)
      if (p.endsWith('/metadata/version-hint.text')) {
        w.finish = async () => { throw new Error('hint blocked: 503') }
      }
      return w
    }

    const committed = await icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })
    expect(committed.snapshots).toHaveLength(1)
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    // Hint left at v1 (unchanged from create).
    const hint = files.get(`${tableUrl}/metadata/version-hint.text`)
    expect(hint && new TextDecoder().decode(hint)).toBe('1')
  })

  it('hint failure still propagates without conditionalCommits', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-hint-fail-default'
    const { resolver } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    const catalog = fileCatalog({ resolver })

    await icebergCreateTable({ catalog, tableUrl, schema })

    resolver.writer = (p, options) => {
      const w = realWriter(p, options)
      if (p.endsWith('/metadata/version-hint.text')) {
        w.finish = async () => { throw new Error('hint blocked: 503') }
      }
      return w
    }

    await expect(icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })).rejects.toThrow(/hint blocked/)
  })

  it('concurrent appends: all eventually succeed via retry under conditionalCommits', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-race'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })

    const results = await Promise.allSettled([
      icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 3n, name: 'c' }] }),
    ])

    expect(results.every(r => r.status === 'fulfilled')).toBe(true)
    // Each retry produced one new vN, so v2/v3/v4 all exist.
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/v3.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/v4.metadata.json`)).toBe(true)
  })

  it('exhausts retries and throws when conflicts persist', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/cond-commit-exhaust'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')

    // Create v1 with the plain resolver so we have a valid metadata body.
    // Pin a low retry cap and zero back-off via table properties — the
    // default 50 attempts × 3s would dominate the test run. We're verifying
    // the exhaustion path, not the policy.
    await icebergCreateTable({
      catalog: fileCatalog({ resolver, conditionalCommits: true }),
      tableUrl, schema,
      properties: {
        'commit.retry.num-retries': '5',
        'commit.retry.min-wait-ms': '0',
        'commit.retry.max-wait-ms': '0',
      },
    })
    const v1 = files.get(`${tableUrl}/metadata/v1.metadata.json`)
    if (!v1) throw new Error('v1 missing')

    // Now wrap the resolver: every conditional PUT against vN sees a planted
    // vN beat it. The planted bytes are a copy of v1, so the retry's
    // loadLatest can still parse and re-stage — but the next conditional PUT
    // collides again. After maxAttempts the loop bails with our message.
    /** @type {import('../../src/types.js').Resolver} */
    const alwaysConflicts = {
      ...resolver,
      writer(p, options) {
        if (/\/metadata\/v\d+\.metadata\.json$/.test(p) && options?.ifNoneMatch === '*') {
          files.set(p, v1)
        }
        return realWriter(p, options)
      },
    }
    const cat = fileCatalog({ resolver: alwaysConflicts, conditionalCommits: true })

    await expect(icebergAppend({
      catalog: cat, tableUrl, records: [{ id: 1n, name: 'a' }],
    })).rejects.toThrow(/6 attempts due to concurrent commits/)
  })
})
