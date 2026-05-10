import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { loadLatestFileCatalogMetadata } from '../../src/metadata.js'
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

describe('loadLatestFileCatalogMetadata', () => {
  it('probes forward when version-hint is stale (says 2 but v3 exists)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stale-hint'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] })
    expect(files.has(`${tableUrl}/metadata/v3.metadata.json`)).toBe(true)

    // overwrite hint with a stale value
    files.set(`${tableUrl}/metadata/version-hint.text`, new TextEncoder().encode('2'))

    const latest = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(latest.version).toBe(3)
    expect(latest.metadataFileName).toBe('v3.metadata.json')
  })

  it('falls back to listing when version-hint is missing', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/missing-hint'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    files.delete(`${tableUrl}/metadata/version-hint.text`)

    const latest = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(latest.version).toBe(2)
    expect(latest.metadataFileName).toBe('v2.metadata.json')
  })

  it('returns v1 right after create', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/just-created'
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    await icebergCreateTable({ catalog, tableUrl, schema })

    const latest = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(latest.version).toBe(1)
    expect(latest.metadata['format-version']).toBe(2)
  })

  it('throws if no metadata files exist at all', async () => {
    const tableUrl = 'http://test/empty'
    const { resolver, lister } = memResolver()
    await expect(loadLatestFileCatalogMetadata({ tableUrl, resolver, lister }))
      .rejects.toThrow(/no metadata files/)
  })
})

describe('icebergAppend retry under conditionalCommits', () => {
  it('two writers staged against the same parent both eventually commit', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/two-staged'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })

    // Both calls load v1, stage against v1, race for v2. One wins, the other
    // 412s, reloads (sees v2), re-stages against v2, and writes v3.
    const [a, b] = await Promise.all([
      icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] }),
    ])

    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/v3.metadata.json`)).toBe(true)
    // The committed metadata reflects whatever ran last; both have a
    // current-snapshot-id and one of them sees 2 snapshots.
    const finalSnapCount = Math.max(a.snapshots?.length ?? 0, b.snapshots?.length ?? 0)
    expect(finalSnapCount).toBe(2)
  })

  it('does not retry when conditionalCommits is off (legacy overwrite)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/no-retry-default'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })

    await icebergCreateTable({ catalog, tableUrl, schema })

    // Two parallel appends without the flag — last one wins by overwrite. We
    // just verify both resolved and exactly v2 was written.
    await Promise.all([
      icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] }),
    ])

    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/v3.metadata.json`)).toBe(false)
  })

  it('10 concurrent writers all commit with no app-level retry', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/ten-writers'
    const { resolver, files } = memResolver()
    // Default policy with zeroed back-off so the test stays fast — what we're
    // verifying is that the retry budget alone (default 50 attempts) absorbs
    // 10 racing writers without losing any.
    const catalog = fileCatalog({
      resolver, conditionalCommits: true,
      commitRetry: { backoff: { initialMs: 0, maxMs: 0 } },
    })
    await icebergCreateTable({ catalog, tableUrl, schema })

    const N = 10
    const results = await Promise.all(
      Array.from({ length: N }, (_, i) =>
        icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i), name: `w${i}` }] })
      )
    )
    expect(results).toHaveLength(N)
    // Every writer added one snapshot; they serialized into v2..v(N+1).
    for (let v = 2; v <= N + 1; v++) {
      expect(files.has(`${tableUrl}/metadata/v${v}.metadata.json`)).toBe(true)
    }
    expect(files.has(`${tableUrl}/metadata/v${N + 2}.metadata.json`)).toBe(false)
  })

  it('respects a custom maxAttempts cap', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/custom-cap'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    await icebergCreateTable({
      catalog: fileCatalog({ resolver, conditionalCommits: true }),
      tableUrl, schema,
    })
    const v1 = files.get(`${tableUrl}/metadata/v1.metadata.json`)
    if (!v1) throw new Error('v1 missing')

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
    const catalog = fileCatalog({
      resolver: alwaysConflicts,
      conditionalCommits: true,
      commitRetry: { maxAttempts: 3, backoff: { initialMs: 0, maxMs: 0 } },
    })
    await expect(icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })).rejects.toThrow(/3 attempts due to concurrent commits/)
  })

  it('grows backoff exponentially between attempts', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/backoff-growth'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    await icebergCreateTable({
      catalog: fileCatalog({ resolver, conditionalCommits: true }),
      tableUrl, schema,
    })
    const v1 = files.get(`${tableUrl}/metadata/v1.metadata.json`)
    if (!v1) throw new Error('v1 missing')
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

    // Spy on setTimeout to capture every back-off delay the retry loop
    // requests. With Math.random pinned to 1, full-jitter degenerates to the
    // raw exponential ceiling: initial * factor^(attempt-1).
    /** @type {number[]} */
    const sleeps = []
    const realSetTimeout = globalThis.setTimeout
    const stSpy = vi.spyOn(globalThis, 'setTimeout').mockImplementation(
      /** @type {any} */ ((/** @type {Function} */ fn, /** @type {number} */ ms) => {
        sleeps.push(Number(ms))
        return realSetTimeout(/** @type {any} */ (fn), 0)
      })
    )
    // 1.0 keeps the captured sleep deterministic at the exponential ceiling.
    // Subtracting epsilon would put it under the ceiling but Math.floor still
    // matches; we just want the values to be predictable, not random.
    const randSpy = vi.spyOn(Math, 'random').mockReturnValue(0.999_999)

    try {
      const catalog = fileCatalog({
        resolver: alwaysConflicts,
        conditionalCommits: true,
        commitRetry: {
          maxAttempts: 5,
          backoff: { initialMs: 10, maxMs: 1000, factor: 3 },
        },
      })
      await expect(icebergAppend({
        catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
      })).rejects.toThrow(/5 attempts/)
    } finally {
      stSpy.mockRestore()
      randSpy.mockRestore()
    }

    // 4 retries between 5 attempts → 4 sleeps. Bases: 10, 30, 90, 270; cap
    // 1000 doesn't bind. Math.floor(0.999999 * base) trims by 1 in some
    // cases — assert monotonic growth and rough magnitude rather than exact
    // values, which keeps the test stable across jitter implementations.
    expect(sleeps).toHaveLength(4)
    expect(sleeps[0]).toBeGreaterThanOrEqual(9)
    expect(sleeps[0]).toBeLessThanOrEqual(10)
    expect(sleeps[1]).toBeGreaterThan(sleeps[0])
    expect(sleeps[2]).toBeGreaterThan(sleeps[1])
    expect(sleeps[3]).toBeGreaterThan(sleeps[2])
    expect(sleeps[3]).toBeLessThanOrEqual(1000)
  })
})
