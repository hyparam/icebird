import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { restCatalogConnect } from '../../src/catalog/rest.js'
import { loadLatestFileCatalogMetadata } from '../../src/metadata.js'
import { icebergAppend, icebergCreateTable, icebergSetRef } from '../../src/write/write.js'
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

  // Desired behavior: when a foreign writer (java/rust/python convention,
  // `<NNNNN>-<uuid>.metadata.json`) has advanced the table since the last
  // icebird `v<N>.metadata.json`, discovery must surface the higher
  // foreign-named version. Today the linear probe-forward only follows
  // `v<N>.metadata.json` and breaks at the first gap, so it misses the
  // foreign file. `it.fails` runs the assertion and expects it to fail;
  // when the bug is fixed this case will report as "should have failed
  // but passed" and the developer can flip it to `it`.
  it('discovers a foreign-named version past a v<N> gap', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/probe-foreign-gap'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    const v1Bytes = files.get(`${tableUrl}/metadata/v1.metadata.json`)
    if (!v1Bytes) throw new Error('v1 missing')
    files.set(`${tableUrl}/metadata/00005-deadbeef-1111-2222-3333-444444444444.metadata.json`, v1Bytes)

    const latest = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(latest.version).toBe(5)
  })

  // Lister is required for the maxProbe-cap fallback path. With no lister
  // and a hint that's >maxProbe versions stale, recovery would fail. The
  // s3Lister default works for S3 URLs; non-S3 backends must supply one.
  it('falls back to listing when probe walks past maxProbe', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/probe-cap-fallback'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    const v1Bytes = files.get(`${tableUrl}/metadata/v1.metadata.json`)
    if (!v1Bytes) throw new Error('v1 missing')
    // Plant a contiguous run v2..v70 so probe hits the maxProbe cap (default 64).
    for (let v = 2; v <= 70; v++) {
      files.set(`${tableUrl}/metadata/v${v}.metadata.json`, v1Bytes)
    }

    const ok = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(ok.version).toBe(70)

    // Without a lister, the s3Lister default cannot list http:// URLs.
    await expect(loadLatestFileCatalogMetadata({ tableUrl, resolver }))
      .rejects.toThrow()
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
    // Zeroed back-off via table properties keeps the test fast. What we're
    // verifying is that the retry budget alone (default 50 attempts) absorbs
    // 10 racing writers without losing any.
    const catalog = fileCatalog({ resolver, conditionalCommits: true })
    await icebergCreateTable({
      catalog, tableUrl, schema,
      properties: { 'commit.retry.min-wait-ms': '0', 'commit.retry.max-wait-ms': '0' },
    })

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

  it('reads commit.retry.num-retries from table properties', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/prop-num-retries'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    await icebergCreateTable({
      catalog: fileCatalog({ resolver, conditionalCommits: true }),
      tableUrl, schema,
      properties: {
        'commit.retry.num-retries': '2',
        'commit.retry.min-wait-ms': '0',
        'commit.retry.max-wait-ms': '0',
      },
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
    // num-retries=2 → maxAttempts=3. The default 50 attempts would not
    // raise this error.
    const catalog = fileCatalog({ resolver: alwaysConflicts, conditionalCommits: true })
    await expect(icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })).rejects.toThrow(/3 attempts due to concurrent commits/)
  })

  it('garbage table properties fall through to defaults', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/garbage-props'
    const { resolver, files } = memResolver()
    await icebergCreateTable({
      catalog: fileCatalog({ resolver, conditionalCommits: true }),
      tableUrl, schema,
      properties: {
        'commit.retry.num-retries': 'not-a-number',
        'commit.retry.min-wait-ms': '-5',
        'commit.retry.max-wait-ms': '',
        'commit.retry.total-timeout-ms': 'NaN',
      },
    })
    // Just verify the commit still works — garbage props are ignored and
    // the library defaults apply.
    const catalog = fileCatalog({ resolver, conditionalCommits: true })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
  })

  it('total-timeout-ms terminates the retry loop early', async () => {
    let nowMs = 1700000000000
    vi.spyOn(Date, 'now').mockImplementation(() => nowMs)
    const tableUrl = 'http://test/timeout-budget'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')
    await icebergCreateTable({
      catalog: fileCatalog({ resolver, conditionalCommits: true }),
      tableUrl, schema,
      properties: {
        // Generous attempts so the timeout, not the attempt cap, ends the loop.
        'commit.retry.num-retries': '99',
        'commit.retry.min-wait-ms': '0',
        'commit.retry.max-wait-ms': '0',
        'commit.retry.total-timeout-ms': '500',
      },
    })
    const v1 = files.get(`${tableUrl}/metadata/v1.metadata.json`)
    if (!v1) throw new Error('v1 missing')

    /** @type {import('../../src/types.js').Resolver} */
    const alwaysConflicts = {
      ...resolver,
      writer(p, options) {
        if (/\/metadata\/v\d+\.metadata\.json$/.test(p) && options?.ifNoneMatch === '*') {
          // Advance clock 200ms per conflicting write so two failures push
          // elapsed past the 500ms budget.
          nowMs += 200
          files.set(p, v1)
        }
        return realWriter(p, options)
      },
    }
    const catalog = fileCatalog({ resolver: alwaysConflicts, conditionalCommits: true })
    await expect(icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
    })).rejects.toThrow(/retry budget exhausted/)
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
      properties: {
        'commit.retry.num-retries': '4',
        'commit.retry.min-wait-ms': '10',
        'commit.retry.max-wait-ms': '1000',
      },
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
      const catalog = fileCatalog({ resolver: alwaysConflicts, conditionalCommits: true })
      await expect(icebergAppend({
        catalog, tableUrl, records: [{ id: 1n, name: 'a' }],
      })).rejects.toThrow(/5 attempts/)
    } finally {
      stSpy.mockRestore()
      randSpy.mockRestore()
    }

    // 4 retries between 5 attempts → 4 sleeps. Bases (factor=2): 10, 20,
    // 40, 80; cap 1000 doesn't bind. Math.floor(0.999999 * base) trims by
    // 1 in some cases — assert monotonic growth and rough magnitude rather
    // than exact values, which keeps the test stable across jitter
    // implementations.
    expect(sleeps).toHaveLength(4)
    expect(sleeps[0]).toBeGreaterThanOrEqual(9)
    expect(sleeps[0]).toBeLessThanOrEqual(10)
    expect(sleeps[1]).toBeGreaterThan(sleeps[0])
    expect(sleeps[2]).toBeGreaterThan(sleeps[1])
    expect(sleeps[3]).toBeGreaterThan(sleeps[2])
    expect(sleeps[3]).toBeLessThanOrEqual(1000)
  })
})

describe('REST catalog retry on 409 CommitFailedException', () => {
  /**
   * @param {number} snapshotId
   * @param {Record<string, string>} [properties]
   * @returns {object}
   */
  function makeMetadata(snapshotId, properties = {}) {
    return {
      'format-version': 2,
      'table-uuid': 'tbl-uuid-1',
      location: 's3://bucket/orders',
      'last-sequence-number': 1,
      'last-updated-ms': 1700000000000,
      'last-column-id': 2,
      schemas: [schema],
      'current-schema-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'default-spec-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
      snapshots: [{
        'snapshot-id': snapshotId,
        'sequence-number': 1,
        'timestamp-ms': 1700000000000,
        summary: { operation: 'append' },
        'manifest-list': 's3://bucket/orders/metadata/snap-x.avro',
        'schema-id': 0,
      }],
      refs: { main: { 'snapshot-id': snapshotId, type: 'branch' } },
      'current-snapshot-id': snapshotId,
      'snapshot-log': [],
      'metadata-log': [],
      properties,
    }
  }

  it('retries icebergSetRef on a single 409 then succeeds', async () => {
    let commitCalls = 0
    const url = 'https://cat/v1/namespaces/db/tables/orders'
    vi.stubGlobal('fetch', async (/** @type {string} */ u, /** @type {RequestInit} */ init) => {
      if (u === 'https://cat/v1/config') {
        return new Response(JSON.stringify({}), { status: 200 })
      }
      if (u === url && (!init || init.method === undefined || init.method === 'GET')) {
        return new Response(JSON.stringify({
          'metadata-location': 's3://bucket/orders/metadata/v1.metadata.json',
          metadata: makeMetadata(111),
        }), { status: 200 })
      }
      if (u === url && init?.method === 'POST') {
        commitCalls++
        if (commitCalls === 1) {
          return new Response(JSON.stringify({
            error: { code: 409, type: 'CommitFailedException', message: 'lost race' },
          }), { status: 409 })
        }
        return new Response(JSON.stringify({
          'metadata-location': 's3://bucket/orders/metadata/v2.metadata.json',
          metadata: makeMetadata(111),
        }), { status: 200 })
      }
      throw new Error(`unexpected url: ${u} ${init?.method}`)
    })
    try {
      const ctx = await restCatalogConnect({ url: 'https://cat' })
      const result = await icebergSetRef({
        catalog: ctx, namespace: 'db', table: 'orders',
        ref: 'main', snapshotId: 111,
      })
      expect(commitCalls).toBe(2)
      expect(result['current-snapshot-id']).toBe(111)
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('exhausts attempts when 409s persist, honoring commit.retry.num-retries', async () => {
    let commitCalls = 0
    const url = 'https://cat/v1/namespaces/db/tables/orders'
    vi.stubGlobal('fetch', async (/** @type {string} */ u, /** @type {RequestInit} */ init) => {
      if (u === 'https://cat/v1/config') {
        return new Response(JSON.stringify({}), { status: 200 })
      }
      if (u === url && (!init || init.method === undefined || init.method === 'GET')) {
        // Table properties cap retries at 3 (num-retries=2 → maxAttempts=3)
        // with zeroed back-off so the test runs instantly.
        return new Response(JSON.stringify({
          'metadata-location': 's3://bucket/orders/metadata/v1.metadata.json',
          metadata: makeMetadata(111, {
            'commit.retry.num-retries': '2',
            'commit.retry.min-wait-ms': '0',
            'commit.retry.max-wait-ms': '0',
          }),
        }), { status: 200 })
      }
      if (u === url && init?.method === 'POST') {
        commitCalls++
        return new Response(JSON.stringify({
          error: { code: 409, type: 'CommitFailedException', message: 'lost race' },
        }), { status: 409 })
      }
      throw new Error(`unexpected url: ${u} ${init?.method}`)
    })
    try {
      const ctx = await restCatalogConnect({ url: 'https://cat' })
      await expect(icebergSetRef({
        catalog: ctx, namespace: 'db', table: 'orders',
        ref: 'main', snapshotId: 111,
      })).rejects.toThrow(/rest catalog commit failed after 3 attempts/)
      expect(commitCalls).toBe(3)
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('non-409 errors are not retried', async () => {
    let commitCalls = 0
    const url = 'https://cat/v1/namespaces/db/tables/orders'
    vi.stubGlobal('fetch', async (/** @type {string} */ u, /** @type {RequestInit} */ init) => {
      if (u === 'https://cat/v1/config') {
        return new Response(JSON.stringify({}), { status: 200 })
      }
      if (u === url && (!init || init.method === undefined || init.method === 'GET')) {
        return new Response(JSON.stringify({
          'metadata-location': 's3://bucket/orders/metadata/v1.metadata.json',
          metadata: makeMetadata(111),
        }), { status: 200 })
      }
      if (u === url && init?.method === 'POST') {
        commitCalls++
        return new Response(JSON.stringify({
          error: { code: 500, type: 'ServiceUnavailable', message: 'oops' },
        }), { status: 500 })
      }
      throw new Error(`unexpected url: ${u} ${init?.method}`)
    })
    try {
      const ctx = await restCatalogConnect({ url: 'https://cat' })
      await expect(icebergSetRef({
        catalog: ctx, namespace: 'db', table: 'orders',
        ref: 'main', snapshotId: 111,
      })).rejects.toThrow(/500 ServiceUnavailable/)
      expect(commitCalls).toBe(1)
    } finally {
      vi.unstubAllGlobals()
    }
  })
})
