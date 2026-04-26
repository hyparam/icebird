import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  restCatalogConnect,
  restCatalogListNamespaces,
  restCatalogListTables,
  restCatalogLoadTable,
} from '../src/catalog.rest.js'

/**
 * Builds a fetch mock that returns canned responses keyed by URL.
 * Each handler can be a function (req) => Response/object, or a static value.
 *
 * @param {Record<string, any>} routes
 * @returns {{ fn: any, calls: Array<{url: string, init: RequestInit | undefined}> }}
 */
function makeFetch(routes) {
  /** @type {Array<{url: string, init: RequestInit | undefined}>} */
  const calls = []
  /**
   * @param {string} url
   * @param {RequestInit} [init]
   * @returns {Promise<Response>}
   */
  function fn(url, init) {
    calls.push({ url, init })
    const handler = routes[url]
    if (handler === undefined) {
      return Promise.resolve(new Response(JSON.stringify({
        error: { code: 404, type: 'NotFound', message: `no route for ${url}` },
      }), { status: 404 }))
    }
    const value = typeof handler === 'function' ? handler({ url, init }) : handler
    if (value instanceof Response) return Promise.resolve(value)
    return Promise.resolve(new Response(JSON.stringify(value), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    }))
  }
  return { fn, calls }
}

describe('REST Catalog client', () => {
  /** @type {ReturnType<typeof makeFetch>} */
  let mock

  beforeEach(() => { mock = makeFetch({}) })
  afterEach(() => { vi.unstubAllGlobals() })

  it('restCatalogConnect parses /v1/config', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { prefix: 'ws/main', defaults: { a: '1' }, overrides: { b: '2' } },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat/' })
    expect(ctx.url).toBe('https://cat')
    expect(ctx.prefix).toBe('ws/main')
    expect(ctx.defaults).toEqual({ a: '1' })
    expect(ctx.overrides).toEqual({ b: '2' })
  })

  it('restCatalogConnect handles missing prefix', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    expect(ctx.prefix).toBe('')
    expect(ctx.defaults).toEqual({})
    expect(ctx.overrides).toEqual({})
  })

  it('restCatalogConnect forwards warehouse query param', async () => {
    mock = makeFetch({
      'https://cat/v1/config?warehouse=my%2Fwh': {},
    })
    vi.stubGlobal('fetch', mock.fn)

    await restCatalogConnect({ url: 'https://cat', warehouse: 'my/wh' })
    expect(mock.calls[0].url).toBe('https://cat/v1/config?warehouse=my%2Fwh')
  })

  it('restCatalogListNamespaces single page', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { prefix: '' },
      'https://cat/v1/namespaces': { namespaces: [['db'], ['analytics']] },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogListNamespaces(ctx)
    expect(result).toEqual([['db'], ['analytics']])
  })

  it('restCatalogListNamespaces follows next-page-token', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces': { namespaces: [['a']], 'next-page-token': 'tok-1' },
      'https://cat/v1/namespaces?pageToken=tok-1': { namespaces: [['b']], 'next-page-token': 'tok 2' },
      'https://cat/v1/namespaces?pageToken=tok%202': { namespaces: [['c']] },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogListNamespaces(ctx)
    expect(result).toEqual([['a'], ['b'], ['c']])
  })

  it('restCatalogListNamespaces with parent encodes multi-level', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces?parent=db%1Fsub': { namespaces: [['db', 'sub', 'leaf']] },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogListNamespaces(ctx, { parent: ['db', 'sub'] })
    expect(result).toEqual([['db', 'sub', 'leaf']])
  })

  it('restCatalogListTables paginates and applies prefix', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { prefix: 'ws/main' },
      'https://cat/v1/ws/main/namespaces/db/tables': {
        identifiers: [{ namespace: ['db'], name: 't1' }],
        'next-page-token': 'p2',
      },
      'https://cat/v1/ws/main/namespaces/db/tables?pageToken=p2': {
        identifiers: [{ namespace: ['db'], name: 't2' }],
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const tables = await restCatalogListTables(ctx, { namespace: 'db' })
    expect(tables).toEqual([
      { namespace: ['db'], name: 't1' },
      { namespace: ['db'], name: 't2' },
    ])
  })

  it('restCatalogLoadTable returns metadata and metadataLocation', async () => {
    const metadata = {
      'format-version': 2,
      'table-uuid': 'abc',
      location: 's3://bucket/table',
      schemas: [],
    }
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/orders': {
        'metadata-location': 's3://bucket/table/metadata/v3.metadata.json',
        metadata,
        config: { 'client.region': 'us-east-1' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogLoadTable(ctx, { namespace: 'db', table: 'orders' })
    expect(result.metadataLocation).toBe('s3://bucket/table/metadata/v3.metadata.json')
    expect(result.metadata).toEqual(metadata)
    expect(result.config).toEqual({ 'client.region': 'us-east-1' })
  })

  it('restCatalogLoadTable encodes multi-level namespace with %1F', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub/tables/orders': {
        'metadata-location': 'x',
        metadata: { location: 's3://x' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogLoadTable(ctx, { namespace: ['db', 'sub'], table: 'orders' })
    expect(mock.calls.some(c => c.url === 'https://cat/v1/namespaces/db%1Fsub/tables/orders')).toBe(true)
  })

  it('throws ErrorModel message on non-2xx', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/missing': () => new Response(JSON.stringify({
        error: { code: 404, type: 'NoSuchTableException', message: 'Table not found' },
      }), { status: 404 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogLoadTable(ctx, { namespace: 'db', table: 'missing' }))
      .rejects.toThrow('404 NoSuchTableException: Table not found')
  })

  it('forwards Authorization header from requestInit', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces': { namespaces: [] },
    })
    vi.stubGlobal('fetch', mock.fn)

    const requestInit = { headers: { Authorization: 'Bearer secret-token' } }
    const ctx = await restCatalogConnect({ url: 'https://cat', requestInit })
    await restCatalogListNamespaces(ctx)

    for (const call of mock.calls) {
      const headers = /** @type {Record<string,string>} */ (call.init?.headers)
      expect(headers?.Authorization).toBe('Bearer secret-token')
    }
  })
})
