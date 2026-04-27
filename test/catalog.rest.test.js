import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  restCatalogConnect,
  restCatalogCreateNamespace,
  restCatalogDropNamespace,
  restCatalogDropTable,
  restCatalogListNamespaces,
  restCatalogListTables,
  restCatalogLoadTable,
  restCatalogRenameTable,
} from '../src/catalog.rest.js'

/**
 * @typedef {Response | Record<string, unknown>} RouteValue
 * @typedef {RouteValue | (() => RouteValue)} Route
 */

/**
 * Builds a fetch mock that returns canned responses keyed by URL.
 * Each route is a static value/Response or a thunk returning one.
 * Inspect `calls` to assert what was sent.
 *
 * @param {Record<string, Route>} routes
 * @returns {{ fn: (url: string, init?: RequestInit) => Promise<Response>, calls: Array<{url: string, init: RequestInit | undefined}> }}
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
    const route = routes[url]
    if (route === undefined) {
      return Promise.resolve(new Response(JSON.stringify({
        error: { code: 404, type: 'NotFound', message: `no route for ${url}` },
      }), { status: 404 }))
    }
    const value = typeof route === 'function' ? route() : route
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

  it('restCatalogDropTable issues DELETE and applies prefix', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { prefix: 'ws/main' },
      'https://cat/v1/ws/main/namespaces/db/tables/orders': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogDropTable(ctx, { namespace: 'db', table: 'orders' })

    const dropCall = mock.calls.find(c => c.url === 'https://cat/v1/ws/main/namespaces/db/tables/orders')
    expect(dropCall).toBeDefined()
    expect(dropCall?.init?.method).toBe('DELETE')
  })

  it('restCatalogDropTable forwards purgeRequested as query param', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/orders?purgeRequested=true': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogDropTable(ctx, { namespace: 'db', table: 'orders', purgeRequested: true })

    expect(mock.calls.some(c => c.url === 'https://cat/v1/namespaces/db/tables/orders?purgeRequested=true')).toBe(true)
  })

  it('restCatalogDropTable encodes multi-level namespace with %1F', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub/tables/orders': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogDropTable(ctx, { namespace: ['db', 'sub'], table: 'orders' })

    expect(mock.calls.some(c => c.url === 'https://cat/v1/namespaces/db%1Fsub/tables/orders')).toBe(true)
  })

  it('restCatalogCreateNamespace POSTs body with namespace and properties', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { prefix: 'ws/main' },
      'https://cat/v1/ws/main/namespaces': { namespace: ['db', 'sub'], properties: { owner: 'alice' } },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogCreateNamespace(ctx, {
      namespace: ['db', 'sub'],
      properties: { owner: 'alice' },
    })

    expect(result).toEqual({ namespace: ['db', 'sub'], properties: { owner: 'alice' } })
    const call = mock.calls.find(c => c.url === 'https://cat/v1/ws/main/namespaces')
    expect(call?.init?.method).toBe('POST')
    const headers = /** @type {Record<string,string>} */ (call?.init?.headers)
    expect(headers['content-type']).toBe('application/json')
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({
      namespace: ['db', 'sub'],
      properties: { owner: 'alice' },
    })
  })

  it('restCatalogCreateNamespace splits dotted namespace and defaults properties', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces': {},
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    const result = await restCatalogCreateNamespace(ctx, { namespace: 'db.sub' })

    const call = mock.calls.find(c => c.url === 'https://cat/v1/namespaces')
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({
      namespace: ['db', 'sub'],
      properties: {},
    })
    expect(result).toEqual({ namespace: ['db', 'sub'], properties: {} })
  })

  it('restCatalogDropNamespace issues DELETE with %1F-encoded namespace', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db%1Fsub': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogDropNamespace(ctx, { namespace: ['db', 'sub'] })

    const dropCall = mock.calls.find(c => c.url === 'https://cat/v1/namespaces/db%1Fsub')
    expect(dropCall).toBeDefined()
    expect(dropCall?.init?.method).toBe('DELETE')
  })

  it('restCatalogRenameTable POSTs source and destination', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { prefix: 'ws/main' },
      'https://cat/v1/ws/main/tables/rename': () => new Response(null, { status: 204 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await restCatalogRenameTable(ctx, {
      source: { namespace: ['db'], name: 'orders' },
      destination: { namespace: ['db'], name: 'orders_v2' },
    })

    const call = mock.calls.find(c => c.url === 'https://cat/v1/ws/main/tables/rename')
    expect(call?.init?.method).toBe('POST')
    expect(JSON.parse(/** @type {string} */ (call?.init?.body))).toEqual({
      source: { namespace: ['db'], name: 'orders' },
      destination: { namespace: ['db'], name: 'orders_v2' },
    })
  })

  it('restCatalogRenameTable surfaces ErrorModel on failure', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/tables/rename': () => new Response(JSON.stringify({
        error: { code: 409, type: 'AlreadyExistsException', message: 'destination exists' },
      }), { status: 409 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogRenameTable(ctx, {
      source: { namespace: ['db'], name: 'a' },
      destination: { namespace: ['db'], name: 'b' },
    })).rejects.toThrow('409 AlreadyExistsException: destination exists')
  })

  it('restCatalogDropTable surfaces ErrorModel on failure', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/missing': () => new Response(JSON.stringify({
        error: { code: 404, type: 'NoSuchTableException', message: 'Table not found' },
      }), { status: 404 }),
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    await expect(restCatalogDropTable(ctx, { namespace: 'db', table: 'missing' }))
      .rejects.toThrow('404 NoSuchTableException: Table not found')
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
