import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  restCatalogConnect,
  restCatalogCreateNamespace,
  restCatalogDropNamespace,
  restCatalogListNamespaces,
} from '../src/catalog.rest.js'
import { makeFetch } from './catalog.rest.helpers.js'

describe('REST Catalog client — namespaces', () => {
  /** @type {ReturnType<typeof makeFetch>} */
  let mock

  beforeEach(() => { mock = makeFetch({}) })
  afterEach(() => { vi.unstubAllGlobals() })

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
})
