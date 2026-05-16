import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  restCatalogConnect,
  restCatalogListNamespaces,
  restCatalogLoadTable,
} from '../src/catalog/rest.js'
import { makeFetch } from './catalog.rest.helpers.js'

describe('REST Catalog client — connect & infrastructure', () => {
  /** @type {ReturnType<typeof makeFetch>} */
  let mock

  beforeEach(() => { mock = makeFetch({}) })
  afterEach(() => { vi.unstubAllGlobals() })

  it('restCatalogConnect parses /v1/config', async () => {
    mock = makeFetch({
      'https://cat/v1/config': { defaults: { a: '1' }, overrides: { prefix: 'ws/main', b: '2' } },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat/' })
    expect(ctx.url).toBe('https://cat')
    expect(ctx.prefix).toBe('ws/main')
    expect(ctx.defaults).toEqual({ a: '1' })
    expect(ctx.overrides).toEqual({ prefix: 'ws/main', b: '2' })
  })

  it('restCatalogConnect reads prefix from overrides (R2 Data Catalog)', async () => {
    // Cloudflare R2 Data Catalog (and the Iceberg REST spec) conveys the
    // routing prefix via overrides, not a top-level body.prefix.
    mock = makeFetch({
      'https://cat/v1/config': {
        overrides: { prefix: '04ac774a-5162-11f1-8000-ab2337a83aa5' },
        defaults: {},
      },
      'https://cat/v1/04ac774a-5162-11f1-8000-ab2337a83aa5/namespaces': { namespaces: [['default']] },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    expect(ctx.prefix).toBe('04ac774a-5162-11f1-8000-ab2337a83aa5')

    const namespaces = await restCatalogListNamespaces(ctx)
    expect(namespaces).toEqual([['default']])
    expect(mock.calls[1].url).toBe('https://cat/v1/04ac774a-5162-11f1-8000-ab2337a83aa5/namespaces')
  })

  it('restCatalogConnect prefers overrides.prefix over defaults.prefix', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {
        defaults: { prefix: 'default-ws' },
        overrides: { prefix: 'override-ws' },
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const ctx = await restCatalogConnect({ url: 'https://cat' })
    expect(ctx.prefix).toBe('override-ws')
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
