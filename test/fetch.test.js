import { afterEach, describe, expect, it, vi } from 'vitest'
import { urlResolver } from '../src/fetch.js'

/**
 * @import {Resolver} from '../src/types.js'
 */

/**
 * @param {Partial<Response>} init
 * @returns {Response}
 */
function fakeResponse(init) {
  return /** @type {any} */ (init)
}

/**
 * @param {Resolver} resolver
 * @returns {NonNullable<Resolver['deleter']>}
 */
function requireDeleter(resolver) {
  if (!resolver.deleter) throw new Error('resolver.deleter is required')
  return resolver.deleter
}

describe('urlResolver.deleter', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('issues an HTTP DELETE and translates s3:// urls', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: true, status: 204, statusText: 'No Content' }))
    const deleter = requireDeleter(urlResolver())
    await deleter('s3://bucket/path/v1.metadata.json')

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('https://bucket.s3.amazonaws.com/path/v1.metadata.json')
    expect(init).toMatchObject({ method: 'DELETE' })
  })

  it('forwards requestInit (auth headers) and overrides the method', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: true, status: 204, statusText: '' }))
    const deleter = requireDeleter(urlResolver({
      requestInit: { headers: { authorization: 'Bearer t' }, method: 'GET' },
    }))
    await deleter('https://h/foo')

    const init = fetchMock.mock.calls[0][1]
    expect(init?.method).toBe('DELETE')
    expect(init?.headers).toEqual({ authorization: 'Bearer t' })
  })

  it('tolerates 404', async () => {
    vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: false, status: 404, statusText: 'Not Found' }))
    await expect(requireDeleter(urlResolver())('https://h/missing')).resolves.toBeUndefined()
  })

  it('throws on other non-ok responses', async () => {
    vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: false, status: 403, statusText: 'Forbidden' }))
    await expect(requireDeleter(urlResolver())('https://h/forbidden'))
      .rejects.toThrow(/DELETE .*: 403 Forbidden/)
  })
})
