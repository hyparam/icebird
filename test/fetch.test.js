import { afterEach, describe, expect, it, vi } from 'vitest'
import { s3ParseUrl, urlResolver } from '../src/fetch.js'

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

/**
 * @param {Resolver} resolver
 * @returns {NonNullable<Resolver['writer']>}
 */
function requireWriter(resolver) {
  if (!resolver.writer) throw new Error('resolver.writer is required')
  return resolver.writer
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

describe('urlResolver.writer', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('PUTs the buffered bytes on finish() and translates s3:// urls', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: true, status: 200, statusText: 'OK' }))
    const w = requireWriter(urlResolver())('s3://bucket/key/file.json')
    w.appendBytes(new TextEncoder().encode('hello'))
    await w.finish()

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('https://bucket.s3.amazonaws.com/key/file.json')
    expect(init?.method).toBe('PUT')
    const body = /** @type {Uint8Array} */ (init?.body)
    expect(new TextDecoder().decode(body)).toBe('hello')
  })

  it('forwards requestInit and overrides the method', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: true, status: 200, statusText: 'OK' }))
    const w = requireWriter(urlResolver({
      requestInit: { headers: { authorization: 'Bearer t' }, method: 'GET' },
    }))('https://h/foo')
    w.appendBytes(new Uint8Array([1, 2, 3]))
    await w.finish()

    const init = fetchMock.mock.calls[0][1]
    expect(init?.method).toBe('PUT')
    expect(init?.headers).toEqual({ authorization: 'Bearer t' })
  })

  it('throws on non-ok response', async () => {
    vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: false, status: 403, statusText: 'Forbidden' }))
    const w = requireWriter(urlResolver())('https://h/forbidden')
    w.appendBytes(new Uint8Array([0]))
    await expect(w.finish()).rejects.toThrow(/PUT .*: 403 Forbidden/)
  })

  it('sets If-None-Match: * when ifNoneMatch is "*"', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: true, status: 200, statusText: 'OK' }))
    const w = requireWriter(urlResolver())('https://h/key', { ifNoneMatch: '*' })
    w.appendBytes(new Uint8Array([1]))
    await w.finish()

    const headers = /** @type {Record<string, string>} */ (fetchMock.mock.calls[0][1]?.headers)
    expect(headers['If-None-Match']).toBe('*')
  })

  it('omits If-None-Match by default', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: true, status: 200, statusText: 'OK' }))
    const w = requireWriter(urlResolver())('https://h/key')
    w.appendBytes(new Uint8Array([1]))
    await w.finish()

    const headers = /** @type {Record<string, string>} */ (fetchMock.mock.calls[0][1]?.headers)
    expect(headers['If-None-Match']).toBeUndefined()
  })

  it('attaches HTTP status to the thrown error', async () => {
    vi.spyOn(globalThis, 'fetch')
      .mockResolvedValue(fakeResponse({ ok: false, status: 412, statusText: 'Precondition Failed' }))
    const w = requireWriter(urlResolver())('https://h/key', { ifNoneMatch: '*' })
    w.appendBytes(new Uint8Array([1]))
    await expect(w.finish()).rejects.toMatchObject({ status: 412 })
  })
})

describe('s3ParseUrl', () => {
  it('parses s3:// URLs', () => {
    expect(s3ParseUrl('s3://hyperparam-iceberg/test/hypstack1/v1.metadata.json'))
      .toEqual({ bucket: 'hyperparam-iceberg', prefix: 'test/hypstack1/v1.metadata.json' })
  })

  it('parses s3a:// URLs', () => {
    expect(s3ParseUrl('s3a://my-bucket/k')).toEqual({ bucket: 'my-bucket', prefix: 'k' })
  })

  it('parses path-style HTTPS URLs', () => {
    expect(s3ParseUrl('https://s3.amazonaws.com/my-bucket/path/file.parquet'))
      .toEqual({ bucket: 'my-bucket', prefix: 'path/file.parquet' })
  })

  it('parses virtual-hosted global URLs with dashed bucket names', () => {
    expect(s3ParseUrl('https://hyperparam-iceberg.s3.amazonaws.com/k/v.json'))
      .toEqual({ bucket: 'hyperparam-iceberg', prefix: 'k/v.json' })
  })

  it('parses virtual-hosted regional URLs (s3.<region>)', () => {
    expect(s3ParseUrl('https://hyperparam-iceberg.s3.us-east-1.amazonaws.com/k/v.json'))
      .toEqual({ bucket: 'hyperparam-iceberg', prefix: 'k/v.json' })
  })

  it('parses legacy regional URLs (s3-<region>)', () => {
    expect(s3ParseUrl('https://hyperparam-iceberg.s3-us-west-2.amazonaws.com/k/v.json'))
      .toEqual({ bucket: 'hyperparam-iceberg', prefix: 'k/v.json' })
  })

  it('returns undefined for non-S3 URLs', () => {
    expect(s3ParseUrl('https://example.com/foo')).toBeUndefined()
    expect(s3ParseUrl('http://localhost:9000/bucket/key')).toBeUndefined()
  })
})
