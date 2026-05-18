import { afterEach, describe, expect, it, vi } from 'vitest'
import { cachingResolver, s3ParseUrl, urlResolver } from '../src/fetch.js'

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

describe('cachingResolver', () => {
  /**
   * Build a synthetic resolver with counters so tests can assert how many
   * times each operation actually reached the "underlying" layer.
   *
   * @param {object} [opts]
   * @param {Record<string, Uint8Array>} [opts.bytes]
   * @returns {{
   *   resolver: Resolver,
   *   readerCalls: string[],
   *   writerCalls: string[],
   *   deleterCalls: string[],
   *   finishOutcomes: ('ok'|'err')[],
   *   failNextFinish: () => void,
   * }}
   */
  function fakeBase({ bytes = {} } = {}) {
    /** @type {string[]} */
    const readerCalls = []
    /** @type {string[]} */
    const writerCalls = []
    /** @type {string[]} */
    const deleterCalls = []
    /** @type {('ok'|'err')[]} */
    const finishOutcomes = []
    let nextFinishShouldFail = false

    /** @type {Resolver} */
    const resolver = {
      reader(url) {
        readerCalls.push(url)
        const data = bytes[url] ?? new Uint8Array([0])
        return {
          byteLength: data.byteLength,
          slice(start, end) {
            const stop = end ?? data.byteLength
            return data.slice(start, stop).buffer
          },
        }
      },
      writer(url) {
        writerCalls.push(url)
        return {
          buffer: new ArrayBuffer(0),
          view: new DataView(new ArrayBuffer(0)),
          offset: 0,
          ensure() { /* no-op */ },
          appendBytes() { /* no-op */ },
          appendBuffer() { /* no-op */ },
          appendUint8() { /* no-op */ },
          appendUint32() { /* no-op */ },
          appendInt32() { /* no-op */ },
          appendInt64() { /* no-op */ },
          appendFloat32() { /* no-op */ },
          appendFloat64() { /* no-op */ },
          appendVarInt() { /* no-op */ },
          appendVarBigInt() { /* no-op */ },
          appendZigZag() { /* no-op */ },
          getBuffer() { return new ArrayBuffer(0) },
          getBytes() { return new Uint8Array(0) },
          finish() {
            if (nextFinishShouldFail) {
              nextFinishShouldFail = false
              finishOutcomes.push('err')
              throw new Error('finish failed')
            }
            finishOutcomes.push('ok')
          },
        }
      },
      deleter(url) {
        deleterCalls.push(url)
        return Promise.resolve()
      },
    }

    return {
      resolver,
      readerCalls,
      writerCalls,
      deleterCalls,
      finishOutcomes,
      failNextFinish: () => { nextFinishShouldFail = true },
    }
  }

  it('memoizes reader by path', async () => {
    const { resolver, readerCalls } = fakeBase()
    const cached = cachingResolver(resolver)

    const a = await cached.reader('s3://b/x')
    const b = await cached.reader('s3://b/x')

    expect(readerCalls).toEqual(['s3://b/x'])
    expect(a).toBe(b)
  })

  it('caches each distinct path separately', async () => {
    const { resolver, readerCalls } = fakeBase()
    const cached = cachingResolver(resolver)

    await cached.reader('s3://b/x')
    await cached.reader('s3://b/y')
    await cached.reader('s3://b/x')

    expect(readerCalls).toEqual(['s3://b/x', 's3://b/y'])
  })

  it('range reads from the same path do not retrigger the underlying reader', async () => {
    const data = new Uint8Array(100).map((_, i) => i)
    const { resolver, readerCalls } = fakeBase({ bytes: { 's3://b/x': data } })
    const cached = cachingResolver(resolver)

    const buf = await cached.reader('s3://b/x')
    const a = new Uint8Array(await buf.slice(10, 20))
    const b = new Uint8Array(await buf.slice(40, 50))

    expect(readerCalls).toEqual(['s3://b/x'])
    expect(Array.from(a)).toEqual([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
    expect(Array.from(b)).toEqual([40, 41, 42, 43, 44, 45, 46, 47, 48, 49])
  })

  it('writer.finish() invalidates the cache entry on success', async () => {
    const { resolver, readerCalls } = fakeBase()
    const cached = cachingResolver(resolver)

    await cached.reader('s3://b/version-hint.text')
    const w = requireWriter(cached)('s3://b/version-hint.text')
    await w.finish()

    // Cache was invalidated, so the next read fetches again.
    await cached.reader('s3://b/version-hint.text')

    expect(readerCalls).toEqual([
      's3://b/version-hint.text',
      's3://b/version-hint.text',
    ])
  })

  it('writer.finish() failure leaves the cache intact', async () => {
    const { resolver, readerCalls, failNextFinish } = fakeBase()
    const cached = cachingResolver(resolver)

    await cached.reader('s3://b/version-hint.text')
    const w = requireWriter(cached)('s3://b/version-hint.text')
    failNextFinish()
    await expect(w.finish()).rejects.toThrow('finish failed')

    // Cache still valid: object wasn't actually modified.
    await cached.reader('s3://b/version-hint.text')

    expect(readerCalls).toEqual(['s3://b/version-hint.text'])
  })

  it('does not cache failed reads', async () => {
    let attempt = 0
    /** @type {string[]} */
    const readerCalls = []
    /** @type {Resolver} */
    const base = {
      reader(url) {
        readerCalls.push(url)
        attempt++
        if (attempt === 1) throw new Error('not found')
        return { byteLength: 1, slice() { return new ArrayBuffer(1) } }
      },
    }
    const cached = cachingResolver(base)

    await expect(Promise.resolve(cached.reader('s3://b/x'))).rejects.toThrow('not found')
    // Second call must retry, not return the cached rejection.
    const buf = await cached.reader('s3://b/x')
    expect(buf.byteLength).toBe(1)
    expect(readerCalls).toEqual(['s3://b/x', 's3://b/x'])
  })

  it('deleter invalidates the cache entry', async () => {
    const { resolver, readerCalls } = fakeBase()
    const cached = cachingResolver(resolver)

    await cached.reader('s3://b/x')
    await requireDeleter(cached)('s3://b/x')
    await cached.reader('s3://b/x')

    expect(readerCalls).toEqual(['s3://b/x', 's3://b/x'])
  })

  it('omits writer when the base resolver omits it', () => {
    /** @type {Resolver} */
    const readOnly = {
      reader() {
        return { byteLength: 0, slice() { return new ArrayBuffer(0) } }
      },
    }
    const cached = cachingResolver(readOnly)
    expect(cached.writer).toBeUndefined()
    expect(cached.deleter).toBeUndefined()
  })

  it('forwards writer options (e.g. ifNoneMatch) to the underlying writer', () => {
    /** @type {{ url: string, options?: { ifNoneMatch?: '*' } }[]} */
    const writerCalls = []
    /** @type {Resolver} */
    const base = {
      reader() {
        return { byteLength: 0, slice() { return new ArrayBuffer(0) } }
      },
      writer(url, options) {
        writerCalls.push({ url, options })
        return /** @type {any} */ ({ finish() { /* noop */ } })
      },
    }
    const cached = cachingResolver(base)
    requireWriter(cached)('s3://b/x', { ifNoneMatch: '*' })
    expect(writerCalls).toEqual([{ url: 's3://b/x', options: { ifNoneMatch: '*' } }])
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
