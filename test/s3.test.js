import { afterEach, describe, expect, it, vi } from 'vitest'
import { s3SignedResolver } from '../src/s3.js'

describe('s3SignedResolver', () => {
  afterEach(() => { vi.restoreAllMocks() })

  it('signs GET requests with AWS4-HMAC-SHA256 against a virtual-hosted URL', async () => {
    const headers = await captureHeaders(
      { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-east-1' },
      's3://mybucket/path/to/file.parquet'
    )
    expect(headers.authorization).toMatch(/^AWS4-HMAC-SHA256 Credential=AKID\/\d{8}\/us-east-1\/s3\/aws4_request, /)
    expect(headers.authorization).toMatch(/SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=[0-9a-f]{64}$/)
    expect(headers['x-amz-date']).toMatch(/^\d{8}T\d{6}Z$/)
    expect(headers['x-amz-content-sha256']).toMatch(/^[0-9a-f]{64}$/)
  })

  it('routes virtual-hosted-style by default and path-style when requested', async () => {
    const vhUrl = await captureUrl(
      { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'auto', endpoint: 'https://acct.r2.cloudflarestorage.com' },
      's3://bucket/key'
    )
    expect(vhUrl).toBe('https://bucket.acct.r2.cloudflarestorage.com/key')

    const psUrl = await captureUrl(
      { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'auto', endpoint: 'https://acct.r2.cloudflarestorage.com', pathStyle: true },
      's3://bucket/key'
    )
    expect(psUrl).toBe('https://acct.r2.cloudflarestorage.com/bucket/key')
  })

  it('falls back to regional AWS S3 endpoint when no endpoint given', async () => {
    const url = await captureUrl(
      { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-west-2' },
      's3://my-bucket/a/b.parquet'
    )
    expect(url).toBe('https://my-bucket.s3.us-west-2.amazonaws.com/a/b.parquet')
  })

  it('uses the global AWS S3 endpoint for us-east-1', async () => {
    const url = await captureUrl(
      { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-east-1' },
      's3://my-bucket/a/b.parquet'
    )
    expect(url).toBe('https://my-bucket.s3.amazonaws.com/a/b.parquet')
  })

  it('includes x-amz-security-token when sessionToken is supplied', async () => {
    const headers = await captureHeaders(
      { accessKeyId: 'AKID', secretAccessKey: 'SECRET', sessionToken: 'TEMP', region: 'us-east-1' },
      's3://bucket/key'
    )
    expect(headers['x-amz-security-token']).toBe('TEMP')
    expect(headers.authorization).toMatch(/SignedHeaders=[^,]*x-amz-security-token/)
  })

  it('produces stable signatures: same inputs at the same instant yield identical headers', async () => {
    const at = new Date('2026-01-01T00:00:00.000Z')
    vi.useFakeTimers()
    vi.setSystemTime(at)
    try {
      const h1 = await captureHeaders(
        { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-east-1' },
        's3://bucket/key'
      )
      const h2 = await captureHeaders(
        { accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: 'us-east-1' },
        's3://bucket/key'
      )
      expect(h1.authorization).toBe(h2.authorization)
    } finally {
      vi.useRealTimers()
    }
  })
})

/**
 * Drive the resolver to issue a signed HEAD; return the captured headers.
 *
 * @param {object} cfg
 * @param {string} path
 * @returns {Promise<Record<string, string>>}
 */
async function captureHeaders(cfg, path) {
  /** @type {Record<string, string>} */
  let captured = {}
  const fakeFetch = vi.fn((_url, init) => {
    captured = lowercase(init?.headers ?? {})
    return Promise.resolve(new Response('', { status: 200, headers: { 'content-length': '0' } }))
  })
  vi.stubGlobal('fetch', fakeFetch)
  // @ts-expect-error cfg shape is the public API
  const resolver = s3SignedResolver(cfg)
  await resolver.reader(path)
  return captured
}

/**
 * @param {object} cfg
 * @param {string} path
 * @returns {Promise<string>}
 */
async function captureUrl(cfg, path) {
  /** @type {string} */
  let url = ''
  const fakeFetch = vi.fn((u) => {
    url = String(u)
    return Promise.resolve(new Response('', { status: 200, headers: { 'content-length': '0' } }))
  })
  vi.stubGlobal('fetch', fakeFetch)
  // @ts-expect-error cfg shape is the public API
  const resolver = s3SignedResolver(cfg)
  await resolver.reader(path)
  return url
}

/**
 * @param {HeadersInit | Record<string, string>} h
 * @returns {Record<string, string>}
 */
function lowercase(h) {
  /** @type {Record<string, string>} */
  const out = {}
  for (const [k, v] of Object.entries(h)) out[k.toLowerCase()] = String(v)
  return out
}
