import { ByteWriter } from 'hyparquet-writer'

/**
 * @import {Resolver} from '../src/types.js'
 */

const enc = new TextEncoder()

/**
 * Build a SigV4-signing `Resolver` for private S3-compatible buckets (AWS S3,
 * Cloudflare R2, MinIO, etc.). Signs every request with AWS Signature V4
 * using static credentials. Works in browsers and Node via the Web Crypto API.
 *
 * Credentials are taken as inputs; profile/STS/IMDS chains are out of scope.
 * For Iceberg REST catalogs that vend per-table credentials (R2, AWS Glue),
 * pass the `s3.*` keys from `restCatalogLoadCredentials` or `LoadTable.config`.
 *
 * Accepts paths as `s3://bucket/key`, `s3a://bucket/key`, or full https URLs.
 *
 * @param {object} options
 * @param {string} options.accessKeyId
 * @param {string} options.secretAccessKey
 * @param {string} [options.sessionToken] - For temporary credentials (STS/vended).
 * @param {string} options.region - e.g. 'us-east-1', or 'auto' for R2.
 * @param {string} [options.endpoint] - HTTPS endpoint root for non-AWS providers
 *   (e.g. `https://<account>.r2.cloudflarestorage.com`). Defaults to AWS S3.
 * @param {boolean} [options.pathStyle] - When true, URLs are
 *   `<endpoint>/<bucket>/<key>`; when false (default), virtual-hosted-style
 *   `<bucket>.<endpoint-host>/<key>`. R2 requires true.
 * @returns {Resolver}
 */
export function s3SignedResolver({
  accessKeyId, secretAccessKey, sessionToken, region, endpoint, pathStyle = false,
}) {
  const ep = endpoint ? new URL(endpoint.replace(/\/$/, '') + '/') : undefined

  /**
   * Translate `s3://bucket/key` (or `s3a://`) to an https URL targeting this
   * resolver's endpoint. https URLs are passed through unchanged.
   *
   * @param {string} url
   * @returns {string}
   */
  function toHttps(url) {
    if (!url.startsWith('s3://') && !url.startsWith('s3a://')) return url
    const rest = url.slice(url.indexOf('://') + 3)
    const slash = rest.indexOf('/')
    if (slash === -1) throw new Error(`invalid S3 URL: ${url}`)
    const bucket = rest.slice(0, slash)
    const key = rest.slice(slash + 1)
    if (ep) {
      if (pathStyle) return `${ep.origin}${ep.pathname}${bucket}/${key}`
      return `${ep.protocol}//${bucket}.${ep.host}/${key}`
    }
    // No custom endpoint: AWS S3 virtual-hosted-style.
    return `https://${bucket}.s3.amazonaws.com/${key}`
  }

  /**
   * @param {string} method
   * @param {string} url
   * @param {Uint8Array} [body]
   * @param {Record<string, string>} [extra]
   * @returns {Promise<Record<string, string>>}
   */
  async function signRequest(method, url, body, extra = {}) {
    const u = new URL(url)
    const now = new Date()
    const xAmzDate = now.toISOString().replace(/[-:]|\.\d{3}/g, '')
    const dStamp = xAmzDate.slice(0, 8)
    const payloadHash = body !== undefined ? await sha256hex(body) : await sha256hex('')

    /** @type {Record<string, string>} */
    const lc = {}
    for (const [k, v] of Object.entries(extra)) lc[k.toLowerCase()] = String(v)
    lc['host'] = u.host
    lc['x-amz-date'] = xAmzDate
    lc['x-amz-content-sha256'] = payloadHash
    if (sessionToken) lc['x-amz-security-token'] = sessionToken

    const sortedKeys = Object.keys(lc).sort()
    const canonicalHeaders = sortedKeys
      .map(k => `${k}:${lc[k].trim().replace(/\s+/g, ' ')}\n`)
      .join('')
    const signedHeaders = sortedKeys.join(';')

    const canonicalUri = u.pathname
      .split('/')
      .map(seg => encodeRfc3986(decodeURIComponent(seg)))
      .join('/')

    const params = [...u.searchParams.entries()].sort((a, b) => {
      if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1
      return a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0
    })
    const canonicalQuery = params
      .map(([k, v]) => `${encodeRfc3986(k)}=${encodeRfc3986(v)}`)
      .join('&')

    const canonicalRequest = [
      method, canonicalUri, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash,
    ].join('\n')
    const credentialScope = `${dStamp}/${region}/s3/aws4_request`
    const stringToSign = [
      'AWS4-HMAC-SHA256', xAmzDate, credentialScope, await sha256hex(canonicalRequest),
    ].join('\n')

    const signingKey = await deriveSigningKey(secretAccessKey, dStamp, region, 's3')
    const sigBytes = await hmac(signingKey, stringToSign)
    const signature = bytesToHex(sigBytes)

    /** @type {Record<string, string>} */
    const out = {}
    for (const [k, v] of Object.entries(lc)) {
      if (k === 'host') continue // fetch sets host itself
      out[k] = v
    }
    out['Authorization'] = `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`
    return out
  }

  return {
    async reader(path, byteLength) {
      const url = toHttps(path)
      let len = byteLength
      if (len === undefined) {
        const headers = await signRequest('HEAD', url)
        const res = await fetch(url, { method: 'HEAD', headers })
        if (!res.ok) throw new Error(`HEAD ${path}: ${res.status} ${res.statusText}`)
        len = Number(res.headers.get('content-length'))
        if (!Number.isFinite(len)) throw new Error(`HEAD ${path}: missing Content-Length`)
      }
      const fileLength = len
      return {
        byteLength: fileLength,
        async slice(start, end) {
          const last = (end ?? fileLength) - 1
          const range = `bytes=${start}-${last}`
          const headers = await signRequest('GET', url, undefined, { range })
          const res = await fetch(url, { method: 'GET', headers })
          if (!res.ok) throw new Error(`GET ${path} ${range}: ${res.status} ${res.statusText}`)
          return await res.arrayBuffer()
        },
      }
    },
    writer(path, options) {
      // hyparquet-writer expects the full Writer interface; wrap ByteWriter
      // and override finish() to PUT the buffered bytes, same pattern as urlResolver.
      const w = new ByteWriter()
      w.finish = async function() {
        const url = toHttps(path)
        const body = w.getBytes().slice()
        /** @type {Record<string, string>} */
        const extra = {}
        if (options?.ifNoneMatch) extra['if-none-match'] = options.ifNoneMatch
        const headers = await signRequest('PUT', url, body, extra)
        const res = await fetch(url, { method: 'PUT', headers, body })
        if (!res.ok) {
          /** @type {Error & { status?: number }} */
          const err = new Error(`PUT ${path}: ${res.status} ${res.statusText}`)
          err.status = res.status
          throw err
        }
      }
      return w
    },
    async deleter(path) {
      const url = toHttps(path)
      const headers = await signRequest('DELETE', url)
      const res = await fetch(url, { method: 'DELETE', headers })
      if (!res.ok && res.status !== 404) {
        throw new Error(`DELETE ${path}: ${res.status} ${res.statusText}`)
      }
    },
  }
}

/**
 * @param {string | Uint8Array} data
 * @returns {Promise<string>}
 */
async function sha256hex(data) {
  const bytes = /** @type {BufferSource} */ (typeof data === 'string' ? enc.encode(data) : data)
  const hash = await crypto.subtle.digest('SHA-256', bytes)
  return bytesToHex(new Uint8Array(hash))
}

/**
 * @param {string | Uint8Array} key
 * @param {string | Uint8Array} data
 * @returns {Promise<Uint8Array>}
 */
async function hmac(key, data) {
  const keyBytes = /** @type {BufferSource} */ (typeof key === 'string' ? enc.encode(key) : key)
  const dataBytes = /** @type {BufferSource} */ (typeof data === 'string' ? enc.encode(data) : data)
  const cryptoKey = await crypto.subtle.importKey(
    'raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  )
  const sig = await crypto.subtle.sign('HMAC', cryptoKey, dataBytes)
  return new Uint8Array(sig)
}

/**
 * SigV4 signing key: HMAC chain over date, region, service, "aws4_request".
 *
 * @param {string} secret
 * @param {string} dateStamp
 * @param {string} region
 * @param {string} service
 * @returns {Promise<Uint8Array>}
 */
async function deriveSigningKey(secret, dateStamp, region, service) {
  const kDate = await hmac(`AWS4${secret}`, dateStamp)
  const kRegion = await hmac(kDate, region)
  const kService = await hmac(kRegion, service)
  return await hmac(kService, 'aws4_request')
}

/**
 * RFC 3986 percent-encode for SigV4 canonical components. `encodeURIComponent`
 * leaves `!*'()` un-encoded; SigV4 wants them encoded.
 *
 * @param {string} str
 * @returns {string}
 */
function encodeRfc3986(str) {
  return encodeURIComponent(str).replace(
    /[!*'()]/g,
    c => '%' + c.charCodeAt(0).toString(16).toUpperCase()
  )
}

/**
 * @param {Uint8Array} bytes
 * @returns {string}
 */
function bytesToHex(bytes) {
  let s = ''
  for (const b of bytes) s += b.toString(16).padStart(2, '0')
  return s
}
