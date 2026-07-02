import { ByteWriter } from 'hyparquet-writer'
import { signRequest as sigV4SignRequest } from './sigv4.js'

/**
 * @import {Resolver} from '../src/types.js'
 */

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
    // No custom endpoint: AWS S3 virtual-hosted-style. Use the regional
    // endpoint so buckets outside us-east-1 (and S3 Tables `--table-s3`
    // buckets) don't 301-redirect. `us-east-1` and `auto` fall back to the
    // global endpoint.
    if (region && region !== 'us-east-1' && region !== 'auto') {
      return `https://${bucket}.s3.${region}.amazonaws.com/${key}`
    }
    return `https://${bucket}.s3.amazonaws.com/${key}`
  }

  /**
   * @param {string} method
   * @param {string} url
   * @param {Uint8Array} [body]
   * @param {Record<string, string>} [extra]
   * @returns {Promise<Record<string, string>>}
   */
  function signRequest(method, url, body, extra = {}) {
    return sigV4SignRequest(method, url, body, {
      accessKeyId, secretAccessKey, sessionToken, region, service: 's3', extraHeaders: extra,
    })
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
