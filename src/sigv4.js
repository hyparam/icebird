const enc = new TextEncoder()

/**
 * Build SigV4 Authorization and related headers for an HTTP request.
 *
 * @param {string} method
 * @param {string} url
 * @param {Uint8Array | undefined} body
 * @param {object} options
 * @param {string} options.accessKeyId
 * @param {string} options.secretAccessKey
 * @param {string} [options.sessionToken]
 * @param {string} options.region
 * @param {string} options.service - e.g. 's3', 's3tables', 'glue'
 * @param {Record<string, string>} [options.extraHeaders]
 * @returns {Promise<Record<string, string>>}
 */
export async function signRequest(method, url, body, {
  accessKeyId, secretAccessKey, sessionToken, region, service, extraHeaders = {},
}) {
  const u = new URL(url)
  const now = new Date()
  const xAmzDate = now.toISOString().replace(/[-:]|\.\d{3}/g, '')
  const dStamp = xAmzDate.slice(0, 8)
  const payloadHash = body !== undefined ? await sha256hex(body) : await sha256hex('')

  /** @type {Record<string, string>} */
  const lc = {}
  for (const [k, v] of Object.entries(extraHeaders)) lc[k.toLowerCase()] = String(v)
  lc['host'] = u.host
  lc['x-amz-date'] = xAmzDate
  lc['x-amz-content-sha256'] = payloadHash
  if (sessionToken) lc['x-amz-security-token'] = sessionToken

  const sortedKeys = Object.keys(lc).sort()
  const canonicalHeaders = sortedKeys
    .map(k => `${k}:${lc[k].trim().replace(/\s+/g, ' ')}\n`)
    .join('')
  const signedHeaders = sortedKeys.join(';')

  // AWS SigV4 canonical URI. S3 uses the path as-is (single-encoded);
  // every other service (s3tables, glue, …) double-URI-encodes the path.
  const doubleEncodePath = service !== 's3'
  const canonicalUri = u.pathname
    .split('/')
    .map(seg => doubleEncodePath
      ? encodeRfc3986(seg)
      : encodeRfc3986(decodeURIComponent(seg)))
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
  const credentialScope = `${dStamp}/${region}/${service}/aws4_request`
  const stringToSign = [
    'AWS4-HMAC-SHA256', xAmzDate, credentialScope, await sha256hex(canonicalRequest),
  ].join('\n')

  const signingKey = await deriveSigningKey(secretAccessKey, dStamp, region, service)
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

/**
 * Return a {@link RestCatalogContext.signRequest} hook that SigV4-signs fetch calls.
 *
 * @param {object} options
 * @param {string} options.accessKeyId
 * @param {string} options.secretAccessKey
 * @param {string} [options.sessionToken]
 * @param {string} options.region
 * @param {string} options.service
 * @returns {(url: string, init?: RequestInit) => Promise<RequestInit>}
 */
export function createSigV4SignRequest({ accessKeyId, secretAccessKey, sessionToken, region, service }) {
  return async function sigV4SignRequest(url, init) {
    const method = init?.method?.toUpperCase() ?? 'GET'
    let body
    if (init?.body !== undefined && init.body !== null) {
      if (typeof init.body === 'string') body = enc.encode(init.body)
      else if (init.body instanceof Uint8Array) body = init.body
      else if (init.body instanceof ArrayBuffer) body = new Uint8Array(init.body)
      else body = enc.encode(String(init.body))
    }
    /** @type {Record<string, string>} */
    const extra = {}
    const hdrs = headersToObject(init?.headers)
    for (const [k, v] of Object.entries(hdrs)) {
      const lk = k.toLowerCase()
      if (lk === 'host' || lk === 'authorization') continue
      extra[lk] = v
    }
    const signed = await signRequest(method, url, body, {
      accessKeyId, secretAccessKey, sessionToken, region, service, extraHeaders: extra,
    })
    return {
      ...init,
      method,
      body: init?.body,
      headers: { ...hdrs, ...signed },
    }
  }
}

/**
 * @param {HeadersInit | undefined} h
 * @returns {Record<string, string>}
 */
function headersToObject(h) {
  if (!h) return {}
  if (h instanceof Headers) {
    /** @type {Record<string, string>} */
    const out = {}
    h.forEach((v, k) => { out[k] = v })
    return out
  }
  if (Array.isArray(h)) return Object.fromEntries(h)
  return /** @type {Record<string, string>} */ ({ ...h })
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
 * RFC 3986 percent-encode for SigV4 canonical components.
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
