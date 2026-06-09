import { typeName } from '../schema.js'

/**
 * Iceberg single-value serialization (encode/decode) and the canonical
 * per-type comparator. These are pure leaf utilities with no dependency on the
 * stats/geospatial machinery, so the read path (`src/prune.js`) and the write
 * path (`src/write/stats.js`, `src/write/sort.js`) can both share them without
 * pulling geospatial code into the read/SQL bundle.
 *
 * @import {IcebergType} from '../../src/types.js'
 */

/**
 * Serialize a value per the Iceberg single-value serialization spec.
 * Returns undefined for types we don't yet support so the bound is omitted.
 *
 * @param {any} value
 * @param {IcebergType} type
 * @returns {Uint8Array|undefined}
 */
export function serializeValue(value, type) {
  const name = typeName(type)
  if (name.startsWith('decimal(')) {
    const m = /^decimal\((\d+),\s*(\d+)\)$/.exec(name)
    if (!m) return undefined
    const scale = parseInt(m[2], 10)
    if (typeof value !== 'number' && typeof value !== 'bigint') return undefined
    const factor = 10n ** BigInt(scale)
    const unscaled = typeof value === 'bigint'
      ? value * factor
      : BigInt(Math.round(value * Number(factor)))
    return twosComplementMinBigEndian(unscaled)
  }
  if (name.startsWith('fixed[')) {
    return value instanceof Uint8Array ? value : undefined
  }
  switch (name) {
  case 'boolean': {
    return new Uint8Array([value ? 1 : 0])
  }
  case 'int': {
    const buf = new ArrayBuffer(4)
    new DataView(buf).setInt32(0, value, true)
    return new Uint8Array(buf)
  }
  case 'long': {
    const buf = new ArrayBuffer(8)
    new DataView(buf).setBigInt64(0, typeof value === 'bigint' ? value : BigInt(value), true)
    return new Uint8Array(buf)
  }
  case 'float': {
    const buf = new ArrayBuffer(4)
    new DataView(buf).setFloat32(0, value, true)
    return new Uint8Array(buf)
  }
  case 'double': {
    const buf = new ArrayBuffer(8)
    new DataView(buf).setFloat64(0, value, true)
    return new Uint8Array(buf)
  }
  case 'date': {
    // days since epoch, 4-byte little-endian int32
    const days = value instanceof Date
      ? Math.floor(value.getTime() / 86400000)
      : Number(value)
    const buf = new ArrayBuffer(4)
    new DataView(buf).setInt32(0, days, true)
    return new Uint8Array(buf)
  }
  case 'time': {
    // microseconds since midnight, 8-byte little-endian int64
    const buf = new ArrayBuffer(8)
    new DataView(buf).setBigInt64(0, typeof value === 'bigint' ? value : BigInt(value), true)
    return new Uint8Array(buf)
  }
  case 'timestamp':
  case 'timestamptz': {
    // micros since epoch, 8-byte little-endian
    const buf = new ArrayBuffer(8)
    new DataView(buf).setBigInt64(0, timestampToMicros(value), true)
    return new Uint8Array(buf)
  }
  case 'timestamp_ns':
  case 'timestamptz_ns': {
    // nanos since epoch, 8-byte little-endian
    const buf = new ArrayBuffer(8)
    new DataView(buf).setBigInt64(0, timestampToNanos(value), true)
    return new Uint8Array(buf)
  }
  case 'string': {
    return new TextEncoder().encode(value)
  }
  case 'binary': {
    return value instanceof Uint8Array ? value : undefined
  }
  case 'uuid': {
    if (value instanceof Uint8Array && value.length === 16) return value
    if (typeof value === 'string') return uuidStringToBytes(value)
    return undefined
  }
  default:
    return undefined
  }
}

/**
 * Deserialize a single value from Iceberg single-value serialization, the
 * inverse of `serializeValue`. Used to decode manifest `lower_bounds` /
 * `upper_bounds` (and stat bounds generally) into JS values in the same domain
 * the read-side comparators understand (number / bigint / string / boolean /
 * Uint8Array).
 *
 * Returns `undefined` for unsupported types or malformed input so callers can
 * treat the bound as absent (keep the file) rather than mis-prune. Note that
 * string/binary/fixed bounds may be *truncated* prefixes (per Iceberg's
 * default `truncate(16)` metrics): the decoded value is the stored prefix, not
 * necessarily the true column min/max.
 *
 * @param {Uint8Array} bytes
 * @param {IcebergType} type
 * @returns {any}
 */
export function deserializeValue(bytes, type) {
  if (!(bytes instanceof Uint8Array)) return undefined
  const name = typeName(type)
  try {
    if (name.startsWith('decimal(')) {
      const m = /^decimal\((\d+),\s*(\d+)\)$/.exec(name)
      if (!m) return undefined
      const scale = parseInt(m[2], 10)
      const unscaled = twosComplementBigEndianToBigInt(bytes)
      return Number(unscaled) / 10 ** scale
    }
    if (name.startsWith('fixed[')) {
      return bytes
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    switch (name) {
    case 'boolean': return bytes[0] !== 0
    case 'int': return view.getInt32(0, true)
    case 'long': return view.getBigInt64(0, true)
    case 'float': return view.getFloat32(0, true)
    case 'double': return view.getFloat64(0, true)
    // days since epoch (number)
    case 'date': return view.getInt32(0, true)
    // micros since midnight (bigint)
    case 'time': return view.getBigInt64(0, true)
    // micros since epoch (bigint)
    case 'timestamp':
    case 'timestamptz': return view.getBigInt64(0, true)
    // nanos since epoch (bigint)
    case 'timestamp_ns':
    case 'timestamptz_ns': return view.getBigInt64(0, true)
    case 'string': return new TextDecoder().decode(bytes)
    case 'binary': return bytes
    case 'uuid': return bytes
    default: return undefined
    }
  } catch {
    return undefined
  }
}

/**
 * Compare two non-null values of the given iceberg type.
 *
 * @param {any} a
 * @param {any} b
 * @param {IcebergType} type
 * @returns {number}
 */
export function compare(a, b, type) {
  switch (typeName(type)) {
  case 'boolean':
    return (a ? 1 : 0) - (b ? 1 : 0)
  case 'int':
    return a < b ? -1 : a > b ? 1 : 0
  case 'float':
  case 'double':
    return compareFloating(a, b)
  case 'long': {
    const ai = typeof a === 'bigint' ? a : BigInt(a)
    const bi = typeof b === 'bigint' ? b : BigInt(b)
    return ai < bi ? -1 : ai > bi ? 1 : 0
  }
  case 'date': {
    // Bounds decode to days-since-epoch, but query literals can be `Date`
    // objects (or ISO strings). Normalize both sides to days so they compare in
    // one domain; NaN keeps the comparison undecided so callers don't mis-prune.
    const ad = dateToDays(a)
    const bd = dateToDays(b)
    if (Number.isNaN(ad) || Number.isNaN(bd)) return NaN
    return ad < bd ? -1 : ad > bd ? 1 : 0
  }
  case 'timestamp':
  case 'timestamptz':
    return compareBigInt(timestampToMicros(a), timestampToMicros(b))
  case 'timestamp_ns':
  case 'timestamptz_ns':
    return compareBigInt(timestampToNanos(a), timestampToNanos(b))
  case 'string':
    return a < b ? -1 : a > b ? 1 : 0
  case 'binary':
  case 'uuid':
    return compareBytes(a, b)
  default:
    if (typeName(type).startsWith('fixed[')) return compareBytes(a, b)
    return a < b ? -1 : a > b ? 1 : 0
  }
}

/**
 * Floating bounds must preserve -0.0 and +0.0 distinctly, with -0.0 ordered
 * below +0.0. NaNs are counted separately and not compared for bounds.
 *
 * @param {number} a
 * @param {number} b
 * @returns {number}
 */
export function compareFloating(a, b) {
  if (Object.is(a, b)) return 0
  if (a === 0 && b === 0) return Object.is(a, -0) ? -1 : 1
  return a < b ? -1 : a > b ? 1 : 0
}

/**
 * @param {bigint} a
 * @param {bigint} b
 * @returns {number}
 */
export function compareBigInt(a, b) {
  return a < b ? -1 : a > b ? 1 : 0
}

/**
 * Normalize a `date` value to days since the Unix epoch so a manifest bound
 * (decoded as a day count) and a query literal compare in the same domain.
 * Accepts a `Date` (its UTC day), a bigint or number already in days, or an ISO
 * date string. Returns NaN for anything that can't be read as a date, so the
 * comparator stays undecided and the caller keeps the file rather than
 * mis-pruning. Matches `serializeValue`'s date encoding for `Date` inputs.
 *
 * @param {any} value
 * @returns {number}
 */
export function dateToDays(value) {
  if (value instanceof Date) return Math.floor(value.getTime() / 86400000)
  if (typeof value === 'bigint') return Number(value)
  if (typeof value === 'number') return value
  if (typeof value === 'string') {
    const ms = Date.parse(value)
    return Number.isNaN(ms) ? NaN : Math.floor(ms / 86400000)
  }
  return NaN
}

/**
 * @param {any} value
 * @returns {bigint}
 */
export function timestampToMicros(value) {
  return typeof value === 'bigint' ? value
    : value instanceof Date ? BigInt(value.getTime()) * 1000n
      : BigInt(value)
}

/**
 * @param {any} value
 * @returns {bigint}
 */
export function timestampToNanos(value) {
  return typeof value === 'bigint' ? value
    : value instanceof Date ? BigInt(value.getTime()) * 1000000n
      : BigInt(value)
}

/**
 * Lexicographic unsigned-byte comparison.
 *
 * @param {Uint8Array} a
 * @param {Uint8Array} b
 * @returns {number}
 */
export function compareBytes(a, b) {
  const len = Math.min(a.length, b.length)
  for (let i = 0; i < len; i++) {
    if (a[i] !== b[i]) return a[i] - b[i]
  }
  return a.length - b.length
}

/**
 * Encode a signed bigint as the minimum number of bytes in two's-complement
 * big-endian form. Matches the Iceberg single-value serialization for
 * decimals.
 *
 * @param {bigint} value
 * @returns {Uint8Array}
 */
export function twosComplementMinBigEndian(value) {
  const bytes = []
  let v = value
  while (true) {
    const byte = Number(v & 0xffn)
    bytes.unshift(byte)
    v >>= 8n
    const sign = byte & 0x80
    if (!sign && v === 0n || sign && v === -1n) break
  }
  return new Uint8Array(bytes)
}

/**
 * Decode a two's-complement big-endian byte array into a signed bigint, the
 * inverse of `twosComplementMinBigEndian`.
 *
 * @param {Uint8Array} bytes
 * @returns {bigint}
 */
export function twosComplementBigEndianToBigInt(bytes) {
  if (bytes.length === 0) return 0n
  let v = 0n
  for (const b of bytes) v = v << 8n | BigInt(b)
  const bits = BigInt(bytes.length * 8)
  // sign-extend if the high bit is set
  if (v & 1n << bits - 1n) v -= 1n << bits
  return v
}

/**
 * Parse a canonical UUID string to its 16 big-endian bytes.
 *
 * @param {string} s
 * @returns {Uint8Array|undefined}
 */
export function uuidStringToBytes(s) {
  const hex = s.replace(/-/g, '')
  if (hex.length !== 32) return undefined
  const out = new Uint8Array(16)
  for (let i = 0; i < 16; i++) {
    const byte = parseInt(hex.slice(i * 2, i * 2 + 2), 16)
    if (Number.isNaN(byte)) return undefined
    out[i] = byte
  }
  return out
}
