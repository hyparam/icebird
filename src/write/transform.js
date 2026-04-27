/**
 * Iceberg partition transform implementation. Given a source value and the
 * source field's iceberg type, applies the transform and returns the partition
 * value in the form the manifest's `r102` Avro record expects.
 *
 * @import {IcebergType, PartitionTransform} from '../../src/types.js'
 */

/** @typedef {{ kind: 'identity'|'void'|'year'|'month'|'day'|'hour' } | { kind: 'bucket', n: number } | { kind: 'truncate', w: number }} ParsedTransform */

/**
 * @param {PartitionTransform} transform
 * @returns {ParsedTransform}
 */
export function parseTransform(transform) {
  if (transform === 'identity' || transform === 'void' ||
      transform === 'year' || transform === 'month' ||
      transform === 'day' || transform === 'hour') {
    return { kind: transform }
  }
  let m = /^bucket\[(\d+)\]$/.exec(transform)
  if (m) return { kind: 'bucket', n: parseInt(m[1], 10) }
  m = /^truncate\[(\d+)\]$/.exec(transform)
  if (m) return { kind: 'truncate', w: parseInt(m[1], 10) }
  throw new Error(`unsupported partition transform: ${transform}`)
}

/**
 * Iceberg result type for a transform. Used to type the partition column in
 * the manifest's r102 record.
 *
 * @param {PartitionTransform} transform
 * @param {IcebergType} sourceType
 * @returns {IcebergType}
 */
export function transformResultType(transform, sourceType) {
  const parsed = parseTransform(transform)
  switch (parsed.kind) {
  case 'identity':
  case 'void':
  case 'truncate':
    return sourceType
  case 'year':
  case 'month':
  case 'day':
  case 'hour':
  case 'bucket':
    return 'int'
  }
}

/**
 * Apply the transform to a single source value. Null in → null out.
 *
 * @param {PartitionTransform} transform
 * @param {any} value
 * @param {IcebergType} sourceType
 * @returns {any}
 */
export function applyTransform(transform, value, sourceType) {
  if (value == null) return null
  const parsed = parseTransform(transform)
  switch (parsed.kind) {
  case 'identity': return value
  case 'void': return null
  case 'year': return yearTransform(value, sourceType)
  case 'month': return monthTransform(value, sourceType)
  case 'day': return dayTransform(value, sourceType)
  case 'hour': return hourTransform(value, sourceType)
  case 'bucket': return bucketTransform(value, sourceType, parsed.n)
  case 'truncate': return truncateTransform(value, sourceType, parsed.w)
  }
}

/**
 * @param {IcebergType} type
 * @returns {string}
 */
function typeName(type) {
  return typeof type === 'string' ? type : type.type
}

/**
 * Convert a date / timestamp source value to milliseconds since epoch.
 *
 * @param {any} value
 * @param {IcebergType} sourceType
 * @returns {number}
 */
function dateAsMillis(value, sourceType) {
  const t = typeName(sourceType)
  switch (t) {
  case 'date':
  case 'time':
  case 'timestamp':
  case 'timestamptz':
  case 'timestamp_ns':
  case 'timestamptz_ns': break
  default: throw new Error(`date/time transform: unsupported source type ${t}`)
  }
  if (value instanceof Date) return value.getTime()
  const n = typeof value === 'bigint' ? value : BigInt(value)
  switch (t) {
  case 'date': return Number(n) * 86400000
  case 'time': return Number(n / 1000n)
  case 'timestamp':
  case 'timestamptz': return Number(n / 1000n)
  default: return Number(n / 1000000n) // *_ns
  }
}

/**
 * @param {any} v
 * @param {IcebergType} t
 * @returns {number}
 */
function yearTransform(v, t) {
  return new Date(dateAsMillis(v, t)).getUTCFullYear() - 1970
}

/**
 * @param {any} v
 * @param {IcebergType} t
 * @returns {number}
 */
function monthTransform(v, t) {
  const d = new Date(dateAsMillis(v, t))
  return (d.getUTCFullYear() - 1970) * 12 + d.getUTCMonth()
}

/**
 * @param {any} v
 * @param {IcebergType} t
 * @returns {number}
 */
function dayTransform(v, t) {
  return Math.floor(dateAsMillis(v, t) / 86400000)
}

/**
 * @param {any} v
 * @param {IcebergType} t
 * @returns {number}
 */
function hourTransform(v, t) {
  return Math.floor(dateAsMillis(v, t) / 3600000)
}

/**
 * @param {any} value
 * @param {IcebergType} sourceType
 * @param {number} n
 * @returns {number}
 */
function bucketTransform(value, sourceType, n) {
  const bytes = bucketBytes(value, sourceType)
  const h = murmur3_32(bytes, 0)
  return (h & 0x7fffffff) % n
}

/**
 * Per Iceberg spec: integer-like sources are hashed as 8-byte little-endian
 * longs; strings as UTF-8 bytes; binary/fixed/uuid as raw bytes.
 *
 * @param {any} value
 * @param {IcebergType} sourceType
 * @returns {Uint8Array}
 */
function bucketBytes(value, sourceType) {
  const t = typeName(sourceType)
  switch (t) {
  case 'int':
  case 'long':
  case 'date':
  case 'time':
  case 'timestamp':
  case 'timestamptz':
  case 'timestamp_ns':
  case 'timestamptz_ns': {
    let v
    if (t === 'date') {
      v = value instanceof Date ? BigInt(Math.floor(value.getTime() / 86400000)) : BigInt(value)
    } else if (t === 'timestamp' || t === 'timestamptz') {
      v = value instanceof Date ? BigInt(value.getTime()) * 1000n : BigInt(value)
    } else if (t === 'timestamp_ns' || t === 'timestamptz_ns') {
      v = value instanceof Date ? BigInt(value.getTime()) * 1000000n : BigInt(value)
    } else {
      v = typeof value === 'bigint' ? value : BigInt(value)
    }
    const out = new Uint8Array(8)
    new DataView(out.buffer).setBigInt64(0, v, true)
    return out
  }
  case 'string':
    return new TextEncoder().encode(String(value))
  case 'binary':
  case 'fixed':
  case 'uuid':
    return value instanceof Uint8Array ? value : new Uint8Array(value)
  default:
    throw new Error(`bucket transform: unsupported source type ${t}`)
  }
}

/**
 * @param {any} value
 * @param {IcebergType} sourceType
 * @param {number} w
 * @returns {any}
 */
function truncateTransform(value, sourceType, w) {
  const t = typeName(sourceType)
  switch (t) {
  case 'int': {
    const v = Number(value)
    return v - (v % w + w) % w
  }
  case 'long': {
    const W = BigInt(w)
    const v = typeof value === 'bigint' ? value : BigInt(value)
    return v - (v % W + W) % W
  }
  case 'string': {
    const s = String(value)
    let count = 0
    let i = 0
    while (i < s.length && count < w) {
      const code = /** @type {number} */ (s.codePointAt(i))
      i += code > 0xFFFF ? 2 : 1
      count++
    }
    return s.slice(0, i)
  }
  case 'binary':
  case 'fixed': {
    const b = value instanceof Uint8Array ? value : new Uint8Array(value)
    return b.slice(0, w)
  }
  default:
    throw new Error(`truncate transform: unsupported source type ${t}`)
  }
}

/**
 * Murmur3 x86 32-bit hash, matching Apache Iceberg's BucketUtil.
 *
 * @param {Uint8Array} data
 * @param {number} seed
 * @returns {number}
 */
export function murmur3_32(data, seed) {
  const c1 = 0xcc9e2d51
  const c2 = 0x1b873593
  const len = data.length
  const nBlocks = len >>> 2
  let h1 = seed >>> 0

  for (let i = 0; i < nBlocks; i++) {
    const off = i * 4
    let k1 = data[off] |
      data[off + 1] << 8 |
      data[off + 2] << 16 |
      data[off + 3] << 24
    k1 = Math.imul(k1, c1)
    k1 = k1 << 15 | k1 >>> 17
    k1 = Math.imul(k1, c2)

    h1 ^= k1
    h1 = h1 << 13 | h1 >>> 19
    h1 = Math.imul(h1, 5) + 0xe6546b64 | 0
  }

  let k1 = 0
  const tail = nBlocks * 4
  switch (len & 3) {
  case 3: k1 ^= data[tail + 2] << 16
  // fallthrough
  case 2: k1 ^= data[tail + 1] << 8
  // fallthrough
  case 1:
    k1 ^= data[tail]
    k1 = Math.imul(k1, c1)
    k1 = k1 << 15 | k1 >>> 17
    k1 = Math.imul(k1, c2)
    h1 ^= k1
  }

  h1 ^= len
  h1 ^= h1 >>> 16
  h1 = Math.imul(h1, 0x85ebca6b)
  h1 ^= h1 >>> 13
  h1 = Math.imul(h1, 0xc2b2ae35)
  h1 ^= h1 >>> 16
  return h1 >>> 0
}
