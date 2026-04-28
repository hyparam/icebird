/**
 * @import {FieldSummary, IcebergType, Schema} from '../../src/types.js'
 */

// Iceberg's default `write.metadata.metrics.default` is `truncate(16)`.
// Bounds for string and binary columns are truncated to 16 units (UTF-8
// code points for string, bytes for binary). Lower bounds keep the prefix;
// upper bounds increment the prefix so the truncated value still bounds
// the column from above.
const TRUNCATE_LIMIT = 16

/**
 * Compute per-column statistics for a batch of records.
 *
 * Returns the optional stat maps used in a manifest entry's data_file
 * (value_counts, null_value_counts, nan_value_counts, lower_bounds,
 * upper_bounds), keyed by field id. Bounds are serialized per the Iceberg
 * single-value serialization spec; columns whose type lacks a serializer
 * are omitted from the bounds maps.
 *
 * @param {Record<string, any>[]} records
 * @param {Schema} schema
 * @returns {{
 *   value_counts: Record<number, bigint>,
 *   null_value_counts: Record<number, bigint>,
 *   nan_value_counts: Record<number, bigint>,
 *   lower_bounds: Record<number, Uint8Array>,
 *   upper_bounds: Record<number, Uint8Array>,
 * }}
 */
export function computeColumnStats(records, schema) {
  /** @type {Record<number, bigint>} */
  const value_counts = {}
  /** @type {Record<number, bigint>} */
  const null_value_counts = {}
  /** @type {Record<number, bigint>} */
  const nan_value_counts = {}
  /** @type {Record<number, Uint8Array>} */
  const lower_bounds = {}
  /** @type {Record<number, Uint8Array>} */
  const upper_bounds = {}

  for (const field of schema.fields) {
    const type = typeName(field.type)
    if (type === 'unknown') continue

    let nulls = 0n
    let nans = 0n
    let min
    let max
    const isFloat = type === 'float' || type === 'double'
    const trackBounds = hasComparableBounds(field.type)
    const writeDefault = field['write-default']
    for (const record of records) {
      let v = record[field.name]
      if (v === undefined && writeDefault !== undefined) v = writeDefault
      if (v === null || v === undefined) {
        nulls++
        continue
      }
      if (isFloat && Number.isNaN(v)) {
        nans++
        continue
      }
      if (trackBounds) {
        if (min === undefined || compare(v, min, field.type) < 0) min = v
        if (max === undefined || compare(v, max, field.type) > 0) max = v
      }
    }
    value_counts[field.id] = BigInt(records.length)
    null_value_counts[field.id] = nulls
    if (isFloat) nan_value_counts[field.id] = nans
    if (min !== undefined) {
      const lo = serializeValue(truncateLower(min, field.type), field.type)
      if (lo) lower_bounds[field.id] = lo
    }
    if (max !== undefined) {
      const truncated = truncateUpper(max, field.type)
      if (truncated !== undefined) {
        const hi = serializeValue(truncated, field.type)
        if (hi) upper_bounds[field.id] = hi
      }
    }
  }

  return { value_counts, null_value_counts, nan_value_counts, lower_bounds, upper_bounds }
}

/**
 * Compare two non-null values of the given iceberg type.
 *
 * @param {any} a
 * @param {any} b
 * @param {IcebergType} type
 * @returns {number}
 */
function compare(a, b, type) {
  switch (typeName(type)) {
  case 'boolean':
    return (a ? 1 : 0) - (b ? 1 : 0)
  case 'int':
  case 'float':
  case 'double':
    return a < b ? -1 : a > b ? 1 : 0
  case 'long': {
    const ai = typeof a === 'bigint' ? a : BigInt(a)
    const bi = typeof b === 'bigint' ? b : BigInt(b)
    return ai < bi ? -1 : ai > bi ? 1 : 0
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
    return a < b ? -1 : a > b ? 1 : 0
  }
}

/**
 * @param {bigint} a
 * @param {bigint} b
 * @returns {number}
 */
function compareBigInt(a, b) {
  return a < b ? -1 : a > b ? 1 : 0
}

/**
 * @param {any} value
 * @returns {bigint}
 */
function timestampToMicros(value) {
  return typeof value === 'bigint' ? value
    : value instanceof Date ? BigInt(value.getTime()) * 1000n
      : BigInt(value)
}

/**
 * @param {any} value
 * @returns {bigint}
 */
function timestampToNanos(value) {
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
function compareBytes(a, b) {
  const len = Math.min(a.length, b.length)
  for (let i = 0; i < len; i++) {
    if (a[i] !== b[i]) return a[i] - b[i]
  }
  return a.length - b.length
}

/**
 * Serialize a value per the Iceberg single-value serialization spec.
 * Returns undefined for types we don't yet support so the bound is omitted.
 *
 * @param {any} value
 * @param {IcebergType} type
 * @returns {Uint8Array|undefined}
 */
function serializeValue(value, type) {
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
 * Return whether Icebird can produce Iceberg lower/upper bounds for a type.
 *
 * @param {IcebergType} type
 * @returns {boolean}
 */
function hasComparableBounds(type) {
  const name = typeName(type)
  if (name.startsWith('geometry') || name.startsWith('geography')) {
    return false
  }
  return name !== 'unknown' && name !== 'variant'
}

/**
 * @param {IcebergType} type
 * @returns {string}
 */
function typeName(type) {
  return typeof type === 'string' ? type : type.type
}

/**
 * Aggregate a list of partition values into an Iceberg manifest-list
 * `field_summary` (contains_null, contains_nan, lower_bound, upper_bound).
 * Values must already be in the transform's result-type form.
 *
 * @param {any[]} values
 * @param {IcebergType} type
 * @returns {FieldSummary}
 */
export function computeFieldSummary(values, type) {
  const name = typeName(type)
  const isFloat = name === 'float' || name === 'double'
  const trackBounds = hasComparableBounds(type)
  let containsNull = false
  let containsNan = false
  let min
  let max
  for (const v of values) {
    if (v === null || v === undefined) {
      containsNull = true
      continue
    }
    if (isFloat && Number.isNaN(v)) {
      containsNan = true
      continue
    }
    if (trackBounds) {
      if (min === undefined || compare(v, min, type) < 0) min = v
      if (max === undefined || compare(v, max, type) > 0) max = v
    }
  }
  /** @type {FieldSummary} */
  const summary = { contains_null: containsNull }
  if (isFloat) summary.contains_nan = containsNan
  if (min !== undefined) {
    const lo = serializeValue(truncateLower(min, type), type)
    if (lo) summary.lower_bound = lo
  }
  if (max !== undefined) {
    const truncated = truncateUpper(max, type)
    if (truncated !== undefined) {
      const hi = serializeValue(truncated, type)
      if (hi) summary.upper_bound = hi
    }
  }
  return summary
}

/**
 * Truncate a value for use as a lower bound. Strings are truncated at
 * unicode code-point boundaries; binary values are truncated at byte
 * boundaries. Other types are returned unchanged.
 *
 * @param {any} value
 * @param {IcebergType} type
 * @returns {any}
 */
function truncateLower(value, type) {
  const name = typeName(type)
  if (name === 'string' && typeof value === 'string') {
    const cps = Array.from(value)
    if (cps.length <= TRUNCATE_LIMIT) return value
    return cps.slice(0, TRUNCATE_LIMIT).join('')
  }
  if (name === 'binary' && value instanceof Uint8Array) {
    if (value.length <= TRUNCATE_LIMIT) return value
    return value.slice(0, TRUNCATE_LIMIT)
  }
  return value
}

/**
 * Truncate a value for use as an upper bound. The truncated prefix is
 * incremented (last code point for strings, last byte for binary) so the
 * result is still ≥ the original value. Returns `undefined` if no valid
 * upper truncation exists (e.g. all 0xFF bytes, or string ending in U+10FFFF).
 *
 * @param {any} value
 * @param {IcebergType} type
 * @returns {any}
 */
function truncateUpper(value, type) {
  const name = typeName(type)
  if (name === 'string' && typeof value === 'string') {
    const cps = Array.from(value)
    if (cps.length <= TRUNCATE_LIMIT) return value
    const prefix = cps.slice(0, TRUNCATE_LIMIT)
    while (prefix.length > 0) {
      const cp = /** @type {number} */ (prefix[prefix.length - 1].codePointAt(0))
      // Skip the UTF-16 surrogate range when incrementing.
      const next = cp + 1 === 0xD800 ? 0xE000 : cp + 1
      if (next <= 0x10FFFF) {
        prefix[prefix.length - 1] = String.fromCodePoint(next)
        return prefix.join('')
      }
      prefix.pop()
    }
    return undefined
  }
  if (name === 'binary' && value instanceof Uint8Array) {
    if (value.length <= TRUNCATE_LIMIT) return value
    const prefix = value.slice(0, TRUNCATE_LIMIT)
    for (let i = prefix.length - 1; i >= 0; i--) {
      if (prefix[i] < 0xFF) {
        const out = prefix.slice(0, i + 1)
        out[i]++
        return out
      }
    }
    return undefined
  }
  return value
}

/**
 * Encode a signed bigint as the minimum number of bytes in two's-complement
 * big-endian form. Matches the Iceberg single-value serialization for
 * decimals.
 *
 * @param {bigint} value
 * @returns {Uint8Array}
 */
function twosComplementMinBigEndian(value) {
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
 * Parse a canonical UUID string to its 16 big-endian bytes.
 *
 * @param {string} s
 * @returns {Uint8Array|undefined}
 */
function uuidStringToBytes(s) {
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
