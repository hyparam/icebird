/**
 * Shared Iceberg value conversion helpers for write-path metadata encoding.
 */

/**
 * Parse iceberg `decimal(P,S)` / `decimal(P, S)` strings into precision and
 * scale. Returns undefined for non-decimal types so callers can fall through.
 *
 * @param {string} type
 * @returns {{ precision: number, scale: number } | undefined}
 */
export function parseDecimalType(type) {
  const m = /^decimal\((\d+),\s*(\d+)\)$/.exec(type)
  if (!m) return undefined
  return { precision: parseInt(m[1], 10), scale: parseInt(m[2], 10) }
}

/**
 * Minimum bytes required for an Avro/Parquet fixed decimal of the given
 * precision. Matches Iceberg's TypeUtil.decimalRequiredBytes.
 *
 * @param {number} precision
 * @returns {number}
 */
export function decimalRequiredBytes(precision) {
  const limit = 10n ** BigInt(precision)
  let n = 1
  let bound = 128n
  while (limit > bound) {
    n++
    bound <<= 8n
  }
  return n
}

/**
 * Match Parquet decimal inputs: bigint is unscaled; number is scaled and rounded.
 *
 * @param {number | bigint} value
 * @param {number} scale
 * @returns {bigint}
 */
export function decimalToUnscaled(value, scale) {
  return typeof value === 'bigint' ? value : BigInt(Math.round(value * 10 ** scale))
}

/**
 * Convert a decimal value to Avro fixed-width two's-complement bytes.
 *
 * @param {any} value
 * @param {number} precision
 * @param {number} scale
 * @param {string} label
 * @returns {Uint8Array}
 */
export function decimalToFixedBytes(value, precision, scale, label) {
  const size = decimalRequiredBytes(precision)
  if (value instanceof Uint8Array) {
    if (value.length !== size) throw new Error(`expected ${label}`)
    return value
  }
  if (typeof value !== 'number' && typeof value !== 'bigint') {
    throw new Error(`expected ${label}`)
  }
  const unscaled = decimalToUnscaled(value, scale)
  const limit = 10n ** BigInt(precision)
  if (unscaled >= limit || unscaled <= -limit) {
    throw new Error(`${label} exceeds precision ${precision}`)
  }
  return bigintToFixedBytes(unscaled, size, label)
}

/**
 * @param {any} value
 * @returns {Uint8Array}
 */
export function toUint8Array(value) {
  return value instanceof Uint8Array ? value : new Uint8Array(value)
}

/**
 * @param {any} value
 * @param {string} label
 * @returns {Uint8Array}
 */
export function uuidToBytes(value, label) {
  if (value instanceof Uint8Array) {
    if (value.length !== 16) throw new Error(`expected ${label}`)
    return value
  }
  if (typeof value !== 'string') throw new Error(`expected ${label}`)
  const hex = value.toLowerCase().replace(/-/g, '')
  if (!/^[0-9a-f]{32}$/.test(hex)) throw new Error(`expected ${label}`)
  const bytes = new Uint8Array(16)
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  }
  return bytes
}

/**
 * @param {bigint} value
 * @param {number} size
 * @param {string} label
 * @returns {Uint8Array}
 */
function bigintToFixedBytes(value, size, label) {
  const bytes = new Uint8Array(size)
  let v = value
  for (let i = size - 1; i >= 0; i--) {
    bytes[i] = Number(v & 0xffn)
    v >>= 8n
  }
  const negative = value < 0n
  const signBitSet = (bytes[0] & 0x80) !== 0
  if (!negative && (v !== 0n || signBitSet) ||
      negative && (v !== -1n || !signBitSet)) {
    throw new Error(`${label} does not fit in ${size} bytes`)
  }
  return bytes
}
