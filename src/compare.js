import { typeName } from './schema.js'
import { decimalToUnscaled, parseDecimalType, timestampToMicros, timestampToNanos, uuidToBytes } from './write/conversions.js'

/**
 * Canonical per-type comparators for Iceberg values, shared by the read path
 * (`src/prune.js`) and the write path (`src/write/stats.js`,
 * `src/write/sort.js`). `comparator` and `writeComparator` resolve a type to a
 * comparator once, for loops that compare many values of one column.
 *
 * @import {IcebergType} from './types.js'
 */

/**
 * Compare write inputs, where decimal bigints represent unscaled integers.
 * Read-side comparisons use decoded numbers and must not round query literals.
 *
 * @param {any} a
 * @param {any} b
 * @param {IcebergType} type
 * @returns {number}
 */
export function compareWriteValues(a, b, type) {
  return writeComparator(type)(a, b)
}

/**
 * Resolve the write-input comparator for a type once, so per-value loops
 * (column bounds, sort keys) don't re-parse the type on every comparison.
 *
 * @param {IcebergType} type
 * @returns {(a: any, b: any) => number}
 */
export function writeComparator(type) {
  const decimal = parseDecimalType(typeName(type))
  if (decimal) {
    const { scale } = decimal
    return (a, b) => compareBigInt(decimalToUnscaled(a, scale), decimalToUnscaled(b, scale))
  }
  return comparator(type)
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
  return comparator(type)(a, b)
}

/**
 * Resolve the comparator for two non-null values of the given iceberg type.
 * Every result is a module-level function, so resolving allocates nothing.
 *
 * @param {IcebergType} type
 * @returns {(a: any, b: any) => number}
 */
export function comparator(type) {
  const name = typeName(type)
  switch (name) {
  case 'boolean':
    return compareBoolean
  case 'int':
    return compareOrdered
  case 'float':
  case 'double':
    return compareFloating
  case 'long':
    return compareLong
  case 'date':
    return compareDate
  case 'timestamp':
  case 'timestamptz':
    return compareTimestampMicros
  case 'timestamp_ns':
  case 'timestamptz_ns':
    return compareTimestampNanos
  case 'string':
    return compareStringsCodePoint
  case 'binary':
    return compareBytes
  case 'uuid':
    return compareUuid
  default:
    if (name.startsWith('fixed[')) return compareBytes
    return compareOrdered
  }
}

/**
 * @param {any} a
 * @param {any} b
 * @returns {number}
 */
function compareBoolean(a, b) {
  return (a ? 1 : 0) - (b ? 1 : 0)
}

/**
 * @param {any} a
 * @param {any} b
 * @returns {number}
 */
function compareOrdered(a, b) {
  return a < b ? -1 : a > b ? 1 : 0
}

/**
 * @param {any} a
 * @param {any} b
 * @returns {number}
 */
function compareLong(a, b) {
  const ai = typeof a === 'bigint' ? a : BigInt(a)
  const bi = typeof b === 'bigint' ? b : BigInt(b)
  return ai < bi ? -1 : ai > bi ? 1 : 0
}

/**
 * Bounds decode to days-since-epoch, but query literals can be `Date` objects
 * (or ISO strings). Normalize both sides to days so they compare in one
 * domain; NaN keeps the comparison undecided so callers don't mis-prune.
 *
 * @param {any} a
 * @param {any} b
 * @returns {number}
 */
function compareDate(a, b) {
  const ad = dateToDays(a)
  const bd = dateToDays(b)
  if (Number.isNaN(ad) || Number.isNaN(bd)) return NaN
  return ad < bd ? -1 : ad > bd ? 1 : 0
}

/**
 * Two `Date`s compare by their millisecond times directly, which is exact and
 * skips the bigint conversion; any other mix goes through microseconds.
 *
 * @param {any} a
 * @param {any} b
 * @returns {number}
 */
function compareTimestampMicros(a, b) {
  if (a instanceof Date && b instanceof Date) return compareOrdered(a.getTime(), b.getTime())
  return compareBigInt(timestampToMicros(a), timestampToMicros(b))
}

/**
 * @param {any} a
 * @param {any} b
 * @returns {number}
 */
function compareTimestampNanos(a, b) {
  if (a instanceof Date && b instanceof Date) return compareOrdered(a.getTime(), b.getTime())
  return compareBigInt(timestampToNanos(a), timestampToNanos(b))
}

/**
 * Avro decodes UUIDs as strings; bounds and write inputs can be bytes.
 *
 * @param {any} a
 * @param {any} b
 * @returns {number}
 */
function compareUuid(a, b) {
  return compareBytes(uuidToBytes(a, 'uuid value'), uuidToBytes(b, 'uuid value'))
}

/**
 * Compare two strings by Unicode code point, which is the same order as their
 * UTF-8 byte encodings - Iceberg's order for string bounds and sort keys. JS
 * `<` compares UTF-16 code units instead, which disagrees whenever a character
 * in U+E000..U+FFFF meets a supplementary character (its surrogates sort below
 * U+E000), so bounds picked or pruned with `<` can be wrong for such strings.
 *
 * Walks UTF-16 units: equal units mean equal code points, and where the first
 * differing pair holds no surrogate, unit order is code point order. Only a
 * difference at a surrogate takes the code point walk. No allocation.
 *
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
export function compareStringsCodePoint(a, b) {
  // Throw rather than coerce: pruning wraps comparisons in safeCompare, which
  // turns a throw into "undecidable, keep the file"; a coerced comparison of a
  // non-string literal would instead order garbage and could mis-prune.
  if (typeof a !== 'string' || typeof b !== 'string') {
    throw new Error('compareStringsCodePoint expects two strings')
  }
  if (a === b) return 0
  const n = a.length < b.length ? a.length : b.length
  for (let i = 0; i < n; i++) {
    const ca = a.charCodeAt(i)
    const cb = b.charCodeAt(i)
    if (ca !== cb) {
      if ((ca & 0xf800) === 0xd800 || (cb & 0xf800) === 0xd800) return compareCodePointWalk(a, b)
      return ca < cb ? -1 : 1
    }
  }
  return a.length === b.length ? 0 : a.length < b.length ? -1 : 1
}

/**
 * The code point walk, for strings that differ at a surrogate. Lone
 * surrogates compare as their own unit value.
 *
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
function compareCodePointWalk(a, b) {
  let i = 0
  while (i < a.length && i < b.length) {
    const ca = /** @type {number} */ (a.codePointAt(i))
    const cb = /** @type {number} */ (b.codePointAt(i))
    if (ca !== cb) return ca < cb ? -1 : 1
    // Equal code points advance both strings by the same number of units.
    i += ca > 0xffff ? 2 : 1
  }
  return a.length === b.length ? 0 : a.length < b.length ? -1 : 1
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
