import { typeName } from '../schema.js'
import { computeGeoBounds, isGeoType } from './geospatial.js'
import { compare, serializeValue } from './serde.js'

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
    // Iceberg metrics are reported per-leaf field id. Skip nested top-level
    // fields entirely rather than emit value_counts keyed by the parent id
    // (which Spark/pyiceberg never write).
    if (type === 'list' || type === 'map' || type === 'struct') continue

    if (isGeoType(type)) {
      const { value_count, null_count, lower, upper } = computeGeoBounds(records, field)
      value_counts[field.id] = value_count
      null_value_counts[field.id] = null_count
      if (lower) lower_bounds[field.id] = lower
      if (upper) upper_bounds[field.id] = upper
      continue
    }

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
 * Return whether Icebird can produce Iceberg lower/upper bounds for a type.
 * Geometry/geography are handled separately via `computeGeoBounds`.
 *
 * @param {IcebergType} type
 * @returns {boolean}
 */
function hasComparableBounds(type) {
  const name = typeName(type)
  if (isGeoType(name)) return false
  return name !== 'unknown' && name !== 'variant'
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
  if ((name === 'binary' || name.startsWith('fixed[')) && value instanceof Uint8Array) {
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
  if ((name === 'binary' || name.startsWith('fixed[')) && value instanceof Uint8Array) {
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

