import { deserializeValue } from '../write/serde.js'

/**
 * @import {ScanTopK} from 'squirreling'
 * @import {IcebergType, ManifestEntry, Schema} from '../../src/types.js'
 */

/**
 * Compute a conservative Kth-value threshold from file bounds and counts.
 * For DESC, a file with N rows and lower bound L certifies N rows >= L.
 * The weighted Kth greatest lower bound therefore bounds the actual Kth row
 * from below. Only files whose upper bounds are STRICTLY below it can go.
 * ASC is symmetric. Equal bounds remain, preserving ties and input order.
 * Unknown/null-bearing files remain and contribute no certified rows.
 * Position deletes may supply exact surviving counts: the original bounds
 * still enclose every surviving value. Callers must disable this optimization
 * for equality deletes or row filters whose surviving counts are unknown.
 *
 * @param {ManifestEntry[]} entries
 * @param {Schema} schema
 * @param {ScanTopK} hint
 * @param {(entry: ManifestEntry) => bigint} [rowCount] - Exact surviving rows; defaults to the physical record count.
 * @returns {ManifestEntry[]}
 */
export function pruneTopKFiles(entries, schema, hint, rowCount) {
  if (hint.orderBy.length !== 1) return entries
  const term = hint.orderBy[0]
  const descending = term.direction === 'DESC'
  const field = schema.fields.find(f => f.id === term.field)
  if (!field || typeof field.type !== 'string' ||
      !['int', 'long', 'date', 'timestamp', 'timestamptz', 'timestamp_ns', 'timestamptz_ns', 'string'].includes(field.type)) return entries
  const fieldType = field.type
  const counts = entries.map(entry => rowCount ? rowCount(entry) : entry.data_file.record_count)
  const stats = entries.map((entry, i) => {
    const file = entry.data_file
    const count = counts[i]
    if (count <= 0n || metric(file.null_value_counts, field.id) !== 0n ||
        metric(file.value_counts, field.id) !== file.record_count) return undefined
    const lower = decode(metric(file.lower_bounds, field.id), fieldType)
    const upper = decode(metric(file.upper_bounds, field.id), fieldType)
    if (lower === undefined || upper === undefined || lower > upper) return undefined
    return { count, lower, upper }
  })
  const known = stats.filter(s => s !== undefined)
  const direction = descending ? -1 : 1
  known.sort((a, b) => {
    const av = descending ? a.lower : a.upper
    const bv = descending ? b.lower : b.upper
    return direction * (av < bv ? -1 : av > bv ? 1 : 0)
  })
  let remaining = BigInt(hint.limit)
  for (const stat of known) {
    remaining -= BigInt(stat.count)
    if (remaining > 0n) continue
    const threshold = descending ? stat.lower : stat.upper
    return entries.filter((entry, i) => {
      if (counts[i] === 0n) return false
      const bound = stats[i]
      return !bound || (descending ? bound.upper >= threshold : bound.lower <= threshold)
    })
  }
  return entries.filter((entry, i) => counts[i] !== 0n)
}

/**
 * Iceberg's Avro int-keyed maps decode to key/value arrays.
 *
 * @param {any} map
 * @param {number} id
 * @returns {any}
 */
function metric(map, id) {
  return Array.isArray(map) ? map.find(e => Number(e.key) === id)?.value : map?.[id]
}

/**
 * Match SQL's millisecond Date resolution. Only ASCII string endpoints are
 * used: UTF-8 Iceberg bounds and SQL's UTF-16 comparison agree against these
 * endpoints, including truncated ISO date/time bounds.
 *
 * @param {Uint8Array | undefined} bytes
 * @param {IcebergType} type
 * @returns {number | bigint | string | undefined}
 */
function decode(bytes, type) {
  if (!bytes || typeof type !== 'string') return undefined
  const value = deserializeValue(bytes, type)
  if (type === 'string') {
    return typeof value === 'string' && Array.from(value).every(c => c.charCodeAt(0) < 128) ? value : undefined
  }
  if (type === 'date') {
    const millis = new Date(value * 86400000).getTime()
    return Number.isFinite(millis) ? millis : undefined
  }
  if (type.startsWith('timestamp')) {
    if (typeof value !== 'bigint') return undefined
    const millis = new Date(Number(value / (type.endsWith('_ns') ? 1000000n : 1000n))).getTime()
    return Number.isFinite(millis) ? millis : undefined
  }
  return typeof value === 'bigint' || typeof value === 'number' && Number.isFinite(value) ? value : undefined
}
