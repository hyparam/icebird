import { typeName } from './schema.js'
import { applyTransform } from './write/transform.js'
import { compare, deserializeValue } from './write/serde.js'

/**
 * Partition-level scan pruning. Given a hyparquet query filter (keyed by
 * iceberg field name) and a data manifest entry, decide whether the entry's
 * partition tuple could contain a row matching the filter.
 *
 * This is an inclusive projection (Iceberg spec, Scan Planning): a file is
 * skipped only when its partition values prove that no row can match. The
 * implementation is deliberately conservative: on any uncertainty (unknown
 * transform, type mismatch, unconvertible literal, non-partition column) the
 * file is kept. Pruning never changes query results, only which files are read.
 *
 * @import {ParquetQueryFilter} from 'hyparquet'
 * @import {DataFile, IcebergType, ManifestEntry, PartitionSpec, Schema, TableMetadata} from '../src/types.js'
 */

/**
 * @param {ParquetQueryFilter} filter - Filter keyed by iceberg field name.
 * @param {ManifestEntry} dataEntry
 * @param {Schema} schema - Current schema (filter column names map to its fields).
 * @param {TableMetadata} metadata
 * @returns {boolean} true if the file must be read, false if it can be skipped.
 */
export function partitionMightMatch(filter, dataEntry, schema, metadata) {
  const spec = metadata['partition-specs'].find(s => s['spec-id'] === dataEntry.partition_spec_id)
  // No spec or unpartitioned: nothing to prune on.
  if (!spec || spec.fields.length === 0) return true
  /** @type {PruneContext} */
  const ctx = { spec, schema, partition: dataEntry.data_file.partition }
  return nodeMightMatch(filter, (column, condition) => columnMightMatch(column, condition, ctx))
}

/**
 * File-level scan pruning via per-column manifest bounds. Given a hyparquet
 * query filter (keyed by iceberg field name) and a data manifest entry, decide
 * whether the entry's `lower_bounds` / `upper_bounds` could contain a row
 * matching the filter.
 *
 * Like `partitionMightMatch` this is an inclusive projection: a file is skipped
 * only when its bounds prove that no row can match, and any uncertainty keeps
 * the file. Bounds for string/binary/fixed/uuid are stored truncated (Iceberg
 * `truncate(16)` metrics) so those types are never range-pruned. Pruning never
 * changes query results, only which files are read.
 *
 * @param {ParquetQueryFilter} filter - Filter keyed by iceberg field name.
 * @param {ManifestEntry} dataEntry
 * @param {Schema} schema - Current schema (filter column names map to its fields).
 * @returns {boolean} true if the file must be read, false if it can be skipped.
 */
export function fileMightMatch(filter, dataEntry, schema) {
  const { lower_bounds, upper_bounds } = dataEntry.data_file
  // No bounds at all: nothing to prune on.
  if (lower_bounds === undefined && upper_bounds === undefined) return true
  return nodeMightMatch(filter, (column, condition) =>
    boundsMightMatch(column, condition, dataEntry.data_file, schema))
}

/**
 * @typedef {{ spec: PartitionSpec, schema: Schema, partition: DataFile['partition'] }} PruneContext
 */

/**
 * Generic AND/OR/$nor walk over a hyparquet filter. Top-level keys are
 * AND-combined: the file is ruled out if any branch is ruled out. The leaf
 * evaluator decides a single `{column: condition}` predicate and returns false
 * only when it proves the file cannot match.
 *
 * @param {ParquetQueryFilter} node
 * @param {(column: string, condition: any) => boolean} leafFn
 * @returns {boolean}
 */
function nodeMightMatch(node, leafFn) {
  if (!node || typeof node !== 'object') return true
  const anyNode = /** @type {Record<string, any>} */ (node)
  for (const [key, val] of Object.entries(anyNode)) {
    if (key === '$and') {
      const subs = /** @type {ParquetQueryFilter[]} */ (val)
      if (!subs.every(sub => nodeMightMatch(sub, leafFn))) return false
    } else if (key === '$or') {
      const subs = /** @type {ParquetQueryFilter[]} */ (val)
      if (!subs.some(sub => nodeMightMatch(sub, leafFn))) return false
    } else if (key === '$nor') {
      // NOT(a OR b): can't safely prune, keep.
      continue
    } else {
      if (!leafFn(key, val)) return false
    }
  }
  return true
}

/**
 * @param {string} column - iceberg field name
 * @param {any} condition - operator object like {$eq: x} or a bare value (eq)
 * @param {PruneContext} ctx
 * @returns {boolean}
 */
function columnMightMatch(column, condition, ctx) {
  const field = ctx.schema.fields.find(f => f.name === column)
  if (!field) return true
  const partitionFields = ctx.spec.fields.filter(pf => pf['source-id'] === field.id)
  if (partitionFields.length === 0) return true

  for (const { op, value } of normalizeCondition(condition)) {
    for (const pf of partitionFields) {
      const v = ctx.partition[pf.name]
      if (!opMightMatch(op, value, v, pf.transform, field.type)) return false
    }
  }
  return true
}

/**
 * Normalize a filter condition to a list of {op, value} pairs. A bare value
 * (not an operator object) is treated as an equality predicate.
 *
 * @param {any} condition
 * @returns {{ op: string, value: any }[]}
 */
function normalizeCondition(condition) {
  if (condition && typeof condition === 'object' && !Array.isArray(condition) &&
      !(condition instanceof Date) &&
      Object.keys(condition).some(k => k.startsWith('$'))) {
    return Object.entries(condition).map(([op, value]) => ({ op, value }))
  }
  return [{ op: '$eq', value: condition }]
}

/**
 * Whether a file with partition value `v` could match `op value` under the
 * given transform. Returns true (keep) on any uncertainty.
 *
 * @param {string} op - mongo-style operator ($eq,$ne,$lt,$lte,$gt,$gte,$in,$nin)
 * @param {any} value - the predicate literal (array for $in/$nin)
 * @param {any} v - the file's partition value (already transformed + decoded)
 * @param {string} transform
 * @param {IcebergType} sourceType
 * @returns {boolean}
 */
function opMightMatch(op, value, v, transform, sourceType) {
  // A null partition value can never equal a concrete literal.
  if (v === null || v === undefined) {
    if (op === '$eq' && value !== null && value !== undefined) return false
    if (op === '$in' && Array.isArray(value) && value.length > 0 &&
        value.every(x => x !== null && x !== undefined)) return false
    return true
  }

  const kind = transformKind(transform)
  if (kind === 'identity') return identityMightMatch(op, v, value)
  if (kind === 'monotonic') return monotonicMightMatch(op, v, value, transform, sourceType)
  if (kind === 'bucket') return bucketMightMatch(op, v, value, transform, sourceType)
  // void / unknown transform: keep.
  return true
}

/**
 * @param {string} transform
 * @returns {'identity'|'monotonic'|'bucket'|'other'}
 */
function transformKind(transform) {
  if (transform === 'identity') return 'identity'
  if (transform.startsWith('bucket[')) return 'bucket'
  if (transform.startsWith('truncate[') ||
      transform === 'year' || transform === 'month' ||
      transform === 'day' || transform === 'hour') return 'monotonic'
  return 'other'
}

/**
 * Identity transform: every row in the file shares the source value `v`, so
 * predicates can be evaluated exactly against it.
 *
 * @param {string} op
 * @param {any} v
 * @param {any} value
 * @returns {boolean}
 */
function identityMightMatch(op, v, value) {
  switch (op) {
  case '$eq': return equals(v, value) !== false
  case '$ne': return equals(v, value) !== true
  case '$lt': return relOrder(v, value, c => c < 0)
  case '$lte': return relOrder(v, value, c => c <= 0)
  case '$gt': return relOrder(v, value, c => c > 0)
  case '$gte': return relOrder(v, value, c => c >= 0)
  case '$in':
    if (!Array.isArray(value)) return true
    return value.some(x => equals(v, x) !== false)
  case '$nin':
    if (!Array.isArray(value)) return true
    // Ruled out only if v is definitely one of the excluded values.
    return !value.some(x => equals(v, x) === true)
  default: return true
  }
}

/**
 * Monotonic (order-preserving) transforms: project the literal through the
 * transform and compare in partition space. Permissive on boundaries so a file
 * is never wrongly skipped. Cannot prune on `$ne`/`$nin`.
 *
 * @param {string} op
 * @param {any} v
 * @param {any} value
 * @param {string} transform
 * @param {IcebergType} sourceType
 * @returns {boolean}
 */
function monotonicMightMatch(op, v, value, transform, sourceType) {
  if (op === '$ne' || op === '$nin') return true
  if (op === '$in') {
    if (!Array.isArray(value)) return true
    return value.some(x => {
      const t = project(transform, x, sourceType)
      return t === undefined || equals(v, t) !== false
    })
  }
  const t = project(transform, value, sourceType)
  if (t === undefined) return true
  switch (op) {
  case '$eq': return equals(v, t) !== false
  case '$lt':
  case '$lte': return relOrder(v, t, c => c <= 0)
  case '$gt':
  case '$gte': return relOrder(v, t, c => c >= 0)
  default: return true
  }
}

/**
 * Bucket transform: not order-preserving, so only equality predicates project.
 *
 * @param {string} op
 * @param {any} v
 * @param {any} value
 * @param {string} transform
 * @param {IcebergType} sourceType
 * @returns {boolean}
 */
function bucketMightMatch(op, v, value, transform, sourceType) {
  if (op === '$eq') {
    const b = project(transform, value, sourceType)
    return b === undefined || equals(v, b) !== false
  }
  if (op === '$in') {
    if (!Array.isArray(value)) return true
    return value.some(x => {
      const b = project(transform, x, sourceType)
      return b === undefined || equals(v, b) !== false
    })
  }
  return true
}

/**
 * Project a literal through a partition transform, returning undefined if the
 * transform throws (e.g. wrong literal type) so the caller keeps the file.
 *
 * @param {string} transform
 * @param {any} value
 * @param {IcebergType} sourceType
 * @returns {any}
 */
function project(transform, value, sourceType) {
  if (value === null || value === undefined) return undefined
  try {
    return applyTransform(/** @type {any} */ (transform), value, sourceType)
  } catch {
    return undefined
  }
}

/**
 * Apply `test` to the ordering of `a` and `b`. Returns true (keep) when the
 * two values are not orderable, so an undecidable predicate never prunes.
 *
 * @param {any} a
 * @param {any} b
 * @param {(c: number) => boolean} test
 * @returns {boolean}
 */
function relOrder(a, b, test) {
  const c = compareOrder(a, b)
  return c === undefined ? true : test(c)
}

/**
 * Decide equality of two partition values. Returns true/false when decidable,
 * or undefined when the values are not safely comparable (mismatched domains,
 * Date vs non-Date, bigints outside the safe integer range). Equality is
 * decidable for strings and booleans even though their ordering is not.
 *
 * @param {any} a
 * @param {any} b
 * @returns {boolean | undefined}
 */
function equals(a, b) {
  if (a === null || a === undefined || b === null || b === undefined) return undefined

  const aDate = a instanceof Date
  const bDate = b instanceof Date
  if (aDate !== bDate) return undefined
  if (aDate && bDate) return a.getTime() === b.getTime()

  if (typeof a === 'bigint' && typeof b === 'bigint') return a === b
  if (typeof a === 'string' && typeof b === 'string') return a === b
  if (typeof a === 'boolean' && typeof b === 'boolean') return a === b

  const na = numericOf(a)
  const nb = numericOf(b)
  if (na !== undefined && nb !== undefined) return na === nb

  return undefined
}

/**
 * Order two partition values. Returns -1/0/1, or undefined when the values are
 * not safely orderable. Strings and booleans are intentionally not ordered
 * (JS UTF-16 order can differ from Iceberg's UTF-8 order), so range predicates
 * on them never prune.
 *
 * @param {any} a
 * @param {any} b
 * @returns {number | undefined}
 */
function compareOrder(a, b) {
  if (a === null || a === undefined || b === null || b === undefined) return undefined

  const aDate = a instanceof Date
  const bDate = b instanceof Date
  if (aDate !== bDate) return undefined
  if (aDate && bDate) return sign(a.getTime() - b.getTime())

  if (typeof a === 'bigint' && typeof b === 'bigint') return a < b ? -1 : a > b ? 1 : 0

  const na = numericOf(a)
  const nb = numericOf(b)
  if (na !== undefined && nb !== undefined) return na < nb ? -1 : na > nb ? 1 : 0

  return undefined
}

/**
 * @param {any} x
 * @returns {number | undefined}
 */
function numericOf(x) {
  if (typeof x === 'number') return Number.isFinite(x) ? x : undefined
  if (typeof x === 'bigint') {
    if (x <= BigInt(Number.MAX_SAFE_INTEGER) && x >= BigInt(Number.MIN_SAFE_INTEGER)) return Number(x)
    return undefined
  }
  return undefined
}

/**
 * @param {number} n
 * @returns {number}
 */
function sign(n) {
  return n < 0 ? -1 : n > 0 ? 1 : 0
}

/**
 * Whether the file could match `condition` on `column` given the column's
 * decoded [lower, upper] bounds in the manifest entry. Returns true (keep) on
 * any uncertainty, including columns without bounds and non-orderable types.
 *
 * @param {string} column - iceberg field name
 * @param {any} condition - operator object like {$eq: x} or a bare value (eq)
 * @param {DataFile} dataFile
 * @param {Schema} schema
 * @returns {boolean}
 */
function boundsMightMatch(column, condition, dataFile, schema) {
  const field = schema.fields.find(f => f.name === column)
  if (!field) return true
  // Bounds and metric ordering are only defined for orderable scalar types.
  // String/binary/fixed/uuid bounds are truncated prefixes, so we never use
  // them for pruning (equality on a truncated prefix is undecidable).
  if (!isOrderableForBounds(field.type)) return true

  const lowerBytes = boundForField(dataFile.lower_bounds, field.id)
  const upperBytes = boundForField(dataFile.upper_bounds, field.id)
  if (lowerBytes === undefined && upperBytes === undefined) return true
  const lo = lowerBytes !== undefined ? deserializeValue(lowerBytes, field.type) : undefined
  const hi = upperBytes !== undefined ? deserializeValue(upperBytes, field.type) : undefined
  if (lo === undefined && hi === undefined) return true

  for (const { op, value } of normalizeCondition(condition)) {
    if (!boundsOpMightMatch(op, value, lo, hi, field.type)) return false
  }
  return true
}

/**
 * Look up a column's bound bytes by field id. Read-decoded Iceberg maps arrive
 * as an array of `{key, value}` records (Avro int-keyed maps); hand-built
 * entries may instead be a plain `Record<fieldId, bytes>`. Handle both.
 *
 * @param {any} map - lower_bounds or upper_bounds from a manifest data_file
 * @param {number} fieldId
 * @returns {Uint8Array | undefined}
 */
function boundForField(map, fieldId) {
  if (map === undefined || map === null) return undefined
  if (Array.isArray(map)) {
    const entry = map.find(e => e && Number(e.key) === fieldId)
    return entry ? entry.value : undefined
  }
  const v = map[fieldId]
  return v instanceof Uint8Array ? v : undefined
}

/**
 * Whether a type has totally-ordered, untruncated single-value bounds suitable
 * for range/equality pruning. String/binary/fixed/uuid are excluded because
 * their bounds are stored truncated.
 *
 * @param {IcebergType} type
 * @returns {boolean}
 */
function isOrderableForBounds(type) {
  const name = typeName(type)
  if (name.startsWith('decimal(')) return true
  switch (name) {
  case 'boolean':
  case 'int':
  case 'long':
  case 'float':
  case 'double':
  case 'date':
  case 'time':
  case 'timestamp':
  case 'timestamptz':
  case 'timestamp_ns':
  case 'timestamptz_ns':
    return true
  default:
    return false
  }
}

/**
 * Whether a value range [lo, hi] (either side may be open/undefined) could
 * satisfy `op value`. Mirrors hyparquet's `canSkipRowGroup` operator semantics
 * but at file granularity. Returns true (keep) on any uncertainty; returns
 * false only when the predicate is provably unsatisfiable for the whole range.
 *
 * @param {string} op - mongo-style operator
 * @param {any} value - the predicate literal (array for $in/$nin)
 * @param {any} lo - decoded lower bound, or undefined (open below)
 * @param {any} hi - decoded upper bound, or undefined (open above)
 * @param {IcebergType} type
 * @returns {boolean}
 */
function boundsOpMightMatch(op, value, lo, hi, type) {
  switch (op) {
  case '$lt': {
    // need some x < value; smallest is lo. Skip if lo >= value.
    if (lo === undefined) return true
    const c = safeCompare(lo, value, type)
    return c === undefined ? true : c < 0
  }
  case '$lte': {
    if (lo === undefined) return true
    const c = safeCompare(lo, value, type)
    return c === undefined ? true : c <= 0
  }
  case '$gt': {
    // need some x > value; largest is hi. Skip if hi <= value.
    if (hi === undefined) return true
    const c = safeCompare(hi, value, type)
    return c === undefined ? true : c > 0
  }
  case '$gte': {
    if (hi === undefined) return true
    const c = safeCompare(hi, value, type)
    return c === undefined ? true : c >= 0
  }
  case '$eq':
    return eqInRange(value, lo, hi, type)
  case '$in':
    if (!Array.isArray(value)) return true
    // Keep if any listed value could fall in [lo, hi].
    return value.some(x => eqInRange(x, lo, hi, type))
  // $ne / $nin can only prune a single-valued file fully covered by the
  // excluded value(s); too rare to bother — keep.
  default:
    return true
  }
}

/**
 * Whether `value` could lie within [lo, hi] (open sides allowed). Keep on any
 * undecidable comparison.
 *
 * @param {any} value
 * @param {any} lo
 * @param {any} hi
 * @param {IcebergType} type
 * @returns {boolean}
 */
function eqInRange(value, lo, hi, type) {
  if (lo !== undefined) {
    const c = safeCompare(value, lo, type)
    if (c !== undefined && c < 0) return false
  }
  if (hi !== undefined) {
    const c = safeCompare(value, hi, type)
    if (c !== undefined && c > 0) return false
  }
  return true
}

/**
 * Type-aware comparison that returns undefined (rather than throwing or
 * returning NaN) when the two values cannot be meaningfully ordered, so the
 * caller keeps the file.
 *
 * @param {any} a
 * @param {any} b
 * @param {IcebergType} type
 * @returns {number | undefined}
 */
function safeCompare(a, b, type) {
  if (a === null || a === undefined || b === null || b === undefined) return undefined
  try {
    const c = compare(a, b, type)
    return Number.isNaN(c) ? undefined : c
  } catch {
    return undefined
  }
}
