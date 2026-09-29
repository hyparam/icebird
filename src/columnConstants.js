import { deserializeValue } from './write/serde.js'

/**
 * Prove a top-level scalar column constant from identity partitions or Iceberg manifest metrics.
 * Missing counts are unknown, not zero. Mixed null/non-null columns cannot
 * become constants even when their non-null lower and upper bounds agree.
 *
 * @import {DataFile, Field, PartitionSpec} from './types.js'
 * @param {DataFile} file
 * @param {Field} field
 * @param {PartitionSpec} [partitionSpec]
 * @returns {{value: string | number | bigint | boolean | null} | undefined}
 */
export function manifestColumnConstant(file, field, partitionSpec) {
  // Identity strings already use the SQL representation, even without metrics.
  const partition = partitionSpec?.fields.find(partition =>
    partition['source-id'] === field.id && partition.transform === 'identity')
  if (field.type === 'string' && partition && Object.hasOwn(file.partition, partition.name)) {
    const value = file.partition[partition.name] ?? null
    if (value === null || typeof value === 'string') return { value }
  }
  if (typeof field.type !== 'string' || file.record_count <= 0n) return undefined
  const count = metric(file.value_counts, field.id)
  const nulls = metric(file.null_value_counts, field.id)
  if (count !== file.record_count || nulls === undefined) return undefined
  if (nulls === count) return { value: null }
  if (nulls !== 0n) return undefined

  // Avoid floating-point NaN/signed-zero and logical-type conversion issues.
  // These types have the same representation in manifest bounds and vectors.
  if (!['string', 'boolean', 'int', 'long'].includes(field.type)) return undefined
  const lower = metric(file.lower_bounds, field.id)
  const upper = metric(file.upper_bounds, field.id)
  if (!(lower instanceof Uint8Array) || !(upper instanceof Uint8Array)) return undefined
  if (lower.length !== upper.length) return undefined
  for (let i = 0; i < lower.length; i++) if (lower[i] !== upper[i]) return undefined
  const size = field.type === 'boolean' ? 1 : field.type === 'int' ? 4 : field.type === 'long' ? 8 : undefined
  if (size !== undefined && lower.length !== size) return undefined
  // Truncated string bounds enclose the original values: the upper bound is
  // incremented (or omitted), so truncation alone cannot prove equality.
  const value = deserializeValue(lower, field.type)
  return value === undefined ? undefined : { value }
}

/**
 * Build a batch from proven constants, accounting for physical bounds and
 * position deletes. Since every vector is constant, compacting deleted positions
 * to a visible row count preserves values without allocating selection indices.
 *
 * @import {AsyncBatch} from 'squirreling'
 * @param {DataFile} file
 * @param {Array<ReturnType<typeof manifestColumnConstant>>} constants
 * @param {number} start
 * @param {number | undefined} end
 * @param {Set<bigint>} deletes
 * @returns {AsyncBatch | undefined}
 */
export function constantBatch(file, constants, start, end, deletes) {
  if (file.record_count < 0n || file.record_count > BigInt(Number.MAX_SAFE_INTEGER) ||
      !constants.every(constant => constant !== undefined)) return undefined
  const stop = Math.min(Number(file.record_count), end ?? Infinity)
  if (!Number.isSafeInteger(start) || start < 0 || !Number.isSafeInteger(stop) || stop < 0) return undefined
  let length = Math.max(0, stop - start)
  const first = BigInt(start)
  const last = BigInt(stop)
  for (const position of deletes) {
    if (position >= first && position < last) length--
  }
  return {
    selection: { type: 'all', length },
    columns: constants.map(constant => ({ type: 'constant', value: constant.value, length })),
  }
}

/**
 * Avro int-keyed maps decode as entries; hand-built manifests use records.
 * @param {any} map
 * @param {number} id
 * @returns {any}
 */
function metric(map, id) {
  return Array.isArray(map) ? map.find(entry => Number(entry.key) === id)?.value : map?.[id]
}
