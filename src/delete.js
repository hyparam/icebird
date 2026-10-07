import { valuesEqual } from './utils.js'

/**
 * @import {ManifestEntry, PartitionSpec, TableMetadata} from '../src/types.js'
 */

/**
 * Check whether a delete file applies to a data file according to Iceberg scan
 * planning rules. Position deletes can apply when sequence numbers are equal;
 * equality deletes only apply to older data files.
 *
 * @param {ManifestEntry} dataEntry
 * @param {ManifestEntry} deleteEntry
 * @param {TableMetadata} metadata
 * @param {'position'|'equality'} deleteType
 * @returns {boolean}
 */
export function deleteFileAppliesToDataEntry(dataEntry, deleteEntry, metadata, deleteType) {
  const dataSequenceNumber = dataEntry.sequence_number
  const deleteSequenceNumber = deleteEntry.sequence_number
  if (dataSequenceNumber === undefined) throw new Error('data file missing sequence number')
  if (deleteSequenceNumber === undefined) throw new Error('delete file missing sequence number')

  if (deleteType === 'equality') {
    if (deleteSequenceNumber <= dataSequenceNumber) return false
    if (isUnpartitioned(metadata, deleteEntry.partition_spec_id)) return true
  } else if (deleteSequenceNumber < dataSequenceNumber) {
    return false
  }

  return samePartition(dataEntry, deleteEntry, metadata)
}

/**
 * Whether a position delete entry is a deletion vector (puffin blob) rather
 * than a position delete file.
 *
 * @param {ManifestEntry} deleteEntry
 * @returns {boolean}
 */
export function isDeletionVector(deleteEntry) {
  const dataFile = deleteEntry.data_file
  return dataFile.file_format.toLowerCase() === 'puffin' ||
    dataFile.content_offset != null ||
    dataFile.content_size_in_bytes != null
}

/**
 * Collect the row positions deleted from a data file by applicable position
 * deletes. When a deletion vector applies to the data file, position delete
 * files are ignored: the spec requires a newly added vector to contain all
 * deletes from existing position delete files, so the vector replaces them.
 *
 * @param {ManifestEntry} dataEntry
 * @param {Array<{deleteEntry: ManifestEntry, positions: Set<bigint>}> | undefined} positionDeleteGroups
 * @param {TableMetadata} metadata
 * @returns {Set<bigint>}
 */
export function applicablePositionDeletes(dataEntry, positionDeleteGroups, metadata) {
  /** @type {Set<bigint>} */
  const positions = new Set()
  if (!positionDeleteGroups) return positions
  const applicable = positionDeleteGroups.filter(group =>
    deleteFileAppliesToDataEntry(dataEntry, group.deleteEntry, metadata, 'position'))
  const vectors = applicable.filter(group => isDeletionVector(group.deleteEntry))
  for (const group of vectors.length ? vectors : applicable) {
    for (const pos of group.positions) positions.add(pos)
  }
  return positions
}

/**
 * @param {TableMetadata} metadata
 * @param {number|undefined} specId
 * @returns {boolean}
 */
function isUnpartitioned(metadata, specId) {
  const spec = metadata['partition-specs'].find(s => s['spec-id'] === specId)
  return spec?.fields.length === 0
}

/**
 * @param {ManifestEntry} dataEntry
 * @param {ManifestEntry} deleteEntry
 * @param {TableMetadata} metadata
 * @returns {boolean}
 */
function samePartition(dataEntry, deleteEntry, metadata) {
  if (dataEntry.partition_spec_id !== deleteEntry.partition_spec_id) return false
  const spec = metadata['partition-specs'].find(s => s['spec-id'] === dataEntry.partition_spec_id)
  return partitionsEqual(dataEntry.data_file.partition, deleteEntry.data_file.partition, spec)
}

/**
 * @param {Record<string, unknown>} a
 * @param {Record<string, unknown>} b
 * @param {PartitionSpec | undefined} spec
 * @returns {boolean}
 */
function partitionsEqual(a, b, spec) {
  const aKeys = Object.keys(a)
  const bKeys = Object.keys(b)
  if (aKeys.length !== bKeys.length) return false
  for (const key of aKeys) {
    if (!Object.hasOwn(b, key)) return false
    const transform = spec?.fields.find(f => f.name === key)?.transform
    if (!partitionValuesEqual(a[key], b[key], transform)) return false
  }
  return true
}

/**
 * Partition equality follows Iceberg's field-summary rules for floating
 * values: NaNs compare equal after canonicalization, but -0.0 and +0.0 remain
 * distinct. Integer partitions can be numbers in older manifests and bigints
 * after int-to-long promotion, so compare those without losing precision.
 * Day transforms may decode as Avro Dates or integer day ordinals. Normalize
 * both to ordinals, without truncating Dates in identity timestamp partitions.
 *
 * @param {unknown} a
 * @param {unknown} b
 * @param {string | undefined} transform
 * @returns {boolean}
 */
function partitionValuesEqual(a, b, transform) {
  if (transform === 'day') {
    if (a instanceof Date) a = Math.floor(a.getTime() / 86400000)
    if (b instanceof Date) b = Math.floor(b.getTime() / 86400000)
  }
  if (typeof a === 'number' && typeof b === 'number') return Object.is(a, b)
  if (typeof a === 'number' && typeof b === 'bigint') return Number.isSafeInteger(a) && BigInt(a) === b
  if (typeof a === 'bigint' && typeof b === 'number') return Number.isSafeInteger(b) && a === BigInt(b)
  return valuesEqual(a, b)
}
