import { valuesEqual } from './utils.js'

/**
 * @import {ManifestEntry, TableMetadata} from '../src/types.js'
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

  return samePartition(dataEntry, deleteEntry)
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
 * @returns {boolean}
 */
function samePartition(dataEntry, deleteEntry) {
  if (dataEntry.partition_spec_id !== deleteEntry.partition_spec_id) return false
  return partitionsEqual(dataEntry.data_file.partition, deleteEntry.data_file.partition)
}

/**
 * @param {Record<string, unknown>} a
 * @param {Record<string, unknown>} b
 * @returns {boolean}
 */
function partitionsEqual(a, b) {
  const aKeys = Object.keys(a)
  const bKeys = Object.keys(b)
  if (aKeys.length !== bKeys.length) return false
  for (const key of aKeys) {
    if (!Object.hasOwn(b, key)) return false
    if (!partitionValuesEqual(a[key], b[key])) return false
  }
  return true
}

/**
 * Partition equality follows Iceberg's field-summary rules for floating
 * values: NaNs compare equal after canonicalization, but -0.0 and +0.0 remain
 * distinct.
 *
 * @param {unknown} a
 * @param {unknown} b
 * @returns {boolean}
 */
function partitionValuesEqual(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return Object.is(a, b)
  return valuesEqual(a, b)
}
