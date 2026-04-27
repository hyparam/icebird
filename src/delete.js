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
    if (!valuesEqual(a[key], b[key])) return false
  }
  return true
}
