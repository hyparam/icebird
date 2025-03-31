import { asyncBufferFromUrl, cachedAsyncBuffer, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { avroData } from './avro.data.js'
import { avroMetadata } from './avro.metadata.js'

/**
 * Translates an S3A URL to an HTTPS URL for direct access to the object.
 *
 * @param {string} url
 * @returns {string}
 */
export function translateS3Url(url) {
  if (url.startsWith('s3a://') || url.startsWith('s3://')) {
    const rest = url.slice(url.indexOf('://') + 3)
    const slashIndex = rest.indexOf('/')
    if (slashIndex === -1) {
      throw new Error('Invalid S3 URL, missing "/" after bucket')
    }
    const bucket = rest.slice(0, slashIndex)
    const key = rest.slice(slashIndex)
    return `https://${bucket}.s3.amazonaws.com${key}`
  }
  return url
}

/**
 * Reads delete files from delete manifests.
 * Position deletes are grouped by target data file.
 * Equality deletes are grouped by sequence number.
 *
 * @import {ManifestEntry} from '../src/types.js'
 * @param {ManifestEntry[]} deleteEntries
 * @param {RequestInit} [requestInit]
 * @returns {Promise<{positionDeletesMap: Map<string, Set<bigint>>, equalityDeletesMap: Map<bigint, Record<string, any>[]>}>}
 */
export async function fetchDeleteMaps(deleteEntries, requestInit) {
  // Build maps of delete entries keyed by target data file path
  /** @type {Map<string, Set<bigint>>} */
  const positionDeletesMap = new Map()
  /** @type {Map<bigint, Record<string, any>[]>} */
  const equalityDeletesMap = new Map()

  // Fetch delete files in parallel
  await Promise.all(deleteEntries.map(async deleteEntry => {
    const { content, file_path, file_size_in_bytes } = deleteEntry.data_file
    const file = await asyncBufferFromUrl({
      url: translateS3Url(file_path),
      byteLength: Number(file_size_in_bytes),
      requestInit,
    }).then(cachedAsyncBuffer)
    const deleteRows = await parquetReadObjects({ file, compressors })
    for (const deleteRow of deleteRows) {
      if (content === 1) { // Position delete
        const { file_path, pos } = deleteRow
        if (!file_path) throw new Error('position delete missing target file path')
        if (pos === undefined) throw new Error('position delete missing pos')
        if (pos !== undefined && pos !== null) {
          // Note: pos is relative to the data file's row order
          let set = positionDeletesMap.get(file_path)
          if (!set) {
            set = new Set()
            positionDeletesMap.set(file_path, set)
          }
          set.add(pos)
        }
      } else if (content === 2) { // Equality delete
        // Save the entire delete row (restrict this to equalityIds?)
        const { sequence_number } = deleteEntry
        let list = equalityDeletesMap.get(sequence_number)
        if (!list) {
          list = []
          equalityDeletesMap.set(sequence_number, list)
        }
        list.push(deleteRow)
      }
    }
  }))

  return { positionDeletesMap, equalityDeletesMap }
}

/**
 * Decodes Avro records from a url.
 *
 * @param {string} manifestUrl - The URL of the manifest file
 * @param {RequestInit} [requestInit] - Optional fetch request initialization
 * @returns {Promise<Record<string, any>[]>} The decoded Avro records
 */
export async function fetchAvroRecords(manifestUrl, requestInit) {
  const safeUrl = translateS3Url(manifestUrl)
  const buffer = await fetch(safeUrl, requestInit).then(res => res.arrayBuffer())
  const reader = { view: new DataView(buffer), offset: 0 }
  const { metadata, syncMarker } = await avroMetadata(reader)
  return await avroData({ reader, metadata, syncMarker })
}
