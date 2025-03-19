import { asyncBufferFromUrl, cachedAsyncBuffer, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { avroData } from './avro.data.js'
import { avroMetadata } from './avro.metadata.js'
import { fetchDataFilesFromManifests } from './iceberg.manifest.js'

/**
 * Translates an S3A URL to an HTTPS URL for direct access to the object.
 *
 * @param {string} url
 * @returns {string}
 */
export function translateS3Url(url) {
  if (url.startsWith('s3a://')) {
    const rest = url.slice('s3a://'.length)
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
 * Reads delete files from delete manifests and groups them by target data file.
 *
 * @import {FilePositionDelete} from './types.js'
 * @param {string[]} deleteManifestUrls
 * @returns {Promise<{positionDeletesMap: Map<string, Set<bigint>>, equalityDeletesMap: Map<string, FilePositionDelete[]>}>}
 */
export async function fetchDeleteMaps(deleteManifestUrls) {
  // Read delete file info from delete manifests
  const deleteFiles = await fetchDataFilesFromManifests(deleteManifestUrls)

  // Build maps of delete entries keyed by target data file path
  /** @type {Map<string, Set<bigint>>} */
  const positionDeletesMap = new Map()
  /** @type {Map<string, FilePositionDelete[]>} */
  const equalityDeletesMap = new Map()

  // Fetch delete files in parallel
  await Promise.all(deleteFiles.map(async deleteFile => {
    const asyncBuffer = await asyncBufferFromUrl({ url: translateS3Url(deleteFile.file_path) })
    const file = cachedAsyncBuffer(asyncBuffer)
    const deleteRows = /** @type {FilePositionDelete[]} */ (await parquetReadObjects({ file, compressors }))
    for (const deleteRow of deleteRows) {
      const targetFile = deleteRow.file_path
      if (!targetFile) continue
      if (deleteFile.content === 1) { // Position delete
        const { pos } = deleteRow
        if (pos !== undefined && pos !== null) {
          // Note: pos is relative to the data file's row order
          let set = positionDeletesMap.get(targetFile)
          if (!set) {
            set = new Set()
            positionDeletesMap.set(targetFile, set)
          }
          set.add(pos)
        }
      } else if (deleteFile.content === 2) { // Equality delete
        // Save the entire delete row (restrict this to equalityIds?)
        let list = equalityDeletesMap.get(targetFile)
        if (!list) {
          list = []
          equalityDeletesMap.set(targetFile, list)
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
 * @returns {Promise<Record<string, any>[]>} The decoded Avro records
 */
export async function fetchAvroRecords(manifestUrl) {
  const safeUrl = translateS3Url(manifestUrl)
  const buffer = await fetch(safeUrl).then(res => res.arrayBuffer())
  const reader = { view: new DataView(buffer), offset: 0 }
  const { metadata, syncMarker } = await avroMetadata(reader)
  return await avroData({ reader, metadata, syncMarker })
}
