import { asyncBufferFromUrl, cachedAsyncBuffer, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { fetchDeleteMaps, translateS3Url } from './iceberg.fetch.js'
import { icebergLatestVersion, icebergMetadata } from './iceberg.metadata.js'
import { fetchDataFilesFromManifests, getDataUrls, icebergManifests } from './iceberg.manifest.js'

export { icebergMetadata, icebergManifests, icebergLatestVersion }
export { avroMetadata } from './avro.metadata.js'
export { avroData } from './avro.data.js'

/**
 * Helper to check if a row matches an equality delete predicate.
 * For simplicity, compares all fields (except file_path and pos) by strict equality.
 *
 * @param {any} row - Data row from the data file.
 * @param {any} deletePredicate - A delete row from an equality delete file.
 * @returns {boolean} True if row matches the predicate.
 */
function equalityMatch(row, deletePredicate) {
  for (const key in deletePredicate) {
    if (key === 'file_path' || key === 'pos') continue
    if (row[key] !== deletePredicate[key]) return false
  }
  return true
}

/**
 * Reads data from the Iceberg table with optional row-level delete processing.
 * Row indices are zero-based and rowEnd is inclusive.
 *
 * This function:
 *   1. Loads metadata and verifies format-version and table-uuid.
 *   2. Separates manifest URLs into data and delete manifests.
 *   3. Reads delete files from delete manifests and groups them by target data file.
 *   4. When reading each data file, applies position and equality deletes.
 *
 * TODO:
 *   - Sequence number checks when filtering deletes
 *
 * @import {IcebergMetadata} from './types.js'
 * @param {object} options
 * @param {string} options.tableUrl - Base S3 URL of the table.
 * @param {number} [options.rowStart] - The starting global row index to fetch (inclusive).
 * @param {number} [options.rowEnd] - The ending global row index to fetch (exclusive).
 * @param {string} [options.metadataFileName] - Name of the Iceberg metadata file.
 * @param {IcebergMetadata} [options.metadata] - Pre-fetched Iceberg metadata.
 * @returns {Promise<Array<Record<string, any>>>} Array of data records.
 */
export async function icebergRead({
  tableUrl,
  rowStart = 0,
  rowEnd = Infinity,
  metadataFileName,
  metadata,
}) {
  // Fetch table metadata
  metadata ??= await icebergMetadata(tableUrl, metadataFileName)
  const manifests = await icebergManifests(metadata)

  // Get manifest URLs for data and delete files
  const { dataManifestUrls, deleteManifestUrls } = getDataUrls(manifests)
  if (dataManifestUrls.length === 0) {
    throw new Error('No data manifest files found for current snapshot')
  }

  // Build maps of delete entries keyed by target data file path (async)
  const deleteMaps = fetchDeleteMaps(deleteManifestUrls)

  // Read data file info from data manifests
  const dataFiles = await fetchDataFilesFromManifests(dataManifestUrls)
  if (dataFiles.length === 0) {
    throw new Error('No data files found in manifests (table may be empty)')
  }

  // Determine the global row range to read
  const totalRowsToRead = rowEnd === Infinity ? Infinity : rowEnd - rowStart

  // Find the data file that contains the starting global row
  let fileIndex = 0
  let skipRows = rowStart
  while (fileIndex < dataFiles.length && skipRows >= dataFiles[fileIndex].record_count) {
    skipRows -= Number(dataFiles[fileIndex].record_count)
    fileIndex++
  }

  // Read data files one-by-one, applying delete filters
  const results = []
  let rowsNeeded = totalRowsToRead
  for (let i = fileIndex; i < dataFiles.length && rowsNeeded !== 0; i++) {
    const fileInfo = dataFiles[i]
    // Determine the row range to read from this file
    const fileRowStart = i === fileIndex ? skipRows : 0
    const availableRows = Number(fileInfo.record_count) - fileRowStart
    const rowsToRead = rowsNeeded === Infinity ? availableRows : Math.min(rowsNeeded, availableRows)

    // Skip if there are no rows to read from this file
    if (rowsToRead <= 0) continue
    const fileRowEnd = fileRowStart + rowsToRead

    // Read the data file
    const fileUrl = translateS3Url(fileInfo.file_path)
    const asyncBuffer = await asyncBufferFromUrl({ url: fileUrl })
    const fileBuffer = cachedAsyncBuffer(asyncBuffer)
    let rows = await parquetReadObjects({
      file: fileBuffer,
      rowStart: fileRowStart,
      rowEnd: fileRowEnd,
      compressors,
    })

    // If delete files apply to this data file, filter the rows
    const { positionDeletesMap, equalityDeletesMap } = await deleteMaps
    const positionDeletes = positionDeletesMap.get(fileInfo.file_path)
    if (positionDeletes) {
      rows = rows.filter((_, idx) => !positionDeletes.has(BigInt(idx + fileRowStart)))
    }
    const equalityDeletes = equalityDeletesMap.get(fileInfo.file_path)
    if (equalityDeletes) {
      rows = rows.filter(row => !equalityDeletes.some(predicate => equalityMatch(row, predicate)))
    }

    // Map column names to unsanitized names
    const schema = metadata.schemas[metadata.schemas.length - 1]
    const unsanitizedFields = schema.fields.map(f => f.name)
      .filter(column => column !== sanitize(column))
    const sanitizedFields = unsanitizedFields.map(sanitize)
    for (const row of rows) {
      for (let i = 0; i < unsanitizedFields.length; i++) {
        row[unsanitizedFields[i]] = row[sanitizedFields[i]]
        delete row[sanitizedFields[i]]
      }
      results.push(row)
    }

    if (rowsNeeded !== Infinity) {
      rowsNeeded -= rows.length
      if (rowsNeeded <= 0) break
    }
  }

  return results
}

/**
 * Avro sanitization function.
 *
 * @param {string} name
 * @returns {string}
 */
export function sanitize(name) {
  let result = ''
  for (let i = 0; i < name.length; i++) {
    const ch = name.charAt(i)
    const isLetter = /^[A-Za-z]$/.test(ch)
    const isDigit = /^[0-9]$/.test(ch)
    if (i === 0) {
      if (isLetter || ch === '_') {
        result += ch
      } else {
        result += isDigit ? '_' + ch : '_x' + ch.charCodeAt(0).toString(16).toUpperCase()
      }
    } else {
      if (isLetter || isDigit || ch === '_') {
        result += ch
      } else {
        result += '_x' + ch.charCodeAt(0).toString(16).toUpperCase()
      }
    }
  }
  return result
}
