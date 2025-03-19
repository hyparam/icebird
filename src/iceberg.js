import { asyncBufferFromUrl, cachedAsyncBuffer, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { fetchAvroRecords, fetchDataFilesFromManifests, fetchDeleteMaps, translateS3Url } from './iceberg.fetch.js'
import { fetchIcebergMetadata, fetchLatestSequenceNumber } from './iceberg.metadata.js'

export { fetchIcebergMetadata, fetchLatestSequenceNumber }

/**
 * Returns manifest URLs for the current snapshot separated into data and delete manifests.
 *
 * @import {IcebergMetadata} from './types.js'
 * @param {IcebergMetadata} metadata
 * @returns {Promise<{dataManifestUrls: string[], deleteManifestUrls: string[]}>}
 */
async function getManifestUrls(metadata) {
  const currentSnapshotId = metadata['current-snapshot-id']
  if (!currentSnapshotId || currentSnapshotId < 0) {
    throw new Error('No current snapshot id found in table metadata')
  }
  const snapshot = metadata.snapshots.find(s => s['snapshot-id'] === currentSnapshotId)
  if (!snapshot) {
    throw new Error(`Snapshot ${currentSnapshotId} not found in metadata`)
  }
  let manifestUrls = []
  if (snapshot['manifest-list']) {
    const manifestListUrl = snapshot['manifest-list']
    const records = await fetchAvroRecords(manifestListUrl)
    manifestUrls = records.map(rec => rec.manifest_path)
  } else if (snapshot.manifests) {
    manifestUrls = snapshot.manifests.map(m => m.manifest_path)
  } else {
    throw new Error('No manifest information found in snapshot')
  }
  // Separate manifest URLs into data and delete manifests.
  const dataManifestUrls = []
  const deleteManifestUrls = []
  for (const url of manifestUrls) {
    const records = await fetchAvroRecords(url)
    if (records.length === 0) continue
    const content = records[0].data_file.content || 0
    if (content === 0) {
      dataManifestUrls.push(url)
    } else if (content === 1 || content === 2) {
      deleteManifestUrls.push(url)
    }
  }
  return { dataManifestUrls, deleteManifestUrls }
}

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
 * @param {object} options
 * @param {string} options.tableUrl - Base S3 URL of the table.
 * @param {number} [options.rowStart] - The starting global row index to fetch (inclusive).
 * @param {number} [options.rowEnd] - The ending global row index to fetch (exclusive).
 * @param {string} [options.metadataFileName] - Name of the Iceberg metadata file.
 * @returns {Promise<Array<Record<string, any>>>} Array of data records.
 */
export async function icebergRead({
  tableUrl,
  rowStart = 0,
  rowEnd = Infinity,
  metadataFileName,
}) {
  // Fetch table metadata
  const metadata = await fetchIcebergMetadata(tableUrl, metadataFileName)

  // Get manifest URLs for data and delete files
  const { dataManifestUrls, deleteManifestUrls } = await getManifestUrls(metadata)
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
function sanitize(name) {
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
