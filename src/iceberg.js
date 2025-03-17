import { asyncBufferFromUrl, parquetReadObjects } from 'hyparquet'
import { decompress as ZSTD } from 'fzstd'
import {
  fetchAvroRecords, fetchDataFilesFromManifests, fetchIcebergMetadata, fetchSnapshotVersion, translateS3Url,
} from './iceberg.fetch.js'

/**
 * Returns manifest URLs for the current snapshot separated into data and delete manifests.
 *
 * @import {IcebergMetadata} from './types.d.ts'
 * @param {IcebergMetadata} metadata - The Iceberg table metadata.
 * @returns {Promise<{dataManifestUrls: string[], deleteManifestUrls: string[]}>}
 */
async function getManifestUrls(metadata) {
  const currentSnapshotId = metadata['current-snapshot-id']
  if (!currentSnapshotId) {
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
 * Reads an entire parquet file its URL.
 *
 * @param {string} url - The URL of the delete file.
 * @returns {Promise<Record<string, any>[]>} Array of delete rows.
 */
async function readParquetFile(url) {
  const buffer = await asyncBufferFromUrl({ url: translateS3Url(url) })
  const rows = await parquetReadObjects({
    file: buffer,
    // Iceberg uses ZSTD compression for parquet files.
    compressors: {
      ZSTD: input => ZSTD(input),
    },
  })
  return rows
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
 * @param {object} options
 * @param {string} options.tableUrl - Base S3 URL of the table.
 * @param {number} [options.rowStart] - The starting global row index to fetch (inclusive).
 * @param {number} [options.rowEnd] - The ending global row index to fetch (exclusive).
 * @param {string} [options.metadataFileName] - Name of the Iceberg metadata file.
 * @returns {Promise<Array<Record<string, any>>>} Array of data records.
 */
export async function readIcebergData({ tableUrl, rowStart, rowEnd, metadataFileName }) {
  // Find the latest snapshot version.
  if (!metadataFileName) {
    const version = await fetchSnapshotVersion(tableUrl)
    metadataFileName = `v${version}.metadata.json`
  }
  // Fetch table metadata and validate key fields.
  const metadata = await fetchIcebergMetadata(tableUrl, metadataFileName)

  // Get manifest URLs for data and delete files.
  const { dataManifestUrls, deleteManifestUrls } = await getManifestUrls(metadata)
  if (dataManifestUrls.length === 0) {
    throw new Error('No data manifest files found for current snapshot')
  }

  // Read data file info from data manifests.
  const dataFiles = await fetchDataFilesFromManifests(dataManifestUrls)
  if (dataFiles.length === 0) {
    throw new Error('No data files found in manifests (table may be empty)')
  }

  // Read delete file info from delete manifests (if any).
  const deleteFiles = deleteManifestUrls.length > 0
    ? await fetchDataFilesFromManifests(deleteManifestUrls)
    : []

  // Build a map of delete entries keyed by target data file path.
  // Each entry contains a Set of positions to delete (for position deletes)
  // and an array of predicates (for equality deletes).
  /** @type {Record<string, { positionDeletes: Set<bigint>, equalityDeletes: any[] }>} */
  const deleteMap = {}
  for (const deleteFile of deleteFiles) {
    const deleteRows = await readParquetFile(deleteFile.file_path)
    for (const deleteRow of deleteRows) {
      const targetFile = deleteRow.file_path
      if (!targetFile) continue
      if (!deleteMap[targetFile]) {
        deleteMap[targetFile] = { positionDeletes: new Set(), equalityDeletes: [] }
      }
      if (deleteFile.content === 1) { // Position delete
        const { pos } = deleteRow
        if (pos !== undefined && pos !== null) {
          // Note: pos is relative to the data file's row order.
          deleteMap[targetFile].positionDeletes.add(pos)
        }
      } else if (deleteFile.content === 2) { // Equality delete
        // Save the entire delete row (you might want to restrict this to equalityIds)
        deleteMap[targetFile].equalityDeletes.push(deleteRow)
      }
    }
  }

  // Determine the global row range to read.
  const start = rowStart ?? 0
  const end = rowEnd ?? Infinity
  const totalRowsToRead = end === Infinity ? Infinity : end - start + 1

  // Find the data file that contains the starting global row.
  let fileIndex = 0
  let skipRows = start
  while (fileIndex < dataFiles.length && skipRows >= dataFiles[fileIndex].record_count) {
    skipRows -= Number(dataFiles[fileIndex].record_count)
    fileIndex++
  }

  // Read data files one-by-one, applying delete filters.
  const results = []
  let rowsNeeded = totalRowsToRead
  for (let i = fileIndex; i < dataFiles.length && rowsNeeded !== 0; i++) {
    const fileInfo = dataFiles[i]
    // Determine the row range to read from this file.
    const fileRowStart = i === fileIndex ? skipRows : 0
    const availableRows = Number(fileInfo.record_count) - fileRowStart
    const rowsToRead = rowsNeeded === Infinity ? availableRows : Math.min(rowsNeeded, availableRows)

    // Skip if there are no rows to read from this file
    if (rowsToRead <= 0) continue
    const fileRowEnd = fileRowStart + rowsToRead - 1

    // Read the data file
    const fileBuffer = await asyncBufferFromUrl({ url: translateS3Url(fileInfo.file_path) })
    let rows = await parquetReadObjects({
      file: fileBuffer,
      rowStart: fileRowStart,
      rowEnd: fileRowEnd,
      compressors: {
        ZSTD: input => ZSTD(input),
      },
    })

    // If delete files apply to this data file, filter the rows.
    const deletesForFile = deleteMap[fileInfo.file_path]
    if (deletesForFile) {
      // For position deletes, remove rows whose physical row index is in the set.
      if (deletesForFile.positionDeletes && deletesForFile.positionDeletes.size > 0) {
        rows = rows.filter((row, idx) => !deletesForFile.positionDeletes.has(BigInt(idx + fileRowStart)))
      }
      // For equality deletes, filter out rows matching any delete predicate.
      if (deletesForFile.equalityDeletes && deletesForFile.equalityDeletes.length > 0) {
        for (const predicate of deletesForFile.equalityDeletes) {
          rows = rows.filter(row => !equalityMatch(row, predicate))
        }
      }
    }

    results.push(...rows)
    if (rowsNeeded !== Infinity) {
      rowsNeeded -= rows.length
      if (rowsNeeded <= 0) break
    }
  }

  return results
}
