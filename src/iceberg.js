import { asyncBufferFromUrl, parquetReadObjects } from 'hyparquet'
import { decompress as ZSTD } from 'fzstd'
import { fetchDataFilesFromManifests, fetchIcebergMetadata, translateS3Url } from './iceberg.fetch.js'
import { decodeAvroRecords } from './avro.js'

/**
 * Returns a list of manifest file URLs for the current snapshot from the table metadata
 *
 * @import {IcebergMetadata} from './types.d.ts'
 * @param {IcebergMetadata} metadata - The Iceberg table metadata
 * @returns {Promise<Array<string>>} List of manifest file URLs
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
  if (snapshot['manifest-list']) {
    const manifestListUrl = snapshot['manifest-list']
    const records = await decodeAvroRecords(manifestListUrl)
    return records.map(rec => rec.manifest_path)
  } else if (snapshot.manifests) {
    return snapshot.manifests.map(m => m.manifest_path)
  } else {
    throw new Error('No manifest information found in snapshot')
  }
}

/**
 * Reads data from the Iceberg table with the specified global row start and end limits.
 * Row indices are zero-based and rowEnd is inclusive.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base S3 URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {number} [options.rowStart] - The starting row to fetch (inclusive)
 * @param {number} [options.rowEnd] - The ending row to fetch (exclusive)
 * @param {string} [options.metadataFileName='v1.metadata.json'] - Name of the iceberg metadata JSON file
 * @returns {Promise<Array<Record<string, any>>>} Array of data records
 */
export async function readIcebergData({ tableUrl, rowStart, rowEnd, metadataFileName = 'v1.metadata.json' }) {
  // Fetch table metadata from the specified location
  const metadata = await fetchIcebergMetadata(tableUrl, metadataFileName)

  // Get URLs of manifest files for the current snapshot
  const manifestUrls = await getManifestUrls(metadata)
  if (manifestUrls.length === 0) {
    throw new Error('No manifest files found for current snapshot')
  }

  // Fetch data file information from all manifest files
  const dataFiles = await fetchDataFilesFromManifests(manifestUrls)
  if (dataFiles.length === 0) {
    throw new Error('No data files found in manifests (table may be empty)')
  }

  // Calculate effective row range and total rows to read
  const start = rowStart ?? 0
  const end = rowEnd ?? Infinity
  const totalRowsToRead = end === Infinity ? Infinity : end - start + 1

  // Find the first data file that contains the starting row
  let fileIndex = 0
  let skipRows = start
  while (fileIndex < dataFiles.length && skipRows >= dataFiles[fileIndex].record_count) {
    skipRows -= dataFiles[fileIndex].record_count
    fileIndex++
  }

  // Collect results by reading rows from data files
  const results = []
  let rowsNeeded = totalRowsToRead
  for (let i = fileIndex; i < dataFiles.length && rowsNeeded !== 0; i++) {
    const fileInfo = dataFiles[i]

    // Calculate file-specific row range to read
    // For the first file, we might need to skip initial rows
    const fileRowStart = i === fileIndex ? skipRows : 0
    const availableRows = fileInfo.record_count - fileRowStart
    const rowsToRead = rowsNeeded === Infinity ? availableRows : Math.min(rowsNeeded, availableRows)

    // Skip if there are no rows to read from this file
    if (rowsToRead <= 0) {
      continue
    }

    // Calculate the ending row in this file
    const fileRowEnd = fileRowStart + rowsToRead - 1

    // Read the data file
    const fileBuffer = await asyncBufferFromUrl({ url: translateS3Url(fileInfo.file_path) })
    const rows = await parquetReadObjects({
      file: fileBuffer,
      rowStart: fileRowStart,
      rowEnd: fileRowEnd,
      // Iceberg specifies ZSTD compression for Parquet files
      compressors: {
        ZSTD: input => ZSTD(input),
      },
    })
    results.push(...rows)

    // Update remaining rows needed and break if we've read all requested rows
    if (rowsNeeded !== Infinity) {
      rowsNeeded -= rowsToRead
      if (rowsNeeded <= 0) break
    }
  }

  return results
}
