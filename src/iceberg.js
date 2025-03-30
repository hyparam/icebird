import { asyncBufferFromUrl, cachedAsyncBuffer, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { fetchDeleteMaps, translateS3Url } from './iceberg.fetch.js'
import { icebergLatestVersion, icebergMetadata } from './iceberg.metadata.js'
import { icebergManifests, splitManifestEntries } from './iceberg.manifest.js'
import { equalityMatch, sanitize } from './utils.js'

export { icebergMetadata, icebergManifests, icebergLatestVersion }
export { avroMetadata } from './avro.metadata.js'
export { avroData } from './avro.data.js'

/**
 * Reads data from the Iceberg table with optional row-level delete processing.
 * Row indices are zero-based and rowEnd is exclusive.
 *
 * TODO:
 *   - Sequence number checks when filtering deletes
 *
 * @import {IcebergMetadata} from '../src/types.js'
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
  if (!tableUrl) throw new Error('tableUrl is required')
  if (rowStart > rowEnd) throw new Error('rowStart must be less than rowEnd')
  if (rowStart < 0) throw new Error('rowStart must be positive')

  // Fetch table metadata
  metadata ??= await icebergMetadata(tableUrl, metadataFileName)
  // TODO: Fetch manifests asynchronously
  const manifestList = await icebergManifests(metadata)

  // Get current schema id
  const currentSchemaId = metadata['current-schema-id']
  const schema = metadata.schemas.find(s => s['schema-id'] === currentSchemaId)
  if (!schema) throw new Error('current schema not found in metadata')

  // Get current sequence number
  const lastSequenceNumber = metadata['last-sequence-number']

  // Get manifest URLs for data and delete files
  const { dataEntries, deleteEntries } = splitManifestEntries(manifestList)
  if (dataEntries.length === 0) {
    throw new Error('No data manifest files found for current snapshot')
  }

  // Determine the global row range to read
  const totalRowsToRead = rowEnd === Infinity ? Infinity : rowEnd - rowStart

  // Find the data file that contains the starting global row
  let fileIndex = 0
  let skipRows = rowStart
  while (fileIndex < dataEntries.length && skipRows >= dataEntries[fileIndex].data_file.record_count) {
    skipRows -= Number(dataEntries[fileIndex].data_file.record_count)
    fileIndex++
  }

  // Read data files one-by-one, applying delete filters
  const results = []
  let rowsNeeded = totalRowsToRead
  // TODO: Fetch data files in parallel
  for (let i = fileIndex; i < dataEntries.length && rowsNeeded !== 0; i++) {
    const { data_file, sequence_number } = dataEntries[i]
    // assert(status !== 2)

    // Check sequence numbers
    if (sequence_number === null) throw new Error('sequence number not found, check v2 inheritance logic')

    // Determine the row range to read from this file
    const fileRowStart = i === fileIndex ? skipRows : 0
    const availableRows = Number(data_file.record_count) - fileRowStart
    const rowsToRead = rowsNeeded === Infinity ? availableRows : Math.min(rowsNeeded, availableRows)

    // Skip if there are no rows to read from this file
    if (rowsToRead <= 0) continue
    const fileRowEnd = fileRowStart + rowsToRead

    // Read the data file
    const fileUrl = translateS3Url(data_file.file_path)
    const asyncBuffer = await asyncBufferFromUrl({ url: fileUrl })
    const fileBuffer = cachedAsyncBuffer(asyncBuffer)
    let rows = await parquetReadObjects({
      file: fileBuffer,
      rowStart: fileRowStart,
      rowEnd: fileRowEnd,
      compressors,
    })

    // If delete files apply to this data file, filter the rows
    const { positionDeletesMap, equalityDeletesMap } = await fetchDeleteMaps(deleteEntries)

    const positionDeletes = positionDeletesMap.get(data_file.file_path)
    if (positionDeletes) {
      rows = rows.filter((_, idx) => !positionDeletes.has(BigInt(idx + fileRowStart)))
    }
    for (const [deleteSequenceNumber, deleteRows] of equalityDeletesMap) {
      // An equality delete file must be applied to a data file when all of the following are true:
      // - The data file's data sequence number is strictly less than the delete's data sequence number
      // - The data file's partition (both spec id and partition values) is equal to the delete file's
      //   partition or the delete file's partition spec is unpartitioned
      // In general, deletes are applied only to data files that are older and in the same partition, except for two special cases:
      // - Equality delete files stored with an unpartitioned spec are applied as global deletes.
      //   Otherwise, delete files do not apply to files in other partitions.
      // - Position deletes (vectors and files) must be applied to data files from the same commit,
      //   when the data and delete file data sequence numbers are equal.
      //   This allows deleting rows that were added in the same commit.
      if (deleteSequenceNumber > lastSequenceNumber) continue // Skip future deletes
      if (deleteSequenceNumber <= sequence_number) continue // Skip deletes that are too old
      rows = rows.filter(row => !deleteRows.some(predicate => equalityMatch(row, predicate)))
    }

    // Map column names to unsanitized names
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
