import { asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
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
 * @import {IcebergMetadata, Schema} from '../src/types.js'
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

  // Fetch table metadata if not provided
  metadata ??= await icebergMetadata(tableUrl, metadataFileName)
  // TODO: Handle manifests asynchronously
  const manifestList = await icebergManifests(metadata)

  if (metadata['format-version'] > 2) throw new Error('iceberg format version > 2 not supported')

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
  const deleteMaps = fetchDeleteMaps(deleteEntries)

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
    // TODO: This would be faster if we could rely on the file size in the manifest
    // But it doesn't always match the actual file size
    // const byteLength = Number(data_file.file_size_in_bytes)
    const asyncBuffer = await asyncBufferFromUrl({ url: fileUrl })
    const fileBuffer = cachedAsyncBuffer(asyncBuffer)

    // Read iceberg schema from parquet metadata
    const parquetMetadata = await parquetMetadataAsync(fileBuffer)
    const kv = parquetMetadata.key_value_metadata?.find(k => k.key === 'iceberg.schema')
    if (!kv?.value) throw new Error('iceberg.schema not found in parquet metadata')
    /** @type {Schema} */
    const parquetIcebergSchema = JSON.parse(kv.value)

    // TODO: Tables may also define a property schema.name-mapping.default with
    // a JSON name mapping containing a list of field mapping objects. These
    // mappings provide fallback field ids to be used when a data file does not
    // contain field id information.

    // Determine which columns to read based on field ids
    const parquetColumnNames = []
    for (const field of schema.fields) {
      const parquetField = parquetIcebergSchema.fields.find(f => f.id === field.id)
      // May be undefined if the field was added later
      if (parquetField) {
        parquetColumnNames.push(sanitize(parquetField.name))
      } else {
        parquetColumnNames.push(undefined)
      }
    }

    let rows = await parquetReadObjects({
      file: fileBuffer,
      metadata: parquetMetadata,
      columns: parquetColumnNames.filter(n => n !== undefined),
      rowStart: fileRowStart,
      rowEnd: fileRowEnd,
      compressors,
    })

    // If delete files apply to this data file, filter the rows
    const { positionDeletesMap, equalityDeletesMap } = await deleteMaps

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

    // Map parquet column names to iceberg names by field id
    for (const row of rows) {
      /** @type {Record<string, any>} */
      const out = {}
      for (let i = 0; i < schema.fields.length; i++) {
        const parquetColumnName = parquetColumnNames[i]
        if (parquetColumnName) {
          out[schema.fields[i].name] = row[parquetColumnName]
        } else {
          // TODO: Values for field ids which are not present in a data file must
          // be resolved according the following rules:
          // - Return the value from partition metadata if an Identity Transform
          //   exists for the field and the partition value is present in the
          //   partition struct on data_file object in the manifest. This allows
          //   for metadata only migrations of Hive tables.
          // - Use schema.name-mapping.default metadata to map field id to columns
          //   without field id as described below and use the column if it is present.
          // - Return the default value if it has a defined initial-default.
          // - Return null in all other cases.
        }
      }
      results.push(out)
    }

    if (rowsNeeded !== Infinity) {
      rowsNeeded -= rows.length
      if (rowsNeeded <= 0) break
    }
  }

  return results
}
