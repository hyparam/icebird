import { cachedAsyncBuffer, parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { fetchDeleteMaps, urlResolver } from './fetch.js'
import { icebergMetadata } from './metadata.js'
import { icebergManifests, splitManifestEntries } from './manifest.js'
import { deleteFileAppliesToDataEntry } from './delete.js'
import { equalityMatch, sanitize } from './utils.js'

/**
 * Reads data from the Iceberg table with optional row-level delete processing.
 * Row indices are zero-based and rowEnd is exclusive.
 *
 * @import {Field, Lister, NameMapping, Resolver, Schema, TableMetadata} from '../src/types.js'
 * @param {object} options
 * @param {string} options.tableUrl - Base URL or path of the table.
 * @param {number} [options.rowStart] - The starting global row index to fetch (inclusive).
 * @param {number} [options.rowEnd] - The ending global row index to fetch (exclusive).
 * @param {string} [options.metadataFileName] - Name of the Iceberg metadata file.
 * @param {TableMetadata} [options.metadata] - Pre-fetched Iceberg metadata.
 * @param {Resolver} [options.resolver] - Resolves a path to an AsyncBuffer.
 * @param {Lister} [options.lister] - Lists files in a directory.
 * @returns {Promise<Array<Record<string, any>>>} Array of data records.
 */
export async function icebergRead({
  tableUrl,
  rowStart = 0,
  rowEnd = Infinity,
  metadataFileName,
  metadata,
  resolver,
  lister,
}) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (rowStart > rowEnd) throw new Error('rowStart must be less than rowEnd')
  if (rowStart < 0) throw new Error('rowStart must be positive')

  resolver ??= urlResolver()

  // Fetch table metadata if not provided
  metadata ??= await icebergMetadata({ tableUrl, metadataFileName, resolver, lister })
  // TODO: Handle manifests asynchronously
  const manifestList = await icebergManifests(metadata, resolver)

  // Get current schema id
  const currentSchemaId = metadata['current-schema-id']
  const schema = metadata.schemas.find(s => s['schema-id'] === currentSchemaId)
  if (!schema) throw new Error('current schema not found in metadata')
  const rowLineage = metadata['format-version'] >= 3

  // Get manifest URLs for data and delete files
  const { dataEntries, deleteEntries } = splitManifestEntries(manifestList)
  if (dataEntries.length === 0) {
    throw new Error('No data manifest files found for current snapshot')
  }
  const deleteMaps = fetchDeleteMaps(deleteEntries, resolver)

  // Determine the global row range to read
  const totalRowsToRead = rowEnd === Infinity ? Infinity : rowEnd - rowStart

  // Find the data file that contains the starting global row
  let fileIndex = 0
  let skipRows = rowStart
  while (fileIndex < dataEntries.length && skipRows >= dataEntries[fileIndex].data_file.record_count) {
    skipRows -= Number(dataEntries[fileIndex].data_file.record_count)
    fileIndex++
  }

  // Pre-compute the per-file row ranges based on record_count, so reads can run
  // in parallel. Deletes may reduce the post-filter count below totalRowsToRead.
  const fileReads = []
  let rowsRemaining = totalRowsToRead
  for (let i = fileIndex; i < dataEntries.length && rowsRemaining > 0; i++) {
    const recordCount = Number(dataEntries[i].data_file.record_count)
    const fileRowStart = i === fileIndex ? skipRows : 0
    const availableRows = recordCount - fileRowStart
    if (availableRows <= 0) continue
    const rowsToRead = rowsRemaining === Infinity ? availableRows : Math.min(rowsRemaining, availableRows)
    fileReads.push({ entry: dataEntries[i], fileRowStart, fileRowEnd: fileRowStart + rowsToRead })
    if (rowsRemaining !== Infinity) rowsRemaining -= rowsToRead
  }

  // Resolve delete maps once, shared across all parallel reads
  const { positionDeletesMap, equalityDeleteGroups } = await deleteMaps

  // Fetch data files in parallel
  const fileResults = await Promise.all(fileReads.map(({ entry, fileRowStart, fileRowEnd }) =>
    readDataFile({
      dataEntry: entry,
      fileRowStart,
      fileRowEnd,
      schema,
      metadata,
      resolver,
      rowLineage,
      positionDeletesMap,
      equalityDeleteGroups,
    })
  ))

  return fileResults.flat()
}

/**
 * Read a single data file, apply deletes, and map parquet columns to iceberg
 * field names by id.
 *
 * @import {ManifestEntry} from '../src/types.js'
 * @param {object} options
 * @param {ManifestEntry} options.dataEntry
 * @param {number} options.fileRowStart
 * @param {number} options.fileRowEnd
 * @param {Schema} options.schema
 * @param {TableMetadata} options.metadata
 * @param {Resolver} options.resolver
 * @param {boolean} options.rowLineage
 * @param {Map<string, Array<{deleteEntry: ManifestEntry, positions: Set<bigint>}>>} options.positionDeletesMap
 * @param {Array<{deleteEntry: ManifestEntry, rows: Record<string, any>[]}>} options.equalityDeleteGroups
 * @returns {Promise<Array<Record<string, any>>>}
 */
async function readDataFile({
  dataEntry,
  fileRowStart,
  fileRowEnd,
  schema,
  metadata,
  resolver,
  rowLineage,
  positionDeletesMap,
  equalityDeleteGroups,
}) {
  const { data_file, sequence_number, partition_spec_id } = dataEntry
  // assert(status !== 2)

  // Check sequence numbers
  if (sequence_number === undefined) throw new Error('sequence number not found, check v2 inheritance logic')

  // Use the spec the file was written under, not the table's current default
  // spec, since partition spec can evolve. Field names in `data_file.partition`
  // come from that historical spec.
  const partitionSpec = metadata['partition-specs'].find(s => s['spec-id'] === partition_spec_id)

  // Read the data file
  const resolved = await resolver.reader(data_file.file_path, Number(data_file.file_size_in_bytes))
  const asyncBuffer = cachedAsyncBuffer(resolved)

  // Read iceberg schema from parquet metadata
  const parquetMetadata = await parquetMetadataAsync(asyncBuffer)
  const kv = parquetMetadata.key_value_metadata?.find(k => k.key === 'iceberg.schema')
  /** @type {Schema} */
  let parquetIcebergSchema
  if (kv?.value) {
    parquetIcebergSchema = JSON.parse(kv.value)
  } else if (parquetMetadata.schema.some(s => s.field_id !== undefined)) {
    // No `iceberg.schema` kv, but the parquet schema carries field_ids
    // (iceberg-rust, iceberg-java, pyiceberg all set these). Build a
    // parquet-shaped schema so columns added later in the iceberg schema
    // correctly fall through to the initial-default / name-mapping chain
    // instead of silently looking up a name that isn't in the row.
    parquetIcebergSchema = parquetSchemaToIceberg(parquetMetadata.schema)
  } else {
    // AWS Athena tables: no kv and no field_ids. Fall back to the current
    // iceberg schema and rely on `schema.name-mapping.default` to map
    // physical column names back to ids.
    parquetIcebergSchema = schema
  }

  // Determine which columns to read based on field ids
  const parquetColumnNames = []
  for (const field of schema.fields) {
    const parquetField = parquetIcebergSchema.fields.find(f => f.id === field.id)
    // May be undefined if the field was added later
    if (parquetField && field.type !== 'unknown') {
      parquetColumnNames.push(sanitize(parquetField.name))
    } else {
      parquetColumnNames.push(undefined)
    }
  }
  const lineageColumns = rowLineage ? rowLineageColumnNames(parquetIcebergSchema) : {}
  const columns = parquetColumnNames.filter(n => n !== undefined)
  for (const column of [lineageColumns.rowId, lineageColumns.lastUpdatedSequenceNumber]) {
    if (column && !columns.includes(column)) columns.push(column)
  }
  const dataColumnNamesById = columnNamesById(parquetIcebergSchema)

  const rows = await parquetReadObjects({
    file: asyncBuffer,
    metadata: parquetMetadata,
    columns,
    rowStart: fileRowStart,
    rowEnd: fileRowEnd,
    compressors,
    // Iceberg `binary`/`fixed[N]` columns are plain BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY
    // with no UTF8/STRING annotation; hyparquet's default would silently decode
    // them as strings. Disabling its global utf8 fallback preserves bytes —
    // genuine string columns still convert because the writer always annotates
    // them with UTF8/STRING.
    utf8: false,
  })
  let rowEntries = rows.map((row, idx) => ({
    row,
    pos: BigInt(fileRowStart + idx),
  }))

  // If delete files apply to this data file, filter the rows
  const positionDeleteGroups = positionDeletesMap.get(data_file.file_path)
  if (positionDeleteGroups) {
    const positionDeletes = new Set()
    for (const group of positionDeleteGroups) {
      if (!deleteFileAppliesToDataEntry(dataEntry, group.deleteEntry, metadata, 'position')) continue
      for (const pos of group.positions) positionDeletes.add(pos)
    }
    rowEntries = rowEntries.filter(entry => !positionDeletes.has(entry.pos))
  }
  for (const group of equalityDeleteGroups) {
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
    if (!deleteFileAppliesToDataEntry(dataEntry, group.deleteEntry, metadata, 'equality')) continue
    rowEntries = rowEntries.filter(({ row }) => {
      return !group.rows.some(predicate => equalityMatch(row, predicate, dataColumnNamesById))
    })
  }

  // Map parquet column names to iceberg names by field id
  const out = []
  for (const { row, pos } of rowEntries) {
    /** @type {Record<string, any>} */
    const mapped = {}
    for (let i = 0; i < schema.fields.length; i++) {
      const field = schema.fields[i]
      const parquetColumnName = parquetColumnNames[i]
      if (parquetColumnName) {
        mapped[field.name] = row[parquetColumnName]
      } else {
        const partitionField = partitionSpec?.fields.find(pf => pf['source-id'] === field.id)

        /** @type {NameMapping | undefined} */
        let nameMapping
        if (metadata.properties?.['schema.name-mapping.default']) {
          /** @type {NameMapping[]} */
          const mapping = JSON.parse(metadata.properties['schema.name-mapping.default'])
          nameMapping = nameMappingById(mapping, field.id)
        }

        // Values for field ids which are not present in a data file must
        // be resolved according the following rules:
        if (partitionField?.transform === 'identity') {
          // 1. Return the value from partition metadata if an Identity Transform
          // exists for the field and the partition value is present in the
          // partition struct on data_file object in the manifest. This allows
          // for metadata only migrations of Hive tables.
          // The partition struct is keyed by partition-field name in Avro.
          mapped[field.name] = data_file.partition[partitionField.name]
        } else if (nameMapping) {
          // 2. Use schema.name-mapping.default metadata to map field id to columns
          for (const name of nameMapping.names) {
            const idx = parquetColumnNames.indexOf(name)
            if (idx !== -1) {
              mapped[field.name] = row[name]
              break
            }
          }
        } else if (field['initial-default'] !== undefined) {
          // 3. Return the default value if it has a defined initial-default.
          mapped[field.name] = field['initial-default']
        } else {
          // 4. Return null in all other cases.
          mapped[field.name] = null
        }
      }
    }
    if (rowLineage) {
      applyRowLineage(mapped, {
        row,
        pos,
        firstRowId: data_file.first_row_id,
        sequenceNumber: sequence_number,
        rowIdColumn: lineageColumns.rowId,
        lastUpdatedSequenceNumberColumn: lineageColumns.lastUpdatedSequenceNumber,
      })
    }
    out.push(mapped)
  }
  return out
}

/**
 * Recursively find the name mapping object that belongs to a particular field‑id.
 *
 * @param {NameMapping[]} mappings
 * @param {number} fieldId
 * @returns {NameMapping|undefined}
 */
function nameMappingById(mappings, fieldId) {
  for (const m of mappings) {
    if (m['field-id'] === fieldId) return m
    if (m.fields) {
      const hit = nameMappingById(m.fields, fieldId)
      if (hit) return hit
    }
  }
}

/**
 * @param {Schema} parquetIcebergSchema
 * @returns {{rowId?: string, lastUpdatedSequenceNumber?: string}}
 */
function rowLineageColumnNames(parquetIcebergSchema) {
  return {
    rowId: columnNameByFieldId(parquetIcebergSchema, 2147483540),
    lastUpdatedSequenceNumber: columnNameByFieldId(parquetIcebergSchema, 2147483539),
  }
}

/**
 * @param {Schema} schema
 * @param {number} fieldId
 * @returns {string|undefined}
 */
function columnNameByFieldId(schema, fieldId) {
  const field = schema.fields.find(f => f.id === fieldId)
  return field ? sanitize(field.name) : undefined
}

/**
 * Synthesize a parquet-shaped iceberg schema from the parquet schema
 * elements when the file has no `iceberg.schema` kv but does carry
 * field_ids on each column. Only top-level leaf fields are included;
 * nested types fall through with an `unknown` type marker.
 *
 * @import {SchemaElement} from 'hyparquet'
 * @param {SchemaElement[]} parquetSchema
 * @returns {Schema}
 */
function parquetSchemaToIceberg(parquetSchema) {
  /** @type {Field[]} */
  const fields = []
  for (const elem of parquetSchema) {
    if (elem.field_id === undefined) continue
    fields.push({
      id: elem.field_id,
      name: elem.name,
      required: false,
      type: 'unknown',
    })
  }
  return { type: 'struct', 'schema-id': 0, fields }
}

/**
 * @param {Schema} schema
 * @returns {Record<number, string>}
 */
function columnNamesById(schema) {
  /** @type {Record<number, string>} */
  const out = {}
  for (const field of schema.fields) {
    out[field.id] = sanitize(field.name)
  }
  return out
}

/**
 * @param {Record<string, any>} out
 * @param {object} options
 * @param {Record<string, any>} options.row
 * @param {bigint} options.pos
 * @param {bigint | number | undefined} options.firstRowId
 * @param {bigint} options.sequenceNumber
 * @param {string} [options.rowIdColumn]
 * @param {string} [options.lastUpdatedSequenceNumberColumn]
 */
function applyRowLineage(out, {
  row,
  pos,
  firstRowId,
  sequenceNumber,
  rowIdColumn,
  lastUpdatedSequenceNumberColumn,
}) {
  const storedRowId = rowIdColumn ? row[rowIdColumn] : undefined
  const storedLastUpdatedSequenceNumber = lastUpdatedSequenceNumberColumn
    ? row[lastUpdatedSequenceNumberColumn]
    : undefined
  if (storedRowId != null) {
    out._row_id = storedRowId
  } else if (firstRowId != null) {
    out._row_id = BigInt(firstRowId) + pos
  } else {
    out._row_id = null
  }

  if (storedLastUpdatedSequenceNumber != null) {
    out._last_updated_sequence_number = storedLastUpdatedSequenceNumber
  } else if (firstRowId != null) {
    out._last_updated_sequence_number = sequenceNumber
  } else {
    out._last_updated_sequence_number = null
  }
}
