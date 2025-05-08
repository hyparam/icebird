import { translateS3Url } from './fetch.js'
import { uuid4 } from './utils.js'

/**
 * @param {object} options
 * @param {string} options.tableUrl - Base S3 URL of the table.
 * @param {(file: string) => Writer} options.writerFactory - Function to create writers for files in storage.
 * @param {Schema} [options.schema] - The schema of the table.
 * @returns {Promise<TableMetadata>} The Iceberg table metadata as a JSON object.
 */
export async function icebergCreate ({
  tableUrl,
  writerFactory,
  schema,
}) {
  if (!tableUrl) throw new Error('tableUrl is required')
  const metadataVersion = 1
  const metadataUrl = translateS3Url(`${tableUrl}/metadata/v${metadataVersion}.metadata.json`)

  /** @type {Schema} */
  const initialSchema = schema ?? { type: 'struct', 'schema-id': 0, fields: [] } // default to no columns

  /** @type {PartitionSpec} */
  const initialPartitionSpec = { 'spec-id': 0, fields: [] }

  /** @type {SortOrder} */
  const initialSortOrder = { 'order-id': 0, fields: [] }

  /** @type {TableMetadata} */
  const metadata = {
    'format-version': 2,
    'table-uuid': uuid4(),
    location: tableUrl,
    'last-sequence-number': 0,
    'last-updated-ms': Date.now(),
    'last-column-id': maxFieldId(initialSchema.fields),
    'current-schema-id': 0,
    schemas: [initialSchema],
    'default-spec-id': 0,
    'partition-specs': [initialPartitionSpec],
    'last-partition-id': maxPartitionFieldId(initialPartitionSpec.fields),
    // properties: { 'write.parquet.compression-codec': 'snappy' },
    // 'current-snapshot-id': 0,
    'sort-orders': [initialSortOrder],
    'default-sort-order-id': 0,
    // statistics: [],
  }

  // write initial metadata
  const metadataWriter = writerFactory(metadataUrl)
  const metadataBytes = new TextEncoder().encode(JSON.stringify(metadata, null, 2))
  metadataWriter.appendBytes(metadataBytes)
  metadataWriter.finish()

  // write version-hint.text
  const versionHintUrl = translateS3Url(`${tableUrl}/version-hint.text`)
  const versionHintWriter = writerFactory(versionHintUrl)
  const versionHintBytes = new TextEncoder().encode(String(metadataVersion))
  versionHintWriter.appendBytes(versionHintBytes)
  versionHintWriter.finish()

  return metadata
}

/**
 * @import {Writer} from 'hyparquet-writer/src/types.js'
 * @import {Field, PartitionField, PartitionSpec, Schema, SortOrder, TableMetadata} from '../src/types.js'
 * @param {Field[]} fields
 * @returns {number}
 */
function maxFieldId (fields = []) {
  let max = 0
  for (const f of fields) {
    if (max < f.id) max = f.id
  }
  return max
}

/**
 * @param {PartitionField[]} partitionFields
 * @returns {number}
 */
function maxPartitionFieldId (partitionFields = []) {
  let max = 0
  for (const pf of partitionFields) {
    if (max < pf['field-id']) max = pf['field-id']
  }
  return max
}
