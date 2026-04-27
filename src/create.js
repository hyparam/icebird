import { translateS3Url } from './fetch.js'
import { uuid4 } from './utils.js'

/**
 * @import {Resolver} from '../src/types.js'
 * @param {object} options
 * @param {string} options.tableUrl - Base S3 URL of the table.
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @param {Schema} [options.schema] - The schema of the table.
 * @param {2 | 3} [options.formatVersion] - Iceberg format version (default 2).
 * @param {PartitionSpec} [options.partitionSpec] - Partition spec (default unpartitioned).
 * @param {SortOrder} [options.sortOrder] - Sort order (default unsorted).
 * @param {Record<string, string>} [options.properties] - Table properties.
 * @returns {Promise<TableMetadata>} The Iceberg table metadata as a JSON object.
 */
export async function icebergCreate ({
  tableUrl,
  resolver,
  schema,
  formatVersion = 2,
  partitionSpec,
  sortOrder,
  properties,
}) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (formatVersion !== 2 && formatVersion !== 3) {
    throw new Error(`unsupported format-version: ${formatVersion}`)
  }
  const metadataVersion = 1
  const metadataUrl = translateS3Url(`${tableUrl}/metadata/v${metadataVersion}.metadata.json`)

  /** @type {Schema} */
  const initialSchema = schema ?? { type: 'struct', 'schema-id': 0, fields: [] } // default to no columns

  /** @type {PartitionSpec} */
  const initialPartitionSpec = partitionSpec ?? { 'spec-id': 0, fields: [] }

  /** @type {SortOrder} */
  const initialSortOrder = sortOrder ?? { 'order-id': 0, fields: [] }

  /** @type {TableMetadata} */
  const metadata = {
    'format-version': formatVersion,
    'table-uuid': uuid4(),
    location: tableUrl,
    'last-sequence-number': 0,
    'last-updated-ms': Date.now(),
    'last-column-id': maxFieldId(initialSchema.fields),
    'current-schema-id': initialSchema['schema-id'] ?? 0,
    schemas: [initialSchema],
    'default-spec-id': initialPartitionSpec['spec-id'],
    'partition-specs': [initialPartitionSpec],
    'last-partition-id': maxPartitionFieldId(initialPartitionSpec.fields),
    'sort-orders': [initialSortOrder],
    'default-sort-order-id': initialSortOrder['order-id'],
  }
  if (properties) metadata.properties = properties
  if (formatVersion >= 3) metadata['next-row-id'] = 0

  if (!resolver.writer) throw new Error('resolver.writer is required')

  // write initial metadata
  const metadataWriter = resolver.writer(metadataUrl)
  const metadataBytes = new TextEncoder().encode(JSON.stringify(metadata, null, 2))
  metadataWriter.appendBytes(metadataBytes)
  metadataWriter.finish()

  // write version-hint.text
  const versionHintUrl = translateS3Url(`${tableUrl}/version-hint.text`)
  const versionHintWriter = resolver.writer(versionHintUrl)
  const versionHintBytes = new TextEncoder().encode(String(metadataVersion))
  versionHintWriter.appendBytes(versionHintBytes)
  versionHintWriter.finish()

  return metadata
}

/**
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
