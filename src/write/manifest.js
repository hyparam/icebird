import { avroWrite } from '../avro/avro.write.js'

/**
 * @import {Writer} from 'hyparquet-writer'
 * @import {AvroRecord, DataFile, Schema} from '../../src/types.js'
 */

/**
 * Avro schema for a v2 manifest entry with an unpartitioned data file.
 * Field order and ids match the Iceberg v2 spec so other readers can parse it.
 *
 * @type {AvroRecord}
 */
const manifestEntrySchema = {
  type: 'record',
  name: 'manifest_entry',
  fields: [
    { name: 'status', type: 'int', 'field-id': 0 },
    { name: 'snapshot_id', type: ['null', 'long'], default: null, 'field-id': 1 },
    { name: 'sequence_number', type: ['null', 'long'], default: null, 'field-id': 3 },
    { name: 'file_sequence_number', type: ['null', 'long'], default: null, 'field-id': 4 },
    {
      name: 'data_file',
      'field-id': 2,
      type: {
        type: 'record',
        name: 'r2',
        fields: [
          { name: 'content', type: 'int', 'field-id': 134 },
          { name: 'file_path', type: 'string', 'field-id': 100 },
          { name: 'file_format', type: 'string', 'field-id': 101 },
          {
            name: 'partition',
            'field-id': 102,
            type: { type: 'record', name: 'r102', fields: [] },
          },
          { name: 'record_count', type: 'long', 'field-id': 103 },
          { name: 'file_size_in_bytes', type: 'long', 'field-id': 104 },
          { name: 'sort_order_id', type: ['null', 'int'], default: null, 'field-id': 140 },
        ],
      },
    },
  ],
}

/**
 * Iceberg schema-as-struct used for the data file's row layout. This is
 * embedded in the manifest's avro file metadata under the "schema" key.
 *
 * @param {Schema} schema
 * @returns {string}
 */
function icebergSchemaJson(schema) {
  return JSON.stringify(schema)
}

/**
 * Write a v2 data manifest containing a single ADDED entry for `dataFile`.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {Schema} options.schema - current table schema (embedded in metadata)
 * @param {bigint} options.snapshotId
 * @param {DataFile} options.dataFile
 */
export function writeDataManifest({ writer, schema, snapshotId, dataFile }) {
  const record = {
    status: 1,
    snapshot_id: snapshotId,
    sequence_number: null,
    file_sequence_number: null,
    data_file: {
      content: dataFile.content,
      file_path: dataFile.file_path,
      file_format: dataFile.file_format.toUpperCase(),
      partition: {},
      record_count: dataFile.record_count,
      file_size_in_bytes: dataFile.file_size_in_bytes,
      sort_order_id: dataFile.sort_order_id ?? 0,
    },
  }

  avroWrite({
    writer,
    schema: manifestEntrySchema,
    records: [record],
    metadata: {
      'format-version': '2',
      content: 'data',
      schema: icebergSchemaJson(schema),
      'partition-spec': '[]',
      'partition-spec-id': '0',
    },
  })
}
