import { avroWrite } from '../avro/avro.write.js'

/**
 * @import {Writer} from 'hyparquet-writer'
 * @import {AvroField, AvroRecord, Manifest} from '../../src/types.js'
 */

/**
 * Build an Avro schema for a manifest list entry.
 *
 * @param {2|3} formatVersion
 * @returns {AvroRecord}
 */
function manifestFileSchema(formatVersion) {
  /** @type {AvroField[]} */
  const fields = [
    { name: 'manifest_path', type: 'string', 'field-id': 500 },
    { name: 'manifest_length', type: 'long', 'field-id': 501 },
    { name: 'partition_spec_id', type: 'int', 'field-id': 502 },
    { name: 'content', type: 'int', 'field-id': 517 },
    { name: 'sequence_number', type: 'long', 'field-id': 515 },
    { name: 'min_sequence_number', type: 'long', 'field-id': 516 },
    { name: 'added_snapshot_id', type: 'long', 'field-id': 503 },
    { name: 'added_files_count', type: 'int', 'field-id': 504 },
    { name: 'existing_files_count', type: 'int', 'field-id': 505 },
    { name: 'deleted_files_count', type: 'int', 'field-id': 506 },
    { name: 'added_rows_count', type: 'long', 'field-id': 512 },
    { name: 'existing_rows_count', type: 'long', 'field-id': 513 },
    { name: 'deleted_rows_count', type: 'long', 'field-id': 514 },
    {
      name: 'partitions',
      type: ['null', {
        type: 'array',
        'element-id': 508,
        items: {
          type: 'record',
          name: 'r508',
          fields: [
            { name: 'contains_null', type: 'boolean', 'field-id': 509 },
            { name: 'contains_nan', type: ['null', 'boolean'], default: null, 'field-id': 518 },
            { name: 'lower_bound', type: ['null', 'bytes'], default: null, 'field-id': 510 },
            { name: 'upper_bound', type: ['null', 'bytes'], default: null, 'field-id': 511 },
          ],
        },
      }],
      default: null,
      'field-id': 507,
    },
  ]
  if (formatVersion >= 3) {
    fields.push({ name: 'first_row_id', type: ['null', 'long'], default: null, 'field-id': 520 })
  }

  return {
    type: 'record',
    name: 'manifest_file',
    fields,
  }
}

/**
 * Write a v2 manifest list containing the given manifest entries.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {bigint} options.snapshotId
 * @param {bigint} options.sequenceNumber
 * @param {Manifest[]} options.manifests
 * @param {2|3} [options.formatVersion]
 * @returns {void | Promise<void>} resolves when the writer's `finish()` lands
 */
export function writeManifestList({ writer, snapshotId, sequenceNumber, manifests, formatVersion = 2 }) {
  const records = manifests.map(m => {
    /** @type {Record<string, any>} */
    const record = {
      manifest_path: m.manifest_path,
      manifest_length: m.manifest_length,
      partition_spec_id: m.partition_spec_id,
      content: m.content,
      sequence_number: m.sequence_number ?? sequenceNumber,
      min_sequence_number: m.min_sequence_number ?? sequenceNumber,
      added_snapshot_id: m.added_snapshot_id,
      added_files_count: m.added_files_count,
      existing_files_count: m.existing_files_count,
      deleted_files_count: m.deleted_files_count,
      added_rows_count: m.added_rows_count,
      existing_rows_count: m.existing_rows_count,
      deleted_rows_count: m.deleted_rows_count,
      partitions: m.partitions ?? null,
    }
    if (formatVersion >= 3) {
      record.first_row_id = m.content === 0 ? m.first_row_id ?? null : null
    }
    return record
  })

  return avroWrite({
    writer,
    schema: manifestFileSchema(formatVersion),
    records,
    metadata: {
      'format-version': String(formatVersion),
      'snapshot-id': String(snapshotId),
      'sequence-number': String(sequenceNumber),
    },
  })
}
