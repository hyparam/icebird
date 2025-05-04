/**
 * @import {AvroRecord} from '../src/types.js'
 */

/** @type {AvroRecord} */
export const manifestSchema = {
  type: 'record',
  name: 'ManifestEntry',
  fields: [
    { 'field-id': 500, name: 'manifest_path', type: 'string' },
    { 'field-id': 501, name: 'manifest_length', type: 'long' },
    { 'field-id': 502, name: 'partition_spec_id', type: 'int' },
    { 'field-id': 517, name: 'content', type: 'int' }, // 0=data, 1=deletes
    { 'field-id': 515, name: 'sequence_number', type: ['null', 'long'], default: null },
    { 'field-id': 516, name: 'min_sequence_number', type: ['null', 'long'], default: null },
    { 'field-id': 503, name: 'added_snapshot_id', type: 'long' },
    { 'field-id': 504, name: 'added_data_files_count', type: 'int' },
    { 'field-id': 505, name: 'existing_data_files_count', type: 'int' },
    { 'field-id': 506, name: 'deleted_data_files_count', type: 'int' },
    { 'field-id': 512, name: 'added_rows_count', type: 'long' },
    { 'field-id': 513, name: 'existing_rows_count', type: 'long' },
    { 'field-id': 514, name: 'deleted_rows_count', type: 'long' },
    {
      'field-id': 507,
      name: 'partitions',
      type: [
        'null',
        {
          type: 'array',
          items: {
            type: 'record',
            name: 'FieldSummary',
            fields: [
              { 'field-id': 509, name: 'contains-null', type: 'boolean' },
              { 'field-id': 518, name: 'contains-nan', type: ['null', 'boolean'], default: null },
              { 'field-id': 510, name: 'lower-bound', type: ['null', 'string'], default: null },
              { 'field-id': 511, name: 'upper-bound', type: ['null', 'string'], default: null },
            ],
          },
        },
      ],
      default: null,
    },
    { 'field-id': 519, name: 'key_metadata', type: ['null', 'bytes'], default: null },
    { 'field-id': 520, name: 'first_row_id', type: ['null', 'long'], default: null },
  ],
}

/** @type {AvroRecord} */
export const dataFileSchema = {
  type: 'record',
  name: 'data_file',
  fields: [
    { 'field-id': 134, name: 'content', type: 'int' },
    { 'field-id': 100, name: 'file_path', type: 'string' },
    { 'field-id': 101, name: 'file_format', type: 'string' },
    { 'field-id': 102, name: 'partition', type: { type: 'record', name: 'void', fields: [] } },
    { 'field-id': 103, name: 'record_count', type: 'long' },
    { 'field-id': 104, name: 'file_size_in_bytes', type: 'long' },
    { 'field-id': 132, name: 'split_offsets', type: ['null', { type: 'array', items: 'long' }], default: null },
  ],
}

/** @type {AvroRecord} */
export const manifestEntrySchema = {
  type: 'record',
  name: 'manifest_entry',
  fields: [
    { 'field-id': 0, name: 'status', type: 'int' },
    { 'field-id': 1, name: 'snapshot_id', type: ['null', 'long'], default: null },
    { 'field-id': 3, name: 'sequence_number', type: ['null', 'long'], default: null },
    { 'field-id': 4, name: 'file_sequence_number', type: ['null', 'long'], default: null },
    { 'field-id': 2, name: 'data_file', type: dataFileSchema },
  ],
}
