import { avroWrite } from '../avro/avro.write.js'
import { partitionAvroSchema, partitionSpecJson, partitionToAvroRecord } from './partition.js'

/**
 * @import {Writer} from 'hyparquet-writer'
 * @import {AvroField, AvroRecord, DataFile, PartitionSpec, Schema} from '../../src/types.js'
 */

/**
 * Build an Avro schema for a manifest entry. The `partition` record's
 * fields are derived from the table's partition spec (empty for unpartitioned
 * tables). When `manifestContent === 1` the data_file struct gains the
 * delete-only fields (`equality_ids`, `referenced_data_file`, and v3 DV
 * `content_offset` / `content_size_in_bytes`).
 *
 * @param {Schema} schema
 * @param {PartitionSpec} partitionSpec
 * @param {2|3} formatVersion
 * @param {0|1} [manifestContent]
 * @returns {AvroRecord}
 */
function manifestEntrySchema(schema, partitionSpec, formatVersion, manifestContent = 0) {
  /** @type {AvroField[]} */
  const dataFileFields = [
    { name: 'content', type: 'int', 'field-id': 134 },
    { name: 'file_path', type: 'string', 'field-id': 100 },
    { name: 'file_format', type: 'string', 'field-id': 101 },
    {
      name: 'partition',
      'field-id': 102,
      type: partitionAvroSchema(schema, partitionSpec),
    },
    { name: 'record_count', type: 'long', 'field-id': 103 },
    { name: 'file_size_in_bytes', type: 'long', 'field-id': 104 },
    mapField('column_sizes', 108, 'k117_v118', 117, 118, 'long'),
    mapField('value_counts', 109, 'k119_v120', 119, 120, 'long'),
    mapField('null_value_counts', 110, 'k121_v122', 121, 122, 'long'),
    mapField('nan_value_counts', 137, 'k138_v139', 138, 139, 'long'),
    mapField('lower_bounds', 125, 'k126_v127', 126, 127, 'bytes'),
    mapField('upper_bounds', 128, 'k129_v130', 129, 130, 'bytes'),
    { name: 'sort_order_id', type: ['null', 'int'], default: null, 'field-id': 140 },
  ]
  if (manifestContent === 1) {
    dataFileFields.push({
      name: 'equality_ids',
      'field-id': 135,
      default: null,
      type: ['null', { type: 'array', items: 'int', 'element-id': 136 }],
    })
    dataFileFields.push({ name: 'referenced_data_file', type: ['null', 'string'], default: null, 'field-id': 143 })
    if (formatVersion >= 3) {
      dataFileFields.push({ name: 'content_offset', type: ['null', 'long'], default: null, 'field-id': 144 })
      dataFileFields.push({ name: 'content_size_in_bytes', type: ['null', 'long'], default: null, 'field-id': 145 })
    }
  }
  if (formatVersion >= 3) {
    dataFileFields.push({ name: 'first_row_id', type: ['null', 'long'], default: null, 'field-id': 142 })
  }

  return {
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
          fields: dataFileFields,
        },
      },
    ],
  }
}

/**
 * Build a nullable Avro array-of-{key,value}-records field with the
 * Iceberg "logical-type": "map" annotation.
 *
 * @param {string} name
 * @param {number} fieldId
 * @param {string} recName
 * @param {number} keyId
 * @param {number} valueId
 * @param {'long'|'bytes'} valueType
 * @returns {AvroField}
 */
function mapField(name, fieldId, recName, keyId, valueId, valueType) {
  return {
    name,
    'field-id': fieldId,
    default: null,
    type: ['null', {
      type: 'array',
      logicalType: 'map',
      items: {
        type: 'record',
        name: recName,
        fields: [
          { name: 'key', type: 'int', 'field-id': keyId },
          { name: 'value', type: valueType, 'field-id': valueId },
        ],
      },
    }],
  }
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
 * Encode an Iceberg stat map as an Avro array of {key, value} records,
 * or null if the input has no entries.
 *
 * @template V
 * @param {Record<number, V>|undefined} m
 * @returns {{key: number, value: V}[]|null}
 */
function encodeMap(m) {
  if (!m) return null
  const entries = Object.entries(m)
  if (!entries.length) return null
  return entries.map(([k, value]) => ({ key: Number(k), value }))
}

/**
 * Write a data manifest containing one ADDED entry per `dataFiles` element.
 * The data files must all share the same partition spec.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {Schema} options.schema - current table schema (embedded in metadata)
 * @param {PartitionSpec} options.partitionSpec - spec for this manifest's entries
 * @param {bigint} options.snapshotId
 * @param {DataFile[]} options.dataFiles
 * @param {2|3} [options.formatVersion]
 * @returns {void | Promise<void>} resolves when the writer's `finish()` lands
 */
export function writeDataManifest({ writer, schema, partitionSpec, snapshotId, dataFiles, formatVersion = 2 }) {
  const records = dataFiles.map(dataFile => {
    if (dataFile.content !== 0) {
      throw new Error(`writeDataManifest expects data files (content=0), got content=${dataFile.content}`)
    }
    return manifestEntryRecord(dataFile, schema, partitionSpec, snapshotId, formatVersion, 0)
  })

  return avroWrite({
    writer,
    schema: manifestEntrySchema(schema, partitionSpec, formatVersion, 0),
    records,
    metadata: {
      'format-version': String(formatVersion),
      content: 'data',
      schema: icebergSchemaJson(schema),
      'partition-spec': partitionSpecJson(partitionSpec),
      'partition-spec-id': String(partitionSpec['spec-id']),
    },
  })
}

/**
 * Write a delete manifest containing one ADDED entry per `deleteFiles`
 * element. Each entry must have `content` 1 (position delete) or 2 (equality
 * delete); a single delete manifest may mix both. All entries must share the
 * same partition spec.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {Schema} options.schema
 * @param {PartitionSpec} options.partitionSpec
 * @param {bigint} options.snapshotId
 * @param {DataFile[]} options.deleteFiles
 * @param {2|3} [options.formatVersion]
 * @returns {void | Promise<void>} resolves when the writer's `finish()` lands
 */
export function writeDeleteManifest({ writer, schema, partitionSpec, snapshotId, deleteFiles, formatVersion = 2 }) {
  const records = deleteFiles.map(deleteFile => {
    if (deleteFile.content !== 1 && deleteFile.content !== 2) {
      throw new Error(`writeDeleteManifest expects delete files (content=1 or 2), got content=${deleteFile.content}`)
    }
    if (deleteFile.content === 2 && !deleteFile.equality_ids?.length) {
      throw new Error('equality delete file missing equality_ids')
    }
    return manifestEntryRecord(deleteFile, schema, partitionSpec, snapshotId, formatVersion, 1)
  })

  return avroWrite({
    writer,
    schema: manifestEntrySchema(schema, partitionSpec, formatVersion, 1),
    records,
    metadata: {
      'format-version': String(formatVersion),
      content: 'deletes',
      schema: icebergSchemaJson(schema),
      'partition-spec': partitionSpecJson(partitionSpec),
      'partition-spec-id': String(partitionSpec['spec-id']),
    },
  })
}

/**
 * Build a single manifest entry record from a DataFile, including the
 * delete-only fields when emitting into a delete manifest.
 *
 * @param {DataFile} dataFile
 * @param {Schema} schema
 * @param {PartitionSpec} partitionSpec
 * @param {bigint} snapshotId
 * @param {2|3} formatVersion
 * @param {0|1} manifestContent
 * @returns {Record<string, any>}
 */
function manifestEntryRecord(dataFile, schema, partitionSpec, snapshotId, formatVersion, manifestContent) {
  /** @type {Record<string, any>} */
  const dataFileRecord = {
    content: dataFile.content,
    file_path: dataFile.file_path,
    file_format: dataFile.file_format.toUpperCase(),
    partition: partitionToAvroRecord(
      /** @type {Record<string, any>} */ (dataFile.partition ?? {}),
      schema,
      partitionSpec
    ),
    record_count: dataFile.record_count,
    file_size_in_bytes: dataFile.file_size_in_bytes,
    column_sizes: encodeMap(dataFile.column_sizes),
    value_counts: encodeMap(dataFile.value_counts),
    null_value_counts: encodeMap(dataFile.null_value_counts),
    nan_value_counts: encodeMap(dataFile.nan_value_counts),
    lower_bounds: encodeMap(dataFile.lower_bounds),
    upper_bounds: encodeMap(dataFile.upper_bounds),
    sort_order_id: dataFile.sort_order_id ?? 0,
  }
  if (manifestContent === 1) {
    dataFileRecord.equality_ids = dataFile.equality_ids?.length ? dataFile.equality_ids : null
    dataFileRecord.referenced_data_file = dataFile.referenced_data_file ?? null
    if (formatVersion >= 3) {
      dataFileRecord.content_offset = dataFile.content_offset ?? null
      dataFileRecord.content_size_in_bytes = dataFile.content_size_in_bytes ?? null
    }
  }
  if (formatVersion >= 3) {
    dataFileRecord.first_row_id = dataFile.first_row_id ?? null
  }

  return {
    status: 1,
    snapshot_id: snapshotId,
    sequence_number: null,
    file_sequence_number: null,
    data_file: dataFileRecord,
  }
}
