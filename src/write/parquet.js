import { parquetWrite } from 'hyparquet-writer'
import { sanitize } from '../utils.js'

/**
 * @import {CompressionCodec} from 'hyparquet'
 * @import {Writer} from 'hyparquet-writer/src/types.js'
 * @import {ColumnSource} from 'hyparquet-writer/src/types.js'
 * @import {DecodedArray} from 'hyparquet'
 * @import {Field, IcebergType, Schema} from '../../src/types.js'
 * @import {SchemaElement} from 'hyparquet'
 */

/**
 * Write iceberg records to parquet, embedding the iceberg schema in the
 * parquet key/value metadata so that the read path can map field ids back to
 * physical column names.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {Schema} options.schema
 * @param {Record<string, any>[]} options.records
 * @param {CompressionCodec} [options.codec]
 */
export function writeParquet({ writer, schema, records, codec }) {
  /** @type {ColumnSource[]} */
  const columnData = []
  /** @type {SchemaElement[]} */
  const parquetFields = []

  for (const field of schema.fields) {
    const name = sanitize(field.name)
    const parquetField = icebergTypeToParquetField(name, field)
    if (!parquetField) continue
    columnData.push({
      name,
      data: extractColumn(records, field),
    })
    parquetFields.push(parquetField)
  }

  parquetWrite({
    writer,
    columnData,
    schema: [{ name: 'root', num_children: parquetFields.length }, ...parquetFields],
    kvMetadata: [{ key: 'iceberg.schema', value: JSON.stringify(schema) }],
    codec,
  })
}

/**
 * @param {Record<string, any>[]} records
 * @param {Field} field
 * @returns {DecodedArray}
 */
function extractColumn(records, field) {
  const out = new Array(records.length)
  const writeDefault = field['write-default']
  for (let i = 0; i < records.length; i++) {
    const v = records[i][field.name]
    if (v !== undefined) {
      out[i] = v
    } else {
      out[i] = writeDefault !== undefined ? writeDefault : null
    }
  }
  return out
}

/**
 * @param {string} name
 * @param {Field} field
 * @returns {SchemaElement|undefined}
 */
function icebergTypeToParquetField(name, field) {
  const type = typeName(field.type)
  const repetition_type = field.required ? 'REQUIRED' : 'OPTIONAL'
  if (type.startsWith('geometry')) {
    return { name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOMETRY' }, repetition_type }
  }
  if (type.startsWith('geography')) {
    return { name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOGRAPHY' }, repetition_type }
  }
  switch (type) {
  case 'unknown':
    if (field.required) throw new Error('unsupported required iceberg type: unknown')
    return undefined
  case 'boolean': return { name, type: 'BOOLEAN', repetition_type }
  case 'int': return { name, type: 'INT32', repetition_type }
  case 'long': return { name, type: 'INT64', repetition_type }
  case 'float': return { name, type: 'FLOAT', repetition_type }
  case 'double': return { name, type: 'DOUBLE', repetition_type }
  case 'string': return { name, type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type }
  case 'binary': return { name, type: 'BYTE_ARRAY', repetition_type }
  case 'uuid':
    return { name, type: 'FIXED_LEN_BYTE_ARRAY', type_length: 16, logical_type: { type: 'UUID' }, repetition_type }
  case 'timestamp':
    return timestampField(name, repetition_type, false, 'MICROS')
  case 'timestamptz':
    return timestampField(name, repetition_type, true, 'MICROS')
  case 'timestamp_ns':
    return timestampField(name, repetition_type, false, 'NANOS')
  case 'timestamptz_ns':
    return timestampField(name, repetition_type, true, 'NANOS')
  default:
    throw new Error(`unsupported iceberg type: ${type}`)
  }
}

/**
 * @param {IcebergType} type
 * @returns {string}
 */
function typeName(type) {
  return typeof type === 'string' ? type : type.type
}

/**
 * @param {string} name
 * @param {'REQUIRED'|'OPTIONAL'|'REPEATED'} repetition_type
 * @param {boolean} isAdjustedToUTC
 * @param {'MICROS'|'NANOS'} unit
 * @returns {SchemaElement}
 */
function timestampField(name, repetition_type, isAdjustedToUTC, unit) {
  return {
    name,
    type: 'INT64',
    logical_type: { type: 'TIMESTAMP', isAdjustedToUTC, unit },
    repetition_type,
  }
}
