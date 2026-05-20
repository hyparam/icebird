import { parquetWrite } from 'hyparquet-writer'
import { typeName } from '../schema.js'
import { sanitize } from '../utils.js'
import { decimalRequiredBytes, parseDecimalType } from './conversions.js'

/**
 * @import {CompressionCodec, DecodedArray, SchemaElement} from 'hyparquet'
 * @import {ColumnSource, Writer} from 'hyparquet-writer'
 * @import {Field, IcebergType, Schema} from '../../src/types.js'
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
 * @returns {void | Promise<void>} resolves when the writer's `finish()` lands
 */
export function writeParquet({ writer, schema, records, codec }) {
  /** @type {ColumnSource[]} */
  const columnData = []
  /** @type {SchemaElement[]} */
  const parquetFields = []
  let rootChildren = 0

  for (const field of schema.fields) {
    const name = sanitize(field.name)
    const fieldElements = icebergTypeToParquetFields(name, field.type, field.required, field.id)
    if (!fieldElements.length) continue
    columnData.push({
      name,
      data: extractColumn(records, field),
    })
    parquetFields.push(...fieldElements)
    rootChildren++
  }

  return parquetWrite({
    writer,
    columnData,
    schema: [{ name: 'root', num_children: rootChildren }, ...parquetFields],
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
 * Iceberg requires parquet columns to carry the iceberg `field-id` so readers
 * (Spark, pyiceberg) can map by id instead of by name. The top-level element
 * of each iceberg field gets the id; nested logical types (variant's
 * metadata/value, list's repeated wrapper) inherit it via their parent.
 *
 * @param {string} name
 * @param {IcebergType} type
 * @param {boolean} required
 * @param {number} fieldId
 * @returns {SchemaElement[]}
 */
function icebergTypeToParquetFields(name, type, required, fieldId) {
  const repetition_type = required ? 'REQUIRED' : 'OPTIONAL'
  if (typeof type === 'object') {
    if (type.type === 'list') {
      const elementFields = icebergTypeToParquetFields(
        'element', type.element, type['element-required'], type['element-id']
      )
      if (!elementFields.length) {
        throw new Error(`unsupported iceberg list element type: ${typeName(type.element)}`)
      }
      return [
        {
          name,
          converted_type: 'LIST',
          logical_type: { type: 'LIST' },
          repetition_type,
          num_children: 1,
          field_id: fieldId,
        },
        { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
        ...elementFields,
      ]
    }
    if (type.type === 'map') {
      // Iceberg map keys are always required (no `key-required` in the spec).
      const keyFields = icebergTypeToParquetFields('key', type.key, true, type['key-id'])
      const valueFields = icebergTypeToParquetFields(
        'value', type.value, type['value-required'], type['value-id']
      )
      if (!keyFields.length) {
        throw new Error(`unsupported iceberg map key type: ${typeName(type.key)}`)
      }
      if (!valueFields.length) {
        throw new Error(`unsupported iceberg map value type: ${typeName(type.value)}`)
      }
      return [
        {
          name,
          converted_type: 'MAP',
          logical_type: { type: 'MAP' },
          repetition_type,
          num_children: 1,
          field_id: fieldId,
        },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        ...keyFields,
        ...valueFields,
      ]
    }
    throw new Error(`unsupported iceberg type: ${type.type}`)
  }
  if (type.startsWith('geometry')) {
    return [{ name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOMETRY' }, repetition_type, field_id: fieldId }]
  }
  if (type.startsWith('geography')) {
    return [{ name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOGRAPHY' }, repetition_type, field_id: fieldId }]
  }
  const decimal = parseDecimalType(type)
  if (decimal) {
    const { precision, scale } = decimal
    return [{
      name,
      type: 'FIXED_LEN_BYTE_ARRAY',
      type_length: decimalRequiredBytes(precision),
      converted_type: 'DECIMAL',
      logical_type: { type: 'DECIMAL', precision, scale },
      precision,
      scale,
      repetition_type,
      field_id: fieldId,
    }]
  }
  const fixedLen = parseFixedType(type)
  if (fixedLen !== undefined) {
    return [{ name, type: 'FIXED_LEN_BYTE_ARRAY', type_length: fixedLen, repetition_type, field_id: fieldId }]
  }
  switch (type) {
  case 'unknown':
    if (required) throw new Error('unsupported required iceberg type: unknown')
    return []
  case 'variant':
    return [
      { name, repetition_type, num_children: 2, logical_type: { type: 'VARIANT' }, field_id: fieldId },
      { name: 'metadata', type: 'BYTE_ARRAY', repetition_type: 'REQUIRED' },
      { name: 'value', type: 'BYTE_ARRAY', repetition_type: 'OPTIONAL' },
    ]
  case 'boolean': return [{ name, type: 'BOOLEAN', repetition_type, field_id: fieldId }]
  case 'int': return [{ name, type: 'INT32', repetition_type, field_id: fieldId }]
  case 'long': return [{ name, type: 'INT64', repetition_type, field_id: fieldId }]
  case 'float': return [{ name, type: 'FLOAT', repetition_type, field_id: fieldId }]
  case 'double': return [{ name, type: 'DOUBLE', repetition_type, field_id: fieldId }]
  case 'string': return [{ name, type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type, field_id: fieldId }]
  case 'binary': return [{ name, type: 'BYTE_ARRAY', repetition_type, field_id: fieldId }]
  case 'uuid':
    return [{ name, type: 'FIXED_LEN_BYTE_ARRAY', type_length: 16, logical_type: { type: 'UUID' }, repetition_type, field_id: fieldId }]
  case 'date':
    return [{ name, type: 'INT32', converted_type: 'DATE', logical_type: { type: 'DATE' }, repetition_type, field_id: fieldId }]
  case 'time':
    return [{
      name,
      type: 'INT64',
      converted_type: 'TIME_MICROS',
      logical_type: { type: 'TIME', isAdjustedToUTC: false, unit: 'MICROS' },
      repetition_type,
      field_id: fieldId,
    }]
  case 'timestamp':
    return [timestampField(name, repetition_type, false, 'MICROS', fieldId)]
  case 'timestamptz':
    return [timestampField(name, repetition_type, true, 'MICROS', fieldId)]
  case 'timestamp_ns':
    return [timestampField(name, repetition_type, false, 'NANOS', fieldId)]
  case 'timestamptz_ns':
    return [timestampField(name, repetition_type, true, 'NANOS', fieldId)]
  default:
    throw new Error(`unsupported iceberg type: ${type}`)
  }
}

/**
 * Parse iceberg `fixed[N]` strings into the byte length.
 *
 * @param {string} type
 * @returns {number | undefined}
 */
function parseFixedType(type) {
  const m = /^fixed\[(\d+)\]$/.exec(type)
  if (!m) return undefined
  return parseInt(m[1], 10)
}

/**
 * @param {string} name
 * @param {'REQUIRED'|'OPTIONAL'|'REPEATED'} repetition_type
 * @param {boolean} isAdjustedToUTC
 * @param {'MICROS'|'NANOS'} unit
 * @param {number} field_id
 * @returns {SchemaElement}
 */
function timestampField(name, repetition_type, isAdjustedToUTC, unit, field_id) {
  return {
    name,
    type: 'INT64',
    logical_type: { type: 'TIMESTAMP', isAdjustedToUTC, unit },
    repetition_type,
    field_id,
  }
}
