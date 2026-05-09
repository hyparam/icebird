import { bytesToHex } from '../utils.js'
import { applyTransform, transformResultType } from './transform.js'
import {
  decimalRequiredBytes,
  decimalToFixedBytes,
  parseDecimalType,
  toUint8Array,
  uuidToBytes,
} from './conversions.js'

/**
 * @import {AvroField, AvroType, Field, IcebergType, PartitionSpec, Schema} from '../../src/types.js'
 */

/**
 * Group records by their partition tuple. Supports identity, void, bucket[N],
 * truncate[W], year, month, day, and hour transforms.
 *
 * Returns one group per distinct tuple, preserving the order each tuple
 * first appears so output is stable for tests.
 *
 * @param {Record<string, any>[]} records
 * @param {Schema} schema
 * @param {PartitionSpec} partitionSpec
 * @returns {{ partition: Record<string, any>, records: Record<string, any>[] }[]}
 */
export function groupByPartition(records, schema, partitionSpec) {
  const sourceFields = partitionSpec.fields.map(pf => {
    const sourceId = pf['source-id']
    if (sourceId === undefined) {
      throw new Error(`partition field ${pf.name} is missing source-id`)
    }
    const sourceField = schema.fields.find(f => f.id === sourceId)
    if (!sourceField) {
      throw new Error(`partition source field id ${sourceId} not found in schema`)
    }
    return {
      partitionName: pf.name,
      sourceName: sourceField.name,
      sourceType: sourceField.type,
      sourceWriteDefault: sourceField['write-default'],
      transform: pf.transform,
      resultType: transformResultType(pf.transform, sourceField.type),
    }
  })

  /** @type {Map<string, { partition: Record<string, any>, records: Record<string, any>[] }>} */
  const groups = new Map()
  for (const record of records) {
    /** @type {Record<string, any>} */
    const partition = {}
    const keyParts = []
    for (const { partitionName, sourceName, sourceType, sourceWriteDefault, transform, resultType } of sourceFields) {
      let v = record[sourceName]
      if (v === undefined && sourceWriteDefault !== undefined) v = sourceWriteDefault
      partition[partitionName] = applyTransform(transform, v === undefined ? null : v, sourceType)
      keyParts.push(partitionKeyPart(partition[partitionName], resultType))
    }
    const key = JSON.stringify(keyParts)
    let group = groups.get(key)
    if (!group) {
      group = { partition, records: [] }
      groups.set(key, group)
    }
    group.records.push(record)
  }
  return [...groups.values()]
}

/**
 * Build the Avro record type for the manifest entry's `partition` field
 * (`r102`) from the table's partition spec. Each partition field is a
 * nullable Avro field tagged with the partition spec's `field-id`, typed
 * by the transform's result type.
 *
 * @param {Schema} schema
 * @param {PartitionSpec} partitionSpec
 * @returns {{ name: 'r102', type: 'record', fields: AvroField[] }}
 */
export function partitionAvroSchema(schema, partitionSpec) {
  /** @type {AvroField[]} */
  const fields = partitionSpec.fields.map(pf => {
    const sourceField = schema.fields.find(f => f.id === pf['source-id'])
    if (!sourceField) {
      throw new Error(`partition source field id ${pf['source-id']} not found`)
    }
    const resultType = transformResultType(pf.transform, sourceField.type)
    return {
      name: pf.name,
      'field-id': pf['field-id'],
      default: null,
      type: ['null', icebergTypeToAvro(resultType, pf['field-id'])],
    }
  })
  return { type: 'record', name: 'r102', fields }
}

/**
 * JSON serialization of the partition spec for the manifest's
 * `partition-spec` Avro file metadata key.
 *
 * @param {PartitionSpec} partitionSpec
 * @returns {string}
 */
export function partitionSpecJson(partitionSpec) {
  return JSON.stringify(partitionSpec.fields)
}

/**
 * Convert a record's partition values to the form expected by avroWrite. The
 * partition values have already been transformed (so the relevant type is the
 * transform's result type, not the source type).
 *
 * Returns a fresh object so we don't mutate the input.
 *
 * @param {Record<string, any>} partition
 * @param {Schema} schema
 * @param {PartitionSpec} partitionSpec
 * @returns {Record<string, any>}
 */
export function partitionToAvroRecord(partition, schema, partitionSpec) {
  /** @type {Record<string, any>} */
  const out = {}
  for (const pf of partitionSpec.fields) {
    const sourceField = schema.fields.find(f => f.id === pf['source-id'])
    if (!sourceField) {
      throw new Error(`partition source field id ${pf['source-id']} not found`)
    }
    const resultType = transformResultType(pf.transform, sourceField.type)
    const value = partition[pf.name]
    out[pf.name] = value == null ? null : coerceForAvro(value, resultType)
  }
  return out
}

/**
 * @param {any} value
 * @param {IcebergType} type
 * @returns {string}
 */
function partitionKeyPart(value, type) {
  if (value === null || value === undefined) return '__null__'
  const name = typeof type === 'string' ? type : type.type
  if (typeof value === 'number' && (name === 'float' || name === 'double')) {
    return `${name}:${floatPartitionKey(value, name)}`
  }
  if (name === 'long') return `long:${BigInt(value)}`
  if (typeof value === 'bigint') return `b:${value.toString()}`
  if (value instanceof Date) return `d:${value.getTime()}`
  if (value instanceof Uint8Array) return `x:${bytesToHex(value)}`
  return `${typeof value}:${String(value)}`
}

/**
 * @param {number} value
 * @param {'float'|'double'} type
 * @returns {string}
 */
function floatPartitionKey(value, type) {
  if (Number.isNaN(value)) return 'nan'
  const bytes = new Uint8Array(type === 'float' ? 4 : 8)
  const view = new DataView(bytes.buffer)
  if (type === 'float') view.setFloat32(0, value, false)
  else view.setFloat64(0, value, false)
  return bytesToHex(bytes)
}

/**
 * @param {IcebergType} type
 * @param {number} fieldId
 * @returns {AvroType}
 */
function icebergTypeToAvro(type, fieldId) {
  const name = typeof type === 'string' ? type : type.type
  const decimal = parseDecimalType(name)
  if (decimal) {
    return {
      type: 'fixed',
      name: `r102_${fieldId}`,
      size: decimalRequiredBytes(decimal.precision),
      logicalType: 'decimal',
      precision: decimal.precision,
      scale: decimal.scale,
    }
  }
  const fixed = /^fixed\[(\d+)\]$/.exec(name)
  if (fixed) {
    return {
      type: 'fixed',
      name: `r102_${fieldId}`,
      size: parseInt(fixed[1], 10),
    }
  }
  switch (name) {
  case 'boolean': return 'boolean'
  case 'int': return 'int'
  case 'long': return 'long'
  case 'float': return 'float'
  case 'double': return 'double'
  case 'string': return 'string'
  case 'uuid':
    return {
      type: 'fixed',
      name: `r102_${fieldId}`,
      size: 16,
      logicalType: 'uuid',
    }
  case 'binary': return 'bytes'
  case 'date': return { type: 'int', logicalType: 'date' }
  case 'time': return { type: 'long', logicalType: 'time-micros' }
  case 'timestamp':
    return { type: 'long', logicalType: 'timestamp-micros', 'adjust-to-utc': false }
  case 'timestamptz':
    return { type: 'long', logicalType: 'timestamp-micros', 'adjust-to-utc': true }
  case 'timestamp_ns':
    return { type: 'long', logicalType: 'timestamp-nanos', 'adjust-to-utc': false }
  case 'timestamptz_ns':
    return { type: 'long', logicalType: 'timestamp-nanos', 'adjust-to-utc': true }
  default:
    throw new Error(`unsupported partition source type: ${name}`)
  }
}

/**
 * @param {any} value
 * @param {IcebergType} type
 * @returns {any}
 */
function coerceForAvro(value, type) {
  const name = typeof type === 'string' ? type : type.type
  if (name === 'long') {
    return typeof value === 'bigint' ? value : BigInt(value)
  }
  if (name === 'uuid') {
    return uuidToBytes(value, 'uuid partition value')
  }
  const decimal = parseDecimalType(name)
  if (decimal) {
    return decimalToFixedBytes(
      value,
      decimal.precision,
      decimal.scale,
      `decimal(${decimal.precision},${decimal.scale}) partition value`
    )
  }
  const fixed = /^fixed\[(\d+)\]$/.exec(name)
  if (fixed) {
    const bytes = toUint8Array(value)
    const expected = parseInt(fixed[1], 10)
    if (bytes.length !== expected) {
      throw new Error(`expected fixed[${expected}] partition value`)
    }
    return bytes
  }
  return value
}
