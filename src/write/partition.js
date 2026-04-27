/**
 * @import {AvroField, AvroType, Field, IcebergType, PartitionSpec, Schema} from '../../src/types.js'
 */

/**
 * Group records by their identity-partition tuple. Throws if the partition
 * spec contains any non-identity transform (year/month/day/hour/bucket/
 * truncate/void) — those are TODO.
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
    if (pf.transform !== 'identity') {
      throw new Error(`unsupported partition transform: ${pf.transform} (only 'identity' is implemented)`)
    }
    const sourceId = pf['source-id']
    if (sourceId === undefined) {
      throw new Error(`partition field ${pf.name} is missing source-id`)
    }
    const sourceField = schema.fields.find(f => f.id === sourceId)
    if (!sourceField) {
      throw new Error(`partition source field id ${sourceId} not found in schema`)
    }
    return { partitionName: pf.name, sourceName: sourceField.name }
  })

  /** @type {Map<string, { partition: Record<string, any>, records: Record<string, any>[] }>} */
  const groups = new Map()
  for (const record of records) {
    /** @type {Record<string, any>} */
    const partition = {}
    const keyParts = []
    for (const { partitionName, sourceName } of sourceFields) {
      const v = record[sourceName]
      partition[partitionName] = v === undefined ? null : v
      keyParts.push(partitionKeyPart(partition[partitionName]))
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
 * nullable Avro field tagged with the partition spec's `field-id`.
 *
 * Identity transforms only.
 *
 * @param {Schema} schema
 * @param {PartitionSpec} partitionSpec
 * @returns {{ name: 'r102', type: 'record', fields: AvroField[] }}
 */
export function partitionAvroSchema(schema, partitionSpec) {
  /** @type {AvroField[]} */
  const fields = partitionSpec.fields.map(pf => {
    if (pf.transform !== 'identity') {
      throw new Error(`unsupported partition transform: ${pf.transform}`)
    }
    const sourceField = schema.fields.find(f => f.id === pf['source-id'])
    if (!sourceField) {
      throw new Error(`partition source field id ${pf['source-id']} not found`)
    }
    return {
      name: pf.name,
      'field-id': pf['field-id'],
      default: null,
      type: ['null', icebergTypeToAvro(sourceField.type)],
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
 * Convert a record's partition values to the form expected by avroWrite:
 *  - 'long' wants bigint
 *  - dates as JS numbers / Date are accepted by the writer's logicalType
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
    if (!sourceField) continue
    const value = partition[pf.name]
    out[pf.name] = value == null ? null : coerceForAvro(value, sourceField.type)
  }
  return out
}

/**
 * @param {any} value
 * @returns {string}
 */
function partitionKeyPart(value) {
  if (value === null || value === undefined) return '__null__'
  if (typeof value === 'bigint') return `b:${value.toString()}`
  if (value instanceof Date) return `d:${value.getTime()}`
  if (value instanceof Uint8Array) return `x:${[...value].map(b => b.toString(16).padStart(2, '0')).join('')}`
  return `${typeof value}:${String(value)}`
}

/**
 * @param {IcebergType} type
 * @returns {AvroType}
 */
function icebergTypeToAvro(type) {
  const name = typeof type === 'string' ? type : type.type
  switch (name) {
  case 'boolean': return 'boolean'
  case 'int': return 'int'
  case 'long': return 'long'
  case 'float': return 'float'
  case 'double': return 'double'
  case 'string': return 'string'
  case 'binary': return 'bytes'
  case 'date': return { type: 'int', logicalType: 'date' }
  case 'timestamp':
  case 'timestamptz':
    return { type: 'long', logicalType: 'timestamp-micros' }
  case 'timestamp_ns':
  case 'timestamptz_ns':
    return { type: 'long', logicalType: 'timestamp-nanos' }
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
  return value
}

/**
 * Helper for reusing one Field type. Re-exported for tests.
 *
 * @param {Field} field
 * @returns {string}
 */
export function fieldTypeName(field) {
  return typeof field.type === 'string' ? field.type : field.type.type
}
