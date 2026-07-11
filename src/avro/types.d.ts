/* Avro types */
interface AvroField {
  name: string
  type: AvroType
  doc?: string
  default?: any
  'field-id'?: number
}

export type AvroType = AvroPrimitiveType | AvroComplexType | AvroLogicalType | AvroBoxedPrimitive

type AvroPrimitiveType = 'null' | 'boolean' | 'int' | 'long' | 'float' | 'double' | 'bytes' | 'string'

// Avro allows a primitive to be written boxed in an object, e.g. { "type": "string" }.
type AvroBoxedPrimitive = {
  type: AvroPrimitiveType
}

interface AvroRecord {
  type: 'record'
  name: string
  namespace?: string
  doc?: string
  aliases?: string[]
  fields: AvroField[]
  'schema-id'?: number
}

interface AvroArray {
  type: 'array'
  items: AvroType
  default?: any[]
  logicalType?: 'map' // Iceberg map-as-array annotation for non-string keys
  'element-id'?: number // Iceberg field id of the array element (e.g. equality_ids[*])
}

type AvroUnion = AvroType[]

interface AvroMap {
  type: 'map'
  values: AvroType
  default?: any
  'key-id'?: number
  'value-id'?: number
}

type AvroEnum = {
  type: 'enum'
  name: string
  symbols: string[]
  default?: string
}

type AvroDate = {
  type: 'int'
  logicalType: 'date'
}

type AvroTimeMillis = {
  type: 'int'
  logicalType: 'time-millis'
}

type AvroTimeMicros = {
  type: 'long'
  logicalType: 'time-micros'
}

type AvroDecimal = {
  type: 'bytes'
  logicalType: 'decimal'
  precision: number
  scale?: number
}

type AvroTimestampMillis = {
  type: 'long'
  logicalType: 'timestamp-millis'
  'adjust-to-utc'?: boolean
}

type AvroTimestampMicros = {
  type: 'long'
  logicalType: 'timestamp-micros'
  'adjust-to-utc'?: boolean
}

type AvroTimestampNanos = {
  type: 'long'
  logicalType: 'timestamp-nanos'
  'adjust-to-utc'?: boolean
}

type AvroLogicalTypeType =
  'date' |
  'decimal' |
  'duration' |
  'local-timestamp-millis' |
  'local-timestamp-micros' |
  'time-millis' |
  'time-micros' |
  'timestamp-millis' |
  'timestamp-micros' |
  'timestamp-nanos' |
  'uuid'

// catch-all: "implementations must ignore unknown logical types when reading"
type AvroGenericLogicalType = {
  type: AvroPrimitiveType
  logicalType: AvroLogicalTypeType
}

type AvroLogicalType =
  AvroDate |
  AvroTimeMillis |
  AvroTimeMicros |
  AvroDecimal |
  AvroTimestampMillis |
  AvroTimestampMicros |
  AvroTimestampNanos |
  AvroGenericLogicalType

type AvroFixed = {
  type: 'fixed'
  name: string
  size: number
  logicalType?: 'uuid' | 'decimal'
  precision?: number
  scale?: number
}

// Avro complex types: records, enums, arrays, maps, unions, fixed
type AvroComplexType = AvroRecord | AvroArray | AvroMap | AvroEnum | AvroUnion | AvroFixed
