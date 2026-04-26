import type { AsyncBuffer } from 'hyparquet'
import type { Writer } from 'hyparquet-writer/src/types.js'

export interface Resolver {
  reader: (path: string, byteLength?: number) => AsyncBuffer | Promise<AsyncBuffer>
  writer?: (path: string) => Writer
}
export type Lister = (path: string) => Promise<string[]>

export interface RestCatalogContext {
  url: string
  prefix: string
  defaults: Record<string, string>
  overrides: Record<string, string>
  requestInit?: RequestInit
}

export interface TableIdentifier {
  namespace: string[]
  name: string
}

export interface LoadTableResponse {
  metadataLocation?: string
  metadata: TableMetadata
  config: Record<string, string>
}

export interface TableMetadata {
  'format-version': number
  'table-uuid': string
  location: string
  'last-sequence-number': number // missing in V1, required in V2+
  'last-updated-ms': number
  'last-column-id': number
  'current-schema-id': number // optional in V1, required in V2+
  schemas: Schema[] // optional in V1, required in V2+
  'default-spec-id': number // optional in V1, required in V2+
  'partition-specs': PartitionSpec[] // optional in V1, required in V2+
  'last-partition-id': number // optional in V1, required in V2+
  properties?: Record<string, string>
  'current-snapshot-id'?: number
  snapshots?: Snapshot[]
  'snapshot-log'?: SnapshotLog[]
  'metadata-log'?: MetadataLog[]
  'sort-orders': SortOrder[] // optional in V1, required in V2+
  'default-sort-order-id': number // optional in V1, required in V2+
  refs?: Record<string, SnapshotRef>
  statistics?: TableStatistics[]
  'partition-statistics'?: PartitionStatistics[]
  'next-row-id'?: bigint // required in V3
  // 'encryption-keys'?: EncryptionKeys[]
}

export interface Schema {
  type: 'struct'
  'schema-id': number
  'identifier-field-ids'?: number[]
  fields: Field[]
}

export interface Field {
  id: number
  name: string
  required: boolean
  type: IcebergType
  doc?: string
  'initial-default'?: any
  'write-default'?: any
}

export type IcebergType =
  'unknown' |
  'boolean' |
  'int' |
  'long' |
  'float' |
  'double' |
  'date' |
  'time' |
  'timestamp' |
  'timestamptz' |
  'timestamp_ns' |
  'timestamptz_ns' |
  'string' |
  'uuid' |
  `fixed[${number}]` |
  'binary' |
  `decimal(${number},${number})` |
  `decimal(${number}, ${number})` |
  'variant' |
  'geometry' |
  `geometry(${string})` |
  'geography' |
  `geography(${string})` |
  IcebergNestedType

export type IcebergNestedType =
  | Schema
  | {
      type: 'list'
      'element-id': number
      'element-required': boolean
      element: IcebergType
    }
  | {
      type: 'map'
      'key-id': number
      key: IcebergType
      'value-id': number
      'value-required': boolean
      value: IcebergType
    }

export interface PartitionSpec {
  'spec-id': number
  fields: PartitionField[]
}
interface PartitionField {
  'source-id'?: number
  'source-ids'?: number[]
  'field-id': number
  name: string
  transform: PartitionTransform
}
export type PartitionTransform =
  'identity' |
  `bucket[${number}]` |
  `truncate[${number}]` |
  'year' |
  'month' |
  'day' |
  'hour' |
  'void'
interface PartitionStatistics {
  'snapshot-id': bigint
  'statistics-path': string
  'file-size-in-bytes': bigint
}

interface SortOrder {
  'order-id': number
  'fields': SortField[]
}
interface SortField {
  transform: string
  'source-id'?: number
  'source-ids'?: number[] // V3
  'direction': 'asc' | 'desc'
  'null-order': 'nulls-first' | 'nulls-last'
}

export interface NameMapping {
  names: string[]
  'field-id'?: number
  fields?: NameMapping[]
}

export interface Snapshot {
  'snapshot-id': number
  'parent-snapshot-id'?: number
  'sequence-number': number
  'timestamp-ms': number
  'manifest-list': string
  manifests?: Manifest[]
  summary: {
    // spec: "value of these fields should be of string type"
    operation: string
    // 'spark.app.id'?: string
    'added-data-files': string
    'added-records': string
    'added-files-size': string
    'changed-partition-count': string
    'total-records': string
    'total-files-size': string
    'total-data-files': string
    'total-delete-files': string
    'total-position-deletes': string
    'total-equality-deletes': string
  }
  'schema-id'?: number
  'first-row-id'?: bigint // V3
  'added-rows'?: number // V3
}

interface TableStatistics {
  'snapshot-id': number
  'statistics-path': string
  'file-size-in-bytes': bigint
  'file-footer-size-in-bytes': bigint
}

interface SnapshotLog {
  'timestamp-ms': number
  'snapshot-id': number
}

export interface SnapshotRef {
  'snapshot-id': number
  type: 'branch' | 'tag'
  'min-snapshots-to-keep'?: number
  'max-snapshot-age-ms'?: number
  'max-ref-age-ms'?: number
}

/**
 * Subset of Iceberg REST `TableRequirement`s that the staging API emits.
 * The full spec has more (assert-create, assert-last-assigned-*, etc).
 */
export type TableRequirement =
  | { type: 'assert-table-uuid', uuid: string }
  | { type: 'assert-ref-snapshot-id', ref: string, 'snapshot-id': number | null }

/**
 * Subset of Iceberg REST `TableUpdate`s that the staging API emits.
 */
export type TableUpdate =
  | { action: 'add-snapshot', snapshot: Snapshot }
  | {
      action: 'set-snapshot-ref'
      'ref-name': string
      type: 'branch' | 'tag'
      'snapshot-id': number
      'min-snapshots-to-keep'?: number
      'max-snapshot-age-ms'?: number
      'max-ref-age-ms'?: number
    }

/**
 * Output of an `icebergStage*` call: the snapshot just produced, the CAS
 * preconditions and updates a catalog must apply, and the data/manifest files
 * already written to storage (useful for cleanup on commit failure).
 */
export interface StagedUpdate {
  snapshot: Snapshot
  requirements: TableRequirement[]
  updates: TableUpdate[]
  writtenFiles: string[]
}

interface MetadataLog {
  'timestamp-ms': number
  'metadata-file': string
}

export interface Manifest {
  manifest_path: string
  manifest_length: bigint
  partition_spec_id: number
  content: 0 | 1 // 0=data, 1=deletes
  sequence_number?: bigint
  min_sequence_number?: bigint
  added_snapshot_id: bigint
  added_files_count: number
  existing_files_count: number
  deleted_files_count: number
  added_rows_count: bigint
  existing_rows_count: bigint
  deleted_rows_count: bigint
  partitions?: FieldSummary[]
  // key_metadata?: unknown
  first_row_id?: bigint
}

export interface ManifestEntry {
  status: 0 | 1 | 2 // 0=existing, 1=added, 2=deleted
  snapshot_id?: bigint
  sequence_number?: bigint
  file_sequence_number?: bigint
  data_file: DataFile
}

interface FieldSummary {
  'contains-null': boolean
  'contains-nan'?: boolean
  'lower-bound'?: unknown
  'upper-bound'?: unknown
}

export interface DataFile {
  content: 0 | 1 | 2 // 0=data, 1=position_delete, 2=equality_delete
  file_path: string
  file_format: 'avro' | 'orc' | 'parquet' | 'puffin'
  partition: Record<number, unknown> // indexed by field id
  record_count: bigint
  file_size_in_bytes: bigint
  column_sizes?: Record<number, bigint>
  value_counts?: Record<number, bigint>
  null_value_counts?: Record<number, bigint>
  nan_value_counts?: Record<number, bigint>
  lower_bounds?: Record<number, unknown>
  upper_bounds?: Record<number, unknown>
  // key_metadata?: string
  split_offsets?: bigint[]
  equality_ids?: number[]
  sort_order_id?: number
  first_row_id?: bigint
  referenced_data_file?: string
  content_offset?: bigint
  content_size_in_bytes?: bigint
}

export interface FilePositionDelete {
  file_path: string
  pos: bigint
}

/* Avro types */
interface AvroField {
  name: string
  type: AvroType
  doc?: string
  default?: any
  'field-id'?: number
}

export type AvroType = AvroPrimitiveType | AvroComplexType | AvroLogicalType

type AvroPrimitiveType = 'null' | 'boolean' | 'int' | 'long' | 'float' | 'double' | 'bytes' | 'string'

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
}

type AvroUnion = AvroType[]

type AvroDate = {
  type: 'int'
  logicalType: 'date'
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
}

type AvroTimestampMicros = {
  type: 'long'
  logicalType: 'timestamp-micros'
}

type AvroTimestampNanos = {
  type: 'long'
  logicalType: 'timestamp-nanos'
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
  AvroDecimal |
  AvroTimestampMillis |
  AvroTimestampMicros |
  AvroTimestampNanos |
  AvroGenericLogicalType

// Avro complex types: records, enums, arrays, maps, unions, fixed
type AvroComplexType = AvroRecord | AvroArray | AvroUnion
