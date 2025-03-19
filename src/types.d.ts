
export interface IcebergMetadata {
  'format-version': number
  'table-uuid': string
  location: string
  'last-sequence-number': number
  'last-updated-ms': number
  'last-column-id': number
  'current-schema-id': number
  schemas: Schema[]
  'default-spec-id': number
  'partition-specs': PartitionSpec[]
  'last-partition-id': number
  'default-sort-order-id': number
  'sort-orders': SortOrder[]
  properties: object
  'current-snapshot-id': number
  refs: object
  snapshots: Snapshot[]
  statistics: TableStatistics[]
  'snapshot-log': SnapshotLog[]
  'metadata-log': MetadataLog[]
}

export interface Schema {
  type: string
  "schema-id": number
  fields: Field[]
}

interface Field {
  id: number
  name: string
  required: boolean
  type: string
}

interface PartitionSpec {
  'spec-id': number
  fields: Field[]
}

interface SortOrder {
  "order-id": number
  "fields": unknown[]
}

export interface Snapshot {
  'sequence-number': number
  'snapshot-id': number
  'timestamp-ms': number
  summary: {
    operation: string
    'spark.app.id': string
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
  'manifest-list': string
  manifests?: Manifest[]
  'schema-id': number
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

interface MetadataLog {
  'timestamp-ms': number
  'metadata-file': string
}

export interface Manifest {
  manifest_path: string
  manifest_length: bigint
  partition_spec_id: number
  content: 0 | 1
  sequence_number: bigint
  min_sequence_number: bigint
  added_snapshot_id: bigint
  added_data_files_count: number
  existing_data_files_count: number
  deleted_data_files_count: number
  added_rows_count: bigint
  existing_rows_count: bigint
  deleted_rows_count: bigint
  partitions?: FieldSummary[]
}

interface FieldSummary {
  'contains-null': boolean
}

export interface DataFile {
  content: 0 | 1 | 2
  file_path: string
  file_format: string
  record_count: bigint
  file_size_in_bytes: bigint
  split_offsets: bigint[]
  equality_ids?: number[]
  sort_order_id: number
}

export interface FilePositionDelete {
  file_path: string
  pos: bigint
}
