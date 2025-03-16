
export interface IcebergMetadata {
  'format-version': number
  'table-uuid': string
  location: string
  'last-sequence-number': number
  'last-updated-ms': number
  'last-column-id': number
  'current-schema-id': number
  schemas: any[]
  'default-spec-id': number
  'partition-specs': any[]
  'last-partition-id': number
  'default-sort-order-id': number
  'sort-orders': any[]
  properties: object
  'current-snapshot-id': number
  refs: object
  snapshots: Snapshot[]
  statistics: any[]
  'snapshot-log': any[]
  'metadata-log': any[]
}

interface Snapshot {
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

interface Manifest {
  manifest_path: string
  manifest_length: number
  partition_spec_id: number
}

export interface DataFile {
  content: 0 | 1 | 2
  file_path: string
  file_format: string
  record_count: bigint
  file_size_in_bytes: bigint
  equality_ids: any[]
}
