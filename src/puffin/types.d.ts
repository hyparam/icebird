export interface PuffinFileMetadata {
  blobs: PuffinBlobMetadata[]
  properties?: Record<string, string>
}

export interface PuffinBlobMetadata {
  type: string
  fields: number[]
  'snapshot-id': number
  'sequence-number': number
  offset: number
  length: number
  'compression-codec'?: string
  properties?: Record<string, string>
}
