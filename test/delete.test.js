import { describe, expect, it } from 'vitest'
import { deleteFileAppliesToDataEntry } from '../src/delete.js'

/**
 * @import {ManifestEntry, TableMetadata} from '../src/types.js'
 */

/** @type {TableMetadata} */
const metadata = {
  'format-version': 2,
  'table-uuid': 'table',
  location: 's3://bucket/table',
  'last-sequence-number': 2,
  'last-updated-ms': 0,
  'last-column-id': 1,
  'current-schema-id': 0,
  schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
  'default-spec-id': 1,
  'partition-specs': [
    { 'spec-id': 0, fields: [] },
    {
      'spec-id': 1,
      fields: [{ 'source-id': 1, 'field-id': 1000, name: 'category', transform: 'identity' }],
    },
  ],
  'last-partition-id': 1000,
  'sort-orders': [{ 'order-id': 0, fields: [] }],
  'default-sort-order-id': 0,
}

describe('deleteFileAppliesToDataEntry', () => {
  it('applies equality deletes from unpartitioned specs globally', () => {
    const data = entry({ sequenceNumber: 1n, partitionSpecId: 1, partition: { 1000: 'books' } })
    const del = entry({ sequenceNumber: 2n, partitionSpecId: 0, content: 2, partition: {} })

    expect(deleteFileAppliesToDataEntry(data, del, metadata, 'equality')).toBe(true)
  })

  it('applies equality deletes only to older data files in the same partition', () => {
    const data = entry({ sequenceNumber: 1n, partitionSpecId: 1, partition: { 1000: 'books' } })
    const samePartition = entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 2, partition: { 1000: 'books' } })
    const otherPartition = entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 2, partition: { 1000: 'music' } })
    const sameSequence = entry({ sequenceNumber: 1n, partitionSpecId: 1, content: 2, partition: { 1000: 'books' } })

    expect(deleteFileAppliesToDataEntry(data, samePartition, metadata, 'equality')).toBe(true)
    expect(deleteFileAppliesToDataEntry(data, otherPartition, metadata, 'equality')).toBe(false)
    expect(deleteFileAppliesToDataEntry(data, sameSequence, metadata, 'equality')).toBe(false)
  })

  it('applies position deletes to same-sequence data files in the same partition', () => {
    const data = entry({ sequenceNumber: 2n, partitionSpecId: 1, partition: { 1000: 'books' } })
    const samePartition = entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 1, partition: { 1000: 'books' } })
    const otherPartition = entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 1, partition: { 1000: 'music' } })
    const olderDelete = entry({ sequenceNumber: 1n, partitionSpecId: 1, content: 1, partition: { 1000: 'books' } })

    expect(deleteFileAppliesToDataEntry(data, samePartition, metadata, 'position')).toBe(true)
    expect(deleteFileAppliesToDataEntry(data, otherPartition, metadata, 'position')).toBe(false)
    expect(deleteFileAppliesToDataEntry(data, olderDelete, metadata, 'position')).toBe(false)
  })
})

/**
 * @param {object} options
 * @param {bigint} options.sequenceNumber
 * @param {number} options.partitionSpecId
 * @param {Record<number, unknown>} options.partition
 * @param {0|1|2} [options.content]
 * @returns {ManifestEntry}
 */
function entry({ sequenceNumber, partitionSpecId, partition, content = 0 }) {
  return {
    status: 1,
    sequence_number: sequenceNumber,
    file_sequence_number: sequenceNumber,
    partition_spec_id: partitionSpecId,
    data_file: {
      content,
      file_path: 's3://bucket/table/data/a.parquet',
      file_format: content === 1 ? 'puffin' : 'parquet',
      partition,
      record_count: 1n,
      file_size_in_bytes: 1n,
    },
  }
}
