import { describe, expect, it } from 'vitest'
import { applicablePositionDeletes, deleteFileAppliesToDataEntry } from '../src/delete.js'

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

  it('keeps -0.0 and +0.0 distinct for floating partition equality', () => {
    const data = entry({ sequenceNumber: 1n, partitionSpecId: 1, partition: { 1000: -0 } })
    const negativeZero = entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 2, partition: { 1000: -0 } })
    const positiveZero = entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 2, partition: { 1000: 0 } })
    const nan = entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 2, partition: { 1000: NaN } })
    const nanData = entry({ sequenceNumber: 1n, partitionSpecId: 1, partition: { 1000: NaN } })

    expect(deleteFileAppliesToDataEntry(data, negativeZero, metadata, 'equality')).toBe(true)
    expect(deleteFileAppliesToDataEntry(data, positiveZero, metadata, 'equality')).toBe(false)
    expect(deleteFileAppliesToDataEntry(nanData, nan, metadata, 'equality')).toBe(true)
  })

  it.each(['position', 'equality'])('compares date-encoded day partitions for %s deletes', deleteType => {
    const dayMetadata = {
      ...metadata,
      'partition-specs': [{
        'spec-id': 1,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'created_day', transform: 'day' }],
      }],
    }
    const cases = [
      [new Date(-86400000), -1, true],
      [-1, new Date(-86400000), true],
      [new Date(0), 0, true],
      [0, new Date(0), true],
      [new Date(20000 * 86400000), 20000, true],
      [20000, new Date(20000 * 86400000), true],
      [new Date(0), 1, false],
      [1, new Date(0), false],
      [new Date(0), null, false],
      [undefined, new Date(0), false],
    ]
    for (const [dataValue, deleteValue, expected] of cases) {
      const data = entry({ sequenceNumber: 1n, partitionSpecId: 1, partition: { created_day: dataValue } })
      const del = entry({ sequenceNumber: 2n, partitionSpecId: 1, partition: { created_day: deleteValue } })
      expect(deleteFileAppliesToDataEntry(data, del, dayMetadata, /** @type {'position'|'equality'} */ (deleteType))).toBe(expected)
    }
  })

  it('preserves timestamp precision when comparing identity partitions', () => {
    const data = entry({ sequenceNumber: 1n, partitionSpecId: 1, partition: { category: new Date(1000) } })
    const same = entry({ sequenceNumber: 2n, partitionSpecId: 1, partition: { category: new Date(1000) } })
    const later = entry({ sequenceNumber: 2n, partitionSpecId: 1, partition: { category: new Date(2000) } })
    const ordinal = entry({ sequenceNumber: 2n, partitionSpecId: 1, partition: { category: 0 } })
    expect(deleteFileAppliesToDataEntry(data, same, metadata, 'equality')).toBe(true)
    expect(deleteFileAppliesToDataEntry(data, later, metadata, 'equality')).toBe(false)
    expect(deleteFileAppliesToDataEntry(data, ordinal, metadata, 'equality')).toBe(false)
  })

  it.each(['position', 'equality'])('compares promoted integer partitions for %s deletes without lossy coercion', deleteType => {
    const cases = [
      [7, 7n, true],
      [7n, 7, true],
      [-2147483648, -2147483648n, true],
      [2147483647n, 2147483647, true],
      [0, 0n, true],
      [7, 8n, false],
      [8n, 7, false],
      [9007199254740992, 9007199254740993n, false],
      [1.5, 1n, false],
      [NaN, 0n, false],
      [Infinity, 0n, false],
      ['7', 7n, false],
      [null, 0n, false],
      [undefined, 0n, false],
    ]
    for (const [dataValue, deleteValue, expected] of cases) {
      const data = entry({ sequenceNumber: 1n, partitionSpecId: 1, partition: { 1000: dataValue } })
      const del = entry({ sequenceNumber: 2n, partitionSpecId: 1, content: deleteType === 'position' ? 1 : 2, partition: { 1000: deleteValue } })
      expect(deleteFileAppliesToDataEntry(data, del, metadata, /** @type {'position'|'equality'} */ (deleteType))).toBe(expected)
    }
  })
})

describe('applicablePositionDeletes', () => {
  const data = entry({ sequenceNumber: 1n, partitionSpecId: 1, partition: { 1000: 'books' } })

  it('unions position delete files when no deletion vector applies', () => {
    const groups = [
      { deleteEntry: entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 1, partition: { 1000: 'books' }, fileFormat: 'parquet' }), positions: new Set([1n]) },
      { deleteEntry: entry({ sequenceNumber: 3n, partitionSpecId: 1, content: 1, partition: { 1000: 'books' }, fileFormat: 'parquet' }), positions: new Set([2n]) },
    ]
    expect(applicablePositionDeletes(data, groups, metadata)).toEqual(new Set([1n, 2n]))
  })

  it('ignores position delete files when a deletion vector applies', () => {
    const groups = [
      { deleteEntry: entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 1, partition: { 1000: 'books' }, fileFormat: 'parquet' }), positions: new Set([1n]) },
      { deleteEntry: entry({ sequenceNumber: 3n, partitionSpecId: 1, content: 1, partition: { 1000: 'books' } }), positions: new Set([2n]) },
    ]
    expect(applicablePositionDeletes(data, groups, metadata)).toEqual(new Set([2n]))
  })

  it('still applies position delete files when the deletion vector does not apply', () => {
    const groups = [
      { deleteEntry: entry({ sequenceNumber: 2n, partitionSpecId: 1, content: 1, partition: { 1000: 'books' }, fileFormat: 'parquet' }), positions: new Set([1n]) },
      { deleteEntry: entry({ sequenceNumber: 3n, partitionSpecId: 1, content: 1, partition: { 1000: 'music' } }), positions: new Set([2n]) },
    ]
    expect(applicablePositionDeletes(data, groups, metadata)).toEqual(new Set([1n]))
  })

  it('returns an empty set with no groups', () => {
    expect(applicablePositionDeletes(data, undefined, metadata)).toEqual(new Set())
  })
})

/**
 * @param {object} options
 * @param {bigint} options.sequenceNumber
 * @param {number} options.partitionSpecId
 * @param {Record<string, unknown>} options.partition
 * @param {0|1|2} [options.content]
 * @param {'parquet'|'puffin'} [options.fileFormat]
 * @returns {ManifestEntry}
 */
function entry({ sequenceNumber, partitionSpecId, partition, content = 0, fileFormat }) {
  return {
    status: 1,
    sequence_number: sequenceNumber,
    file_sequence_number: sequenceNumber,
    partition_spec_id: partitionSpecId,
    data_file: {
      content,
      file_path: 's3://bucket/table/data/a.parquet',
      file_format: fileFormat ?? (content === 1 ? 'puffin' : 'parquet'),
      partition,
      record_count: 1n,
      file_size_in_bytes: 1n,
    },
  }
}
