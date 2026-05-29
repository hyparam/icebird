import { describe, expect, it } from 'vitest'
import { partitionMightMatch } from '../src/prune.js'

/**
 * @import {ManifestEntry, PartitionField, Schema, TableMetadata} from '../src/types.js'
 */

/**
 * @param {PartitionField[]} fields
 * @returns {TableMetadata}
 */
function meta(fields) {
  return /** @type {TableMetadata} */ ({
    'partition-specs': [{ 'spec-id': 0, fields }],
  })
}

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'int' },
    { id: 2, name: 'name', required: false, type: 'string' },
    { id: 3, name: 'ts', required: false, type: 'timestamp' },
    { id: 4, name: 'price', required: false, type: 'double' },
  ],
}

/**
 * @param {Record<string, any>} partition
 * @param {number} [specId]
 * @returns {ManifestEntry}
 */
function entry(partition, specId = 0) {
  return /** @type {ManifestEntry} */ ({
    status: 1,
    partition_spec_id: specId,
    data_file: { partition },
  })
}

describe('partitionMightMatch — identity', () => {
  const m = meta([{ 'source-id': 1, 'field-id': 1000, name: 'id', transform: 'identity' }])

  it('keeps and prunes equality exactly', () => {
    expect(partitionMightMatch({ id: { $eq: 5n } }, entry({ id: 5 }), schema, m)).toBe(true)
    expect(partitionMightMatch({ id: { $eq: 5n } }, entry({ id: 6 }), schema, m)).toBe(false)
  })

  it('handles range predicates exactly', () => {
    expect(partitionMightMatch({ id: { $gt: 5n } }, entry({ id: 6 }), schema, m)).toBe(true)
    expect(partitionMightMatch({ id: { $gt: 5n } }, entry({ id: 5 }), schema, m)).toBe(false)
    expect(partitionMightMatch({ id: { $lte: 5n } }, entry({ id: 5 }), schema, m)).toBe(true)
    expect(partitionMightMatch({ id: { $lt: 5n } }, entry({ id: 5 }), schema, m)).toBe(false)
  })

  it('handles $in and $nin', () => {
    expect(partitionMightMatch({ id: { $in: [1n, 5n] } }, entry({ id: 5 }), schema, m)).toBe(true)
    expect(partitionMightMatch({ id: { $in: [1n, 2n] } }, entry({ id: 5 }), schema, m)).toBe(false)
    expect(partitionMightMatch({ id: { $nin: [5n] } }, entry({ id: 5 }), schema, m)).toBe(false)
    expect(partitionMightMatch({ id: { $nin: [1n] } }, entry({ id: 5 }), schema, m)).toBe(true)
  })

  it('prunes string identity on equality only', () => {
    const sm = meta([{ 'source-id': 2, 'field-id': 1000, name: 'name', transform: 'identity' }])
    expect(partitionMightMatch({ name: { $eq: 'a' } }, entry({ name: 'a' }), schema, sm)).toBe(true)
    expect(partitionMightMatch({ name: { $eq: 'a' } }, entry({ name: 'b' }), schema, sm)).toBe(false)
    // String ordering is left to the engine, so range predicates never prune.
    expect(partitionMightMatch({ name: { $gt: 'a' } }, entry({ name: 'a' }), schema, sm)).toBe(true)
  })

  it('prunes a null partition value only on equality with a concrete literal', () => {
    expect(partitionMightMatch({ id: { $eq: 5n } }, entry({ id: null }), schema, m)).toBe(false)
    expect(partitionMightMatch({ id: { $ne: 5n } }, entry({ id: null }), schema, m)).toBe(true)
    expect(partitionMightMatch({ id: { $gt: 5n } }, entry({ id: null }), schema, m)).toBe(true)
  })
})

describe('partitionMightMatch — bucket', () => {
  const m = meta([{ 'source-id': 1, 'field-id': 1000, name: 'id_bucket', transform: 'bucket[4]' }])

  it('prunes equality by projecting the literal through the bucket transform', () => {
    // bucket[4](1) === 0
    expect(partitionMightMatch({ id: { $eq: 1n } }, entry({ id_bucket: 0 }), schema, m)).toBe(true)
    expect(partitionMightMatch({ id: { $eq: 1n } }, entry({ id_bucket: 1 }), schema, m)).toBe(false)
  })

  it('cannot prune range predicates (bucket is not order-preserving)', () => {
    expect(partitionMightMatch({ id: { $gt: 1n } }, entry({ id_bucket: 3 }), schema, m)).toBe(true)
  })
})

describe('partitionMightMatch — monotonic (day)', () => {
  const m = meta([{ 'source-id': 3, 'field-id': 1000, name: 'ts_day', transform: 'day' }])
  /**
   * @param {string} s
   * @returns {number}
   */
  function day(s) {
    return Math.floor(new Date(s).getTime() / 86400000)
  }

  it('prunes ranges by projecting the literal into day space', () => {
    const ts = new Date('2022-06-15T12:00:00Z')
    // file holds day(2022-01-01); ts > 2022-06-15 cannot be in it
    expect(partitionMightMatch({ ts: { $gt: ts } }, entry({ ts_day: day('2022-01-01') }), schema, m)).toBe(false)
    expect(partitionMightMatch({ ts: { $gt: ts } }, entry({ ts_day: day('2022-12-01') }), schema, m)).toBe(true)
    // boundary day is kept (permissive inclusive projection)
    expect(partitionMightMatch({ ts: { $gt: ts } }, entry({ ts_day: day('2022-06-15') }), schema, m)).toBe(true)
  })

  it('cannot prune $ne on a monotonic transform', () => {
    const ts = new Date('2022-01-01T00:00:00Z')
    expect(partitionMightMatch({ ts: { $ne: ts } }, entry({ ts_day: day('2022-01-01') }), schema, m)).toBe(true)
  })
})

describe('partitionMightMatch — combinators and conservative defaults', () => {
  const m = meta([{ 'source-id': 1, 'field-id': 1000, name: 'id', transform: 'identity' }])

  it('$and prunes if any conjunct rules out the file', () => {
    expect(partitionMightMatch({ $and: [{ id: { $gte: 1n } }, { id: { $lte: 10n } }] }, entry({ id: 5 }), schema, m)).toBe(true)
    expect(partitionMightMatch({ $and: [{ id: { $gte: 1n } }, { id: { $lte: 10n } }] }, entry({ id: 20 }), schema, m)).toBe(false)
  })

  it('$or prunes only if every disjunct rules out the file', () => {
    expect(partitionMightMatch({ $or: [{ id: { $eq: 1n } }, { id: { $eq: 5n } }] }, entry({ id: 5 }), schema, m)).toBe(true)
    expect(partitionMightMatch({ $or: [{ id: { $eq: 1n } }, { id: { $eq: 2n } }] }, entry({ id: 5 }), schema, m)).toBe(false)
  })

  it('keeps the file for predicates on non-partition columns', () => {
    expect(partitionMightMatch({ price: { $gt: 0 } }, entry({ id: 5 }), schema, m)).toBe(true)
    // ... even combined with a prunable predicate via $or
    expect(partitionMightMatch({ $or: [{ id: { $eq: 1n } }, { price: { $gt: 0 } }] }, entry({ id: 5 }), schema, m)).toBe(true)
  })

  it('keeps the file for an unpartitioned spec', () => {
    expect(partitionMightMatch({ id: { $eq: 5n } }, entry({}), schema, meta([]))).toBe(true)
  })

  it('keeps the file when the entry uses an unknown spec id', () => {
    expect(partitionMightMatch({ id: { $eq: 5n } }, entry({ id: 6 }, 9), schema, m)).toBe(true)
  })
})
