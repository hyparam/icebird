import { describe, expect, it } from 'vitest'
import { computeColumnStats } from '../../src/write/stats.js'

/**
 * @import {Schema} from '../../src/types.js'
 */

describe('computeColumnStats', () => {
  it('counts nulls and bounds for primitive columns', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'string' },
      ],
    }
    const records = [
      { id: 1n, name: 'banana' },
      { id: 5n, name: null },
      { id: 3n, name: 'apple' },
    ]

    const stats = computeColumnStats(records, schema)

    expect(stats.value_counts).toEqual({ 1: 3n, 2: 3n })
    expect(stats.null_value_counts).toEqual({ 1: 0n, 2: 1n })
    expect(stats.nan_value_counts).toEqual({})

    // long: 8-byte little-endian
    const lo = new DataView(stats.lower_bounds[1].buffer).getBigInt64(0, true)
    const hi = new DataView(stats.upper_bounds[1].buffer).getBigInt64(0, true)
    expect(lo).toBe(1n)
    expect(hi).toBe(5n)

    // string: utf-8 bytes
    expect(new TextDecoder().decode(stats.lower_bounds[2])).toBe('apple')
    expect(new TextDecoder().decode(stats.upper_bounds[2])).toBe('banana')
  })

  it('counts NaNs and excludes them from bounds for floats', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 7, name: 'score', required: false, type: 'double' },
      ],
    }
    const records = [
      { score: 1.5 },
      { score: NaN },
      { score: -2.25 },
      { score: null },
    ]

    const stats = computeColumnStats(records, schema)

    expect(stats.value_counts[7]).toBe(4n)
    expect(stats.null_value_counts[7]).toBe(1n)
    expect(stats.nan_value_counts[7]).toBe(1n)

    const lo = new DataView(stats.lower_bounds[7].buffer).getFloat64(0, true)
    const hi = new DataView(stats.upper_bounds[7].buffer).getFloat64(0, true)
    expect(lo).toBe(-2.25)
    expect(hi).toBe(1.5)
  })

  it('omits bounds when all values are null', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'x', required: false, type: 'int' },
      ],
    }
    const stats = computeColumnStats([{ x: null }, { x: null }], schema)
    expect(stats.value_counts[1]).toBe(2n)
    expect(stats.null_value_counts[1]).toBe(2n)
    expect(stats.lower_bounds).toEqual({})
    expect(stats.upper_bounds).toEqual({})
  })

  it('serializes nanosecond timestamp bounds and skips unknown columns', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'ts', required: false, type: 'timestamp_ns' },
        { id: 2, name: 'placeholder', required: false, type: 'unknown' },
      ],
    }
    const records = [
      { ts: new Date('2024-01-02T00:00:00.001Z'), placeholder: 'ignored' },
      { ts: 1704153600000000000n, placeholder: 'ignored' },
    ]

    const stats = computeColumnStats(records, schema)
    const lo = new DataView(stats.lower_bounds[1].buffer).getBigInt64(0, true)
    const hi = new DataView(stats.upper_bounds[1].buffer).getBigInt64(0, true)

    expect(lo).toBe(1704153600000000000n)
    expect(hi).toBe(1704153600001000000n)
    expect(stats.value_counts[2]).toBeUndefined()
    expect(stats.null_value_counts[2]).toBeUndefined()
  })
})
