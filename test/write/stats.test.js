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

  it('truncates string lower/upper bounds to 16 code points', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'short', required: false, type: 'string' },
        { id: 2, name: 'long', required: false, type: 'string' },
      ],
    }
    const records = [
      { short: 'apple', long: 'aaaaaaaaaaaaaaaaXXXX' }, // 20 chars
      { short: 'banana', long: 'bbbbbbbbbbbbbbbbYYYY' }, // 20 chars
    ]

    const stats = computeColumnStats(records, schema)

    // Short strings are unchanged.
    expect(new TextDecoder().decode(stats.lower_bounds[1])).toBe('apple')
    expect(new TextDecoder().decode(stats.upper_bounds[1])).toBe('banana')

    // Long lower bound is truncated to first 16 code points.
    expect(new TextDecoder().decode(stats.lower_bounds[2])).toBe('aaaaaaaaaaaaaaaa')
    // Long upper bound is truncated to 16 with last code point incremented.
    expect(new TextDecoder().decode(stats.upper_bounds[2])).toBe('bbbbbbbbbbbbbbbc')
  })

  it('truncates binary lower/upper bounds to 16 bytes and increments', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'blob', required: false, type: 'binary' },
      ],
    }
    const lo = new Uint8Array(20)
    lo.fill(0x10)
    const hi = new Uint8Array(20)
    hi.fill(0x20)
    const records = [{ blob: lo }, { blob: hi }]

    const stats = computeColumnStats(records, schema)

    expect(stats.lower_bounds[1]).toEqual(new Uint8Array(16).fill(0x10))
    const expectedUpper = new Uint8Array(16).fill(0x20)
    expectedUpper[15] = 0x21
    expect(stats.upper_bounds[1]).toEqual(expectedUpper)
  })

  it('omits binary upper bound when truncation would overflow', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'blob', required: false, type: 'binary' },
      ],
    }
    const overflow = new Uint8Array(20)
    overflow.fill(0xFF)
    const records = [{ blob: overflow }]

    const stats = computeColumnStats(records, schema)
    expect(stats.lower_bounds[1]).toEqual(new Uint8Array(16).fill(0xFF))
    expect(stats.upper_bounds[1]).toBeUndefined()
  })

  it('truncates strings at code-point boundaries (no split surrogate pairs)', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 's', required: false, type: 'string' },
      ],
    }
    // Each emoji is 2 UTF-16 units but 1 code point.
    const long = '🍎'.repeat(20)
    const records = [{ s: long }]

    const stats = computeColumnStats(records, schema)
    const lower = new TextDecoder().decode(stats.lower_bounds[1])
    expect(Array.from(lower).length).toBe(16)
    expect(lower).toBe('🍎'.repeat(16))
  })

  it('omits bounds for v3 variant columns', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'payload', required: false, type: 'variant' },
      ],
    }
    const records = [{ payload: { a: 1 } }, { payload: null }]

    const stats = computeColumnStats(records, schema)

    expect(stats.value_counts).toEqual({ 1: 2n })
    expect(stats.null_value_counts).toEqual({ 1: 1n })
    expect(stats.lower_bounds).toEqual({})
    expect(stats.upper_bounds).toEqual({})
  })

  it('routes geometry/geography columns through computeGeoBounds', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'geom', required: false, type: 'geometry(srid:4326)' },
      ],
    }
    const records = [
      { geom: { type: 'Point', coordinates: [30, 10] } },
      { geom: null },
    ]

    const stats = computeColumnStats(records, schema)

    expect(stats.value_counts).toEqual({ 1: 2n })
    expect(stats.null_value_counts).toEqual({ 1: 1n })
    expect(stats.lower_bounds[1].length).toBe(16)
    expect(stats.upper_bounds[1].length).toBe(16)
  })

  it('serializes date bounds as days since epoch (int32 LE)', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'd', required: false, type: 'date' },
        { id: 2, name: 'd2', required: false, type: 'date' },
      ],
    }
    const records = [
      // 2024-01-01 = day 19723; 2024-01-10 = day 19732
      { d: new Date('2024-01-10T00:00:00Z'), d2: 19723 },
      { d: new Date('2024-01-01T00:00:00Z'), d2: 19732 },
    ]

    const stats = computeColumnStats(records, schema)

    expect(stats.lower_bounds[1].length).toBe(4)
    expect(stats.upper_bounds[1].length).toBe(4)
    const lo1 = new DataView(stats.lower_bounds[1].buffer).getInt32(0, true)
    const hi1 = new DataView(stats.upper_bounds[1].buffer).getInt32(0, true)
    expect(lo1).toBe(19723)
    expect(hi1).toBe(19732)

    const lo2 = new DataView(stats.lower_bounds[2].buffer).getInt32(0, true)
    const hi2 = new DataView(stats.upper_bounds[2].buffer).getInt32(0, true)
    expect(lo2).toBe(19723)
    expect(hi2).toBe(19732)
  })

  it('serializes time bounds as microseconds since midnight (int64 LE)', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 't', required: false, type: 'time' },
      ],
    }
    // 01:00:00 = 3_600_000_000 micros; 02:00:00 = 7_200_000_000 micros
    const records = [
      { t: 7200000000n },
      { t: 3600000000 },
    ]

    const stats = computeColumnStats(records, schema)

    expect(stats.lower_bounds[1].length).toBe(8)
    expect(stats.upper_bounds[1].length).toBe(8)
    const lo = new DataView(stats.lower_bounds[1].buffer).getBigInt64(0, true)
    const hi = new DataView(stats.upper_bounds[1].buffer).getBigInt64(0, true)
    expect(lo).toBe(3600000000n)
    expect(hi).toBe(7200000000n)
  })

  it('substitutes write-default for missing values in stats', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'tag', required: false, type: 'string', 'write-default': 'unknown' },
      ],
    }
    const records = [{ tag: 'red' }, {}, { tag: null }]

    const stats = computeColumnStats(records, schema)

    expect(stats.value_counts[1]).toBe(3n)
    expect(stats.null_value_counts[1]).toBe(1n)
    expect(new TextDecoder().decode(stats.lower_bounds[1])).toBe('red')
    expect(new TextDecoder().decode(stats.upper_bounds[1])).toBe('unknown')
  })
})
