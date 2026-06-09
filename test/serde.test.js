import { describe, expect, it } from 'vitest'
import { deserializeValue, serializeValue } from '../src/write/serde.js'

/**
 * @import {IcebergType} from '../src/types.js'
 */

/**
 * @param {any} value
 * @param {IcebergType} type
 * @returns {any}
 */
function roundtrip(value, type) {
  const bytes = serializeValue(value, type)
  expect(bytes).toBeInstanceOf(Uint8Array)
  return deserializeValue(/** @type {Uint8Array} */ (bytes), type)
}

describe('serde round-trip', () => {
  it('boolean', () => {
    expect(roundtrip(true, 'boolean')).toBe(true)
    expect(roundtrip(false, 'boolean')).toBe(false)
  })

  it('int', () => {
    expect(roundtrip(0, 'int')).toBe(0)
    expect(roundtrip(42, 'int')).toBe(42)
    expect(roundtrip(-7, 'int')).toBe(-7)
    expect(roundtrip(2147483647, 'int')).toBe(2147483647)
    expect(roundtrip(-2147483648, 'int')).toBe(-2147483648)
  })

  it('long returns bigint', () => {
    expect(roundtrip(0n, 'long')).toBe(0n)
    expect(roundtrip(123n, 'long')).toBe(123n)
    expect(roundtrip(-123n, 'long')).toBe(-123n)
    expect(roundtrip(9223372036854775807n, 'long')).toBe(9223372036854775807n)
    expect(roundtrip(-9223372036854775808n, 'long')).toBe(-9223372036854775808n)
  })

  it('float and double, with -0/+0', () => {
    expect(roundtrip(1.5, 'float')).toBeCloseTo(1.5, 5)
    expect(roundtrip(1.5, 'double')).toBe(1.5)
    expect(roundtrip(-1.25, 'double')).toBe(-1.25)
    // -0 preserves sign through IEEE bits
    expect(Object.is(roundtrip(-0, 'double'), -0)).toBe(true)
    expect(Object.is(roundtrip(0, 'double'), 0)).toBe(true)
  })

  it('date as days-since-epoch number', () => {
    expect(roundtrip(0, 'date')).toBe(0)
    expect(roundtrip(19000, 'date')).toBe(19000)
    expect(roundtrip(-1, 'date')).toBe(-1)
  })

  it('timestamp / timestamptz as micros bigint', () => {
    expect(roundtrip(1700000000000000n, 'timestamp')).toBe(1700000000000000n)
    expect(roundtrip(1700000000000000n, 'timestamptz')).toBe(1700000000000000n)
  })

  it('timestamp_ns as nanos bigint', () => {
    expect(roundtrip(1700000000000000000n, 'timestamp_ns')).toBe(1700000000000000000n)
  })

  it('time as micros bigint', () => {
    expect(roundtrip(3600000000n, 'time')).toBe(3600000000n)
  })

  it('string', () => {
    expect(roundtrip('', 'string')).toBe('')
    expect(roundtrip('hello', 'string')).toBe('hello')
    expect(roundtrip('💧 unicode', 'string')).toBe('💧 unicode')
  })

  it('binary returns the same bytes', () => {
    const v = new Uint8Array([1, 2, 3, 255])
    expect(roundtrip(v, 'binary')).toEqual(v)
  })

  it('fixed returns the same bytes', () => {
    const v = new Uint8Array([9, 8, 7, 6])
    expect(roundtrip(v, 'fixed[4]')).toEqual(v)
  })

  it('uuid round-trips string to bytes', () => {
    const s = '12345678-1234-5678-1234-567812345678'
    const bytes = serializeValue(s, 'uuid')
    expect(bytes).toBeInstanceOf(Uint8Array)
    expect(deserializeValue(/** @type {Uint8Array} */ (bytes), 'uuid')).toEqual(bytes)
  })

  it('decimal round-trips numerically', () => {
    expect(roundtrip(12.34, 'decimal(10, 2)')).toBeCloseTo(12.34, 6)
    expect(roundtrip(-5.5, 'decimal(10, 2)')).toBeCloseTo(-5.5, 6)
    expect(roundtrip(0, 'decimal(10, 2)')).toBe(0)
    expect(roundtrip(999.99, 'decimal(8, 2)')).toBeCloseTo(999.99, 6)
  })
})

describe('serde truncated bounds decode to stored prefix', () => {
  it('string decodes to the (possibly truncated) stored bytes', () => {
    // serializeValue does NOT truncate; truncation is applied upstream in
    // stats.js. A pre-truncated 16-char prefix decodes back to that prefix.
    const prefix = 'abcdefghijklmnop' // 16 chars
    expect(roundtrip(prefix, 'string')).toBe(prefix)
  })
})

describe('serde decode failures return undefined', () => {
  it('non-Uint8Array input', () => {
    // @ts-expect-error intentional bad input
    expect(deserializeValue('not bytes', 'int')).toBeUndefined()
  })
  it('unsupported type', () => {
    expect(deserializeValue(new Uint8Array([1]), 'variant')).toBeUndefined()
  })
  it('truncated numeric buffer', () => {
    // 2 bytes is too short for an int32 read -> DataView throws -> undefined
    expect(deserializeValue(new Uint8Array([1, 2]), 'int')).toBeUndefined()
  })
})
