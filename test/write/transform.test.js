import { describe, expect, it } from 'vitest'
import { applyTransform, murmur3_32, parseTransform, transformResultType } from '../../src/write/transform.js'

describe('parseTransform', () => {
  it('parses simple transforms', () => {
    expect(parseTransform('identity')).toEqual({ kind: 'identity' })
    expect(parseTransform('void')).toEqual({ kind: 'void' })
    expect(parseTransform('year')).toEqual({ kind: 'year' })
    expect(parseTransform('month')).toEqual({ kind: 'month' })
    expect(parseTransform('day')).toEqual({ kind: 'day' })
    expect(parseTransform('hour')).toEqual({ kind: 'hour' })
  })

  it('parses parameterised transforms', () => {
    expect(parseTransform('bucket[16]')).toEqual({ kind: 'bucket', n: 16 })
    expect(parseTransform('truncate[5]')).toEqual({ kind: 'truncate', w: 5 })
  })

  it('throws on unknown transforms', () => {
    expect(() => parseTransform('weeknumber')).toThrow(/unsupported partition transform/)
    expect(() => parseTransform('bucket[]')).toThrow(/unsupported partition transform/)
  })
})

describe('transformResultType', () => {
  it('returns the source type for identity and truncate, and int for void', () => {
    expect(transformResultType('identity', 'long')).toBe('long')
    expect(transformResultType('void', 'string')).toBe('int')
    expect(transformResultType('truncate[3]', 'string')).toBe('string')
  })

  it('returns int for bucket and date-extract transforms', () => {
    expect(transformResultType('bucket[16]', 'string')).toBe('int')
    expect(transformResultType('year', 'timestamptz')).toBe('int')
    expect(transformResultType('month', 'date')).toBe('int')
    expect(transformResultType('day', 'timestamp')).toBe('int')
    expect(transformResultType('hour', 'timestamp')).toBe('int')
  })
})

describe('murmur3_32', () => {
  // Reference vectors from the Murmur3 spec.
  it('matches reference vectors', () => {
    const enc = new TextEncoder()
    expect(murmur3_32(new Uint8Array(0), 0)).toBe(0)
    expect(murmur3_32(enc.encode(''), 1)).toBe(0x514E28B7)
    expect(murmur3_32(enc.encode('a'), 0x9747b28c)).toBe(0x7FA09EA6)
    expect(murmur3_32(enc.encode('Hello, world!'), 0x9747b28c)).toBe(0x24884CBA)
  })
})

describe('applyTransform', () => {
  it('passes nulls through', () => {
    expect(applyTransform('bucket[16]', null, 'int')).toBeNull()
    expect(applyTransform('day', null, 'timestamp')).toBeNull()
  })

  // Hash values come from the Iceberg spec (Appendix B, "32-bit hash
  // requirements"). The bucket result is then `(hash & 0x7FFFFFFF) % N`.
  it('matches bucket reference vectors from the Iceberg spec', () => {
    // hashLong(34) = 2017239379 → 79
    expect(applyTransform('bucket[100]', 34, 'int')).toBe(79)
    expect(applyTransform('bucket[100]', 34n, 'long')).toBe(79)
    // hashBytes("iceberg") = 1210000089
    expect(applyTransform('bucket[100]', 'iceberg', 'string')).toBe(89)
    // hashBytes(00 01 02 03) ends in 41
    expect(applyTransform('bucket[100]', new Uint8Array([0, 1, 2, 3]), 'binary')).toBe(41)
    // hashLong(17486) = -653330422 → bucket 26 (2017-11-16 = day 17486)
    expect(applyTransform('bucket[100]', 17486, 'date')).toBe(26)
    // Same date passed as a JS Date hashes identically
    expect(applyTransform('bucket[100]', new Date('2017-11-16T00:00:00Z'), 'date')).toBe(26)
  })

  it('hashes int and long identically for the same value', () => {
    for (const v of [0, 1, -1, 100, 1234567]) {
      expect(applyTransform('bucket[16]', v, 'int'))
        .toBe(applyTransform('bucket[16]', BigInt(v), 'long'))
    }
  })

  it('treats Date and bigint micros identically for timestamp bucket', () => {
    const ts = new Date('2017-11-16T22:31:08Z')
    const micros = BigInt(ts.getTime()) * 1000n
    expect(applyTransform('bucket[100]', ts, 'timestamp'))
      .toBe(applyTransform('bucket[100]', micros, 'timestamp'))
  })

  it('hashes nanosecond timestamps at microsecond precision', () => {
    const ts = new Date('2017-11-16T22:31:08Z')
    const nanos = 1510871468000001001n
    expect(applyTransform('bucket[100]', ts, 'timestamp_ns')).toBe(7)
    expect(applyTransform('bucket[100]', nanos, 'timestamp_ns')).toBe(38)
    expect(applyTransform('bucket[100]', nanos, 'timestamptz_ns')).toBe(38)
  })

  it('truncates ints toward negative infinity', () => {
    expect(applyTransform('truncate[10]', 1, 'int')).toBe(0)
    expect(applyTransform('truncate[10]', 9, 'int')).toBe(0)
    expect(applyTransform('truncate[10]', 10, 'int')).toBe(10)
    expect(applyTransform('truncate[10]', -1, 'int')).toBe(-10)
    expect(applyTransform('truncate[10]', -11, 'int')).toBe(-20)
  })

  it('truncates longs toward negative infinity', () => {
    expect(applyTransform('truncate[10]', 1n, 'long')).toBe(0n)
    expect(applyTransform('truncate[10]', -1n, 'long')).toBe(-10n)
  })

  it('truncates strings by code points', () => {
    expect(applyTransform('truncate[3]', 'iceberg', 'string')).toBe('ice')
    expect(applyTransform('truncate[10]', 'ice', 'string')).toBe('ice')
    // surrogate pair (😀 = U+1F600) counts as one code point
    expect(applyTransform('truncate[2]', 'a😀b', 'string')).toBe('a😀')
  })

  it('truncates binary by leading bytes', () => {
    const b = applyTransform('truncate[3]', new Uint8Array([1, 2, 3, 4, 5]), 'binary')
    expect(b).toEqual(new Uint8Array([1, 2, 3]))
  })

  it('truncates fixed[N] by leading bytes', () => {
    const b = applyTransform('truncate[3]', new Uint8Array([1, 2, 3, 4, 5]), 'fixed[5]')
    expect(b).toEqual(new Uint8Array([1, 2, 3]))
  })

  it('buckets fixed[N] as raw bytes (matches binary)', () => {
    const v = new Uint8Array([0, 1, 2, 3])
    expect(applyTransform('bucket[100]', v, 'fixed[4]'))
      .toBe(applyTransform('bucket[100]', v, 'binary'))
  })

  it('truncates decimals toward negative infinity at the unscaled scale', () => {
    // unscaled 1234, W=10 → 1230 → 12.30
    expect(applyTransform('truncate[10]', 12.34, 'decimal(9,2)')).toBe(12.30)
    // unscaled 50, W=50 → 50
    expect(applyTransform('truncate[50]', 0.50, 'decimal(9,2)')).toBe(0.50)
    // unscaled -123, W=50 → -150 → -1.50
    expect(applyTransform('truncate[50]', -1.23, 'decimal(9,2)')).toBe(-1.50)
    // 0 → 0
    expect(applyTransform('truncate[10]', 0, 'decimal(9,2)')).toBe(0)
  })

  it('hashes a decimal as the unscaled value\'s minimum two\'s-complement bytes', () => {
    // hashBytes(0x04 0xD2) = hash of unscaled 1234 (decimal 12.34, scale 2)
    const a = applyTransform('bucket[100]', 12.34, 'decimal(9,2)')
    const b = applyTransform('bucket[100]', new Uint8Array([0x04, 0xD2]), 'binary')
    expect(a).toBe(b)
  })

  it('extracts year/month/day/hour from a timestamp', () => {
    const ts = new Date('2017-11-16T22:31:08Z')
    expect(applyTransform('year', ts, 'timestamp')).toBe(47)
    expect(applyTransform('month', ts, 'timestamp')).toBe(47 * 12 + 10)
    expect(applyTransform('day', ts, 'timestamp')).toBe(17486)
    expect(applyTransform('hour', ts, 'timestamp')).toBe(Math.floor(ts.getTime() / 3600000))
  })

  it('extracts year/month/day from a date stored as days since epoch', () => {
    expect(applyTransform('year', 17486, 'date')).toBe(47)
    expect(applyTransform('month', 17486, 'date')).toBe(47 * 12 + 10)
    expect(applyTransform('day', 17486, 'date')).toBe(17486)
  })

  it('handles negative dates (pre-1970)', () => {
    // 1969-12-31 is day -1
    expect(applyTransform('day', -1, 'date')).toBe(-1)
    expect(applyTransform('year', -1, 'date')).toBe(-1)
  })

  it('throws on unsupported source types', () => {
    expect(() => applyTransform('bucket[16]', 1.5, 'double'))
      .toThrow(/bucket transform: unsupported source type double/)
    expect(() => applyTransform('truncate[3]', 1.5, 'double'))
      .toThrow(/truncate transform: unsupported source type double/)
    expect(() => applyTransform('day', 'oops', 'string'))
      .toThrow(/date\/time transform: unsupported source type string/)
  })
})
