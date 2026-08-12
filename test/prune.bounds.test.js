import { describe, expect, it } from 'vitest'
import { fileMightMatch } from '../src/prune.js'
import { serializeValue } from '../src/write/serde.js'

/**
 * @import {IcebergType, ManifestEntry, Schema} from '../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
    { id: 3, name: 'ts', required: false, type: 'timestamp' },
    { id: 4, name: 'price', required: false, type: 'double' },
    { id: 5, name: 'amount', required: false, type: 'decimal(10, 2)' },
    { id: 6, name: 'd', required: false, type: 'date' },
    { id: 7, name: 'blob', required: false, type: 'binary' },
  ],
}

/**
 * Build a manifest entry whose lower/upper bounds are the Iceberg-serialized
 * [min, max] for the given fields. `shape` controls the decoded map layout:
 * 'array' mimics the real read path (Avro int-keyed map -> array of {key,value}),
 * 'object' mimics a hand-built Record<fieldId, bytes>.
 *
 * @param {Record<number, {min: any, max: any, type: IcebergType}>} bounds
 * @param {'array'|'object'} [shape]
 * @returns {ManifestEntry}
 */
function entry(bounds, shape = 'array') {
  const lowerEntries = []
  const upperEntries = []
  /** @type {Record<number, Uint8Array>} */
  const lowerObj = {}
  /** @type {Record<number, Uint8Array>} */
  const upperObj = {}
  for (const [id, { min, max, type }] of Object.entries(bounds)) {
    const lo = serializeValue(min, type)
    const hi = serializeValue(max, type)
    if (lo) { lowerEntries.push({ key: Number(id), value: lo }); lowerObj[Number(id)] = lo }
    if (hi) { upperEntries.push({ key: Number(id), value: hi }); upperObj[Number(id)] = hi }
  }
  return /** @type {ManifestEntry} */ ({
    status: 1,
    data_file: shape === 'array'
      ? { lower_bounds: lowerEntries, upper_bounds: upperEntries }
      : { lower_bounds: lowerObj, upper_bounds: upperObj },
  })
}

describe('fileMightMatch — numeric range/equality', () => {
  // id (long) in [1, 5]
  const e = entry({ 1: { min: 1n, max: 5n, type: 'long' } })

  it('range predicates skip vs keep at boundaries', () => {
    expect(fileMightMatch({ id: { $gt: 5n } }, e, schema)).toBe(false) // max 5, nothing > 5
    expect(fileMightMatch({ id: { $gte: 5n } }, e, schema)).toBe(true) // 5 qualifies
    expect(fileMightMatch({ id: { $lt: 1n } }, e, schema)).toBe(false) // min 1, nothing < 1
    expect(fileMightMatch({ id: { $lte: 1n } }, e, schema)).toBe(true)
    expect(fileMightMatch({ id: { $gt: 4n } }, e, schema)).toBe(true) // 5 > 4
    expect(fileMightMatch({ id: { $lt: 2n } }, e, schema)).toBe(true) // 1 < 2
  })

  it('equality skips out-of-range, keeps in-range', () => {
    expect(fileMightMatch({ id: { $eq: 3n } }, e, schema)).toBe(true)
    expect(fileMightMatch({ id: { $eq: 1n } }, e, schema)).toBe(true)
    expect(fileMightMatch({ id: { $eq: 5n } }, e, schema)).toBe(true)
    expect(fileMightMatch({ id: { $eq: 0n } }, e, schema)).toBe(false)
    expect(fileMightMatch({ id: { $eq: 9n } }, e, schema)).toBe(false)
  })

  it('bare value is treated as equality', () => {
    // Bare values (no operator object) are a runtime convenience not modelled by
    // hyparquet's ParquetQueryFilter type, so cast past it.
    expect(fileMightMatch(/** @type {any} */ ({ id: 9n }), e, schema)).toBe(false)
    expect(fileMightMatch(/** @type {any} */ ({ id: 3n }), e, schema)).toBe(true)
  })

  it('$in keeps if any value is in range, skips if all out', () => {
    expect(fileMightMatch({ id: { $in: [9n, 10n] } }, e, schema)).toBe(false)
    expect(fileMightMatch({ id: { $in: [3n, 9n] } }, e, schema)).toBe(true)
  })

  it('$ne / $nin never prune', () => {
    expect(fileMightMatch({ id: { $ne: 3n } }, e, schema)).toBe(true)
    expect(fileMightMatch({ id: { $nin: [3n] } }, e, schema)).toBe(true)
  })

  it('works with object-shaped bound maps too', () => {
    const eo = entry({ 1: { min: 1n, max: 5n, type: 'long' } }, 'object')
    expect(fileMightMatch({ id: { $gt: 5n } }, eo, schema)).toBe(false)
    expect(fileMightMatch({ id: { $eq: 3n } }, eo, schema)).toBe(true)
  })
})

describe('fileMightMatch — double with mixed numeric literals', () => {
  const e = entry({ 4: { min: 1.5, max: 9.99, type: 'double' } })

  it('prunes price > 0 only when out of range', () => {
    expect(fileMightMatch({ price: { $gt: 0n } }, e, schema)).toBe(true) // 9.99 > 0
    expect(fileMightMatch({ price: { $gt: 10n } }, e, schema)).toBe(false) // max 9.99
    expect(fileMightMatch({ price: { $lt: 1n } }, e, schema)).toBe(false) // min 1.5
  })

  it('prunes the all-non-positive file', () => {
    const neg = entry({ 4: { min: -5.0, max: 0.0, type: 'double' } })
    expect(fileMightMatch({ price: { $gt: 0n } }, neg, schema)).toBe(false)
  })
})

describe('fileMightMatch — timestamp with Date literal', () => {
  // ts in micros for 2022-01-01 .. 2022-06-01
  const lo = BigInt(Date.parse('2022-01-01')) * 1000n
  const hi = BigInt(Date.parse('2022-06-01')) * 1000n
  const e = entry({ 3: { min: lo, max: hi, type: 'timestamp' } })

  it('normalizes Date literal to the bound domain', () => {
    expect(fileMightMatch({ ts: { $gt: new Date('2022-07-01') } }, e, schema)).toBe(false)
    expect(fileMightMatch({ ts: { $gt: new Date('2022-03-01') } }, e, schema)).toBe(true)
    expect(fileMightMatch({ ts: { $lt: new Date('2021-01-01') } }, e, schema)).toBe(false)
  })
})

describe('fileMightMatch — date with Date literal', () => {
  // d in days-since-epoch for 2022-01-01 .. 2022-06-01
  const lo = Math.floor(Date.parse('2022-01-01') / 86400000)
  const hi = Math.floor(Date.parse('2022-06-01') / 86400000)
  const e = entry({ 6: { min: lo, max: hi, type: 'date' } })

  it('normalizes a Date literal (ms) against day-domain bounds', () => {
    expect(fileMightMatch({ d: { $gt: new Date('2022-07-01') } }, e, schema)).toBe(false)
    expect(fileMightMatch({ d: { $gt: new Date('2022-03-01') } }, e, schema)).toBe(true)
    expect(fileMightMatch({ d: { $lt: new Date('2021-01-01') } }, e, schema)).toBe(false)
    expect(fileMightMatch({ d: { $eq: new Date('2022-03-01') } }, e, schema)).toBe(true)
    expect(fileMightMatch({ d: { $eq: new Date('2023-01-01') } }, e, schema)).toBe(false)
  })

  it('also prunes with numeric (days) and ISO-string literals', () => {
    expect(fileMightMatch({ d: { $gt: hi } }, e, schema)).toBe(false) // nothing > max
    expect(fileMightMatch({ d: { $gt: '2022-07-01' } }, e, schema)).toBe(false)
    expect(fileMightMatch({ d: { $gt: '2022-03-01' } }, e, schema)).toBe(true)
  })

  it('keeps the file for an unparseable date literal (no mis-prune)', () => {
    expect(fileMightMatch({ d: { $gt: 'not-a-date' } }, e, schema)).toBe(true)
  })
})

describe('fileMightMatch — decimal', () => {
  const e = entry({ 5: { min: 10.0, max: 20.0, type: 'decimal(10, 2)' } })

  it('range prunes decimals', () => {
    expect(fileMightMatch({ amount: { $gt: 20n } }, e, schema)).toBe(false)
    expect(fileMightMatch({ amount: { $lt: 10n } }, e, schema)).toBe(false)
    expect(fileMightMatch({ amount: { $eq: 15n } }, e, schema)).toBe(true)
  })
})

describe('fileMightMatch — string bounds (byte-ordered family)', () => {
  // name (string) in ['delta', 'mango']
  const e = entry({ 2: { min: 'delta', max: 'mango', type: 'string' } })

  it('equality prunes outside the bounds and keeps inside', () => {
    expect(fileMightMatch({ name: { $eq: 'apple' } }, e, schema)).toBe(false)
    expect(fileMightMatch({ name: { $eq: 'zebra' } }, e, schema)).toBe(false)
    expect(fileMightMatch({ name: { $eq: 'fig' } }, e, schema)).toBe(true)
    expect(fileMightMatch({ name: { $eq: 'delta' } }, e, schema)).toBe(true)
    expect(fileMightMatch({ name: { $eq: 'mango' } }, e, schema)).toBe(true)
  })

  it('range predicates skip vs keep at boundaries', () => {
    expect(fileMightMatch({ name: { $gt: 'mango' } }, e, schema)).toBe(false)
    expect(fileMightMatch({ name: { $gte: 'mango' } }, e, schema)).toBe(true)
    expect(fileMightMatch({ name: { $lt: 'delta' } }, e, schema)).toBe(false)
    expect(fileMightMatch({ name: { $lte: 'delta' } }, e, schema)).toBe(true)
  })

  it('$in prunes only when every value is out of range', () => {
    expect(fileMightMatch({ name: { $in: ['apple', 'zebra'] } }, e, schema)).toBe(false)
    expect(fileMightMatch({ name: { $in: ['apple', 'fig'] } }, e, schema)).toBe(true)
  })

  it('$ne keeps even on single-valued bounds: undecidable under truncation', () => {
    const single = entry({ 2: { min: 'fig', max: 'fig', type: 'string' } })
    expect(fileMightMatch({ name: { $ne: 'fig' } }, single, schema)).toBe(true)
  })

  it('truncated bounds act as outer bounds', () => {
    // Writer truncation widens, never narrows: the stored lower is a prefix of
    // the true minimum ('aaaa' bounds a min of 'aaaaq...'), the stored upper
    // has its last unit incremented ('ab' bounds a max of 'aazz...'). Values
    // outside the widened range prune; values in the slack the widening opened
    // keep.
    const t = entry({ 2: { min: 'aaaa', max: 'ab', type: 'string' } })
    expect(fileMightMatch({ name: { $eq: 'a' } }, t, schema)).toBe(false)
    expect(fileMightMatch({ name: { $eq: 'ac' } }, t, schema)).toBe(false)
    expect(fileMightMatch({ name: { $eq: 'aaz' } }, t, schema)).toBe(true)
    expect(fileMightMatch({ name: { $eq: 'aaaa' } }, t, schema)).toBe(true)
  })

  it('orders by code point (UTF-8 byte order), not UTF-16 code units', () => {
    // U+E000 sorts below U+10000 in code point/UTF-8 order but above its
    // surrogate pair in UTF-16 unit order. A file bounded at ['a', U+E000]
    // cannot contain U+10000; UTF-16 ordering would wrongly keep it.
    const u = entry({ 2: { min: 'a', max: '\uE000', type: 'string' } })
    expect(fileMightMatch({ name: { $eq: '\u{10000}' } }, u, schema)).toBe(false)
    expect(fileMightMatch({ name: { $gt: '\uE000' } }, u, schema)).toBe(false)
    expect(fileMightMatch({ name: { $eq: '\uE000' } }, u, schema)).toBe(true)
  })

  it('a mismatched literal type keeps the file', () => {
    expect(fileMightMatch({ name: { $eq: 42 } }, e, schema)).toBe(true)
    expect(fileMightMatch({ name: { $gt: 42 } }, e, schema)).toBe(true)
  })

  it('binary bounds prune bytewise', () => {
    const b = entry({ 7: { min: new Uint8Array([2, 0]), max: new Uint8Array([5]), type: 'binary' } })
    expect(fileMightMatch({ blob: { $eq: new Uint8Array([1]) } }, b, schema)).toBe(false)
    expect(fileMightMatch({ blob: { $eq: new Uint8Array([9]) } }, b, schema)).toBe(false)
    expect(fileMightMatch({ blob: { $eq: new Uint8Array([3]) } }, b, schema)).toBe(true)
  })
})

describe('fileMightMatch — conservative cases (always keep)', () => {
  it('missing bounds for the predicated column keeps the file', () => {
    const e = entry({ 1: { min: 1n, max: 5n, type: 'long' } })
    // predicate on price, which has no bounds in this entry
    expect(fileMightMatch({ price: { $gt: 1000n } }, e, schema)).toBe(true)
  })

  it('no bounds at all keeps the file', () => {
    const e = /** @type {ManifestEntry} */ ({ status: 1, data_file: {} })
    expect(fileMightMatch({ id: { $gt: 1000n } }, e, schema)).toBe(true)
  })

  it('unknown column keeps the file', () => {
    const e = entry({ 1: { min: 1n, max: 5n, type: 'long' } })
    expect(fileMightMatch({ nope: { $eq: 1n } }, e, schema)).toBe(true)
  })
})

describe('fileMightMatch — boolean combinators', () => {
  const e = entry({ 1: { min: 1n, max: 5n, type: 'long' } })

  it('$and rules out the file if any branch rules it out', () => {
    expect(fileMightMatch({ $and: [{ id: { $gte: 1n } }, { id: { $gt: 5n } }] }, e, schema)).toBe(false)
    expect(fileMightMatch({ $and: [{ id: { $gte: 1n } }, { id: { $lte: 5n } }] }, e, schema)).toBe(true)
  })

  it('$or keeps the file if any branch can match', () => {
    expect(fileMightMatch({ $or: [{ id: { $gt: 5n } }, { id: { $eq: 3n } }] }, e, schema)).toBe(true)
    expect(fileMightMatch({ $or: [{ id: { $gt: 5n } }, { id: { $lt: 1n } }] }, e, schema)).toBe(false)
  })

  it('$nor never prunes', () => {
    expect(fileMightMatch({ $nor: [{ id: { $eq: 3n } }] }, e, schema)).toBe(true)
  })
})
