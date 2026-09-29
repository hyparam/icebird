import { describe, expect, it } from 'vitest'
import { collect, fileCatalog, icebergAppend, icebergCreateTable, icebergManifests, icebergMetadata, icebergQuery } from '../../src/index.js'
import { decimalToFixedBytes } from '../../src/write/conversions.js'
import { groupByPartition } from '../../src/write/partition.js'
import { twosComplementBigEndianToBigInt } from '../../src/write/serde.js'
import { buildSortComparator } from '../../src/write/sort.js'
import { computeColumnStats, computeFieldSummary } from '../../src/write/stats.js'
import { applyTransform } from '../../src/write/transform.js'
import { memResolver } from '../helpers.js'

/** @import {Schema, PartitionSpec} from '../../src/types.js' */

/** @type {Schema} */
const schema = {
  type: 'struct', 'schema-id': 0,
  fields: [{ id: 1, name: 'amount', required: false, type: 'decimal(18,2)' }],
}

describe('decimal write inputs', () => {
  it.each([
    [29n, 50n],
    [0.29, 0.5],
    [29n, 0.5],
    [-29n, -0.5],
  ])('keeps filtered reads consistent with full scans for %s, %s', async (a, b) => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister })
    const tableUrl = 'mem://warehouse/decimals'
    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ amount: a }, { amount: b }] })
    const tables = { t: tableUrl }
    const rows = await collect(await icebergQuery({ catalog, tables, query: 'SELECT * FROM t' }))
    const expected = [a, b].map(v => typeof v === 'bigint' ? Number(v) / 100 : v)
    expect(rows.map(row => row.amount)).toEqual(expected)
    for (const amount of expected) {
      const filtered = await collect(await icebergQuery({ catalog, tables, query: `SELECT * FROM t WHERE amount = ${amount}` }))
      expect(filtered).toEqual(rows.filter(row => row.amount === amount))
    }
    const metadata = await icebergMetadata({ tableUrl, resolver, lister })
    const manifests = await icebergManifests({ metadata, resolver })
    const file = manifests[0].entries[0].data_file
    // Assert the actual stored bytes, independently of the decimal serializer.
    expect(file.lower_bounds).toEqual([{ key: 1, value: new Uint8Array([Math.round(Math.min(...expected) * 100) & 255]) }])
    expect(file.upper_bounds).toEqual([{ key: 1, value: new Uint8Array([Math.round(Math.max(...expected) * 100) & 255]) }])
  })

  it.each(['identity', 'truncate[10]', 'bucket[100]'])('round-trips mixed inputs partitioned by %s', async transform => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister })
    const tableUrl = 'mem://warehouse/partitioned'
    await icebergCreateTable({
      catalog, tableUrl, schema,
      partitionSpec: {
        'spec-id': 0,
        fields: [{ 'source-id': 1, 'field-id': 1000, name: 'amount_part', transform }],
      },
    })
    await icebergAppend({ catalog, tableUrl, records: [{ amount: 29n }, { amount: 0.29 }, { amount: -29n }] })
    const tables = { t: tableUrl }
    const rows = await collect(await icebergQuery({ catalog, tables, query: 'SELECT * FROM t ORDER BY amount' }))
    expect(rows).toEqual([{ amount: -0.29 }, { amount: 0.29 }, { amount: 0.29 }])
    for (const amount of [-0.29, 0.29]) {
      const filtered = await collect(await icebergQuery({ catalog, tables, query: `SELECT * FROM t WHERE amount = ${amount}` }))
      expect(filtered).toEqual(rows.filter(row => row.amount === amount))
    }
  })

  it('compares mixed inputs consistently for sorting and partition summaries', () => {
    const values = [29n, 0.5, -29n, -0.5]
    const comparator = buildSortComparator({
      'order-id': 1,
      fields: [{ 'source-id': 1, transform: 'identity', direction: 'asc', 'null-order': 'nulls-first' }],
    }, schema)
    expect(values.map(amount => ({ amount })).sort(comparator).map(row => row.amount))
      .toEqual([-0.5, -29n, 29n, 0.5])
    const summary = computeFieldSummary(values, 'decimal(18,2)')
    expect(summary.lower_bound).toEqual(new Uint8Array([206]))
    expect(summary.upper_bound).toEqual(new Uint8Array([50]))
  })

  it('groups equivalent decimal inputs into the same identity partition', () => {
    /** @type {PartitionSpec} */
    const spec = {
      'spec-id': 0,
      fields: [{ 'source-id': 1, 'field-id': 1000, name: 'amount', transform: 'identity' }],
    }
    const groups = groupByPartition([{ amount: 29n }, { amount: 0.29 }, { amount: 0.5 }], schema, spec)
    expect(groups.map(g => g.records.length)).toEqual([2, 1])
  })

  it('uses unscaled bigints for partition encoding and transforms', () => {
    for (const value of [1234n, -1234n]) {
      expect(decimalToFixedBytes(value, 9, 2, 'decimal'))
        .toEqual(decimalToFixedBytes(Number(value) / 100, 9, 2, 'decimal'))
      expect(applyTransform('bucket[100]', value, 'decimal(9,2)'))
        .toBe(applyTransform('bucket[100]', Number(value) / 100, 'decimal(9,2)'))
    }
    expect(applyTransform('truncate[10]', 1234n, 'decimal(9,2)')).toBe(1230n)
    expect(applyTransform('truncate[10]', -1234n, 'decimal(9,2)')).toBe(-1240n)
  })

  it('preserves wide decimals in bounds, partition encoding, and truncation', () => {
    const value = 12345678901234567890123456789012345678n
    const type = 'decimal(38,10)'
    const records = [{ amount: value }, { amount: -value }]
    const stats = computeColumnStats(records, { ...schema, fields: [{ ...schema.fields[0], type }] })
    expect(twosComplementBigEndianToBigInt(stats.lower_bounds[1])).toBe(-value)
    expect(twosComplementBigEndianToBigInt(stats.upper_bounds[1])).toBe(value)
    expect(twosComplementBigEndianToBigInt(decimalToFixedBytes(value, 38, 10, 'decimal'))).toBe(value)
    expect(applyTransform('truncate[10]', value, type)).toBe(12345678901234567890123456789012345670n)
    expect(applyTransform('truncate[10]', -value, type)).toBe(-12345678901234567890123456789012345680n)
  })
})
