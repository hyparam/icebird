import { describe, expect, it, vi } from 'vitest'
import { buildSortComparator } from '../../src/write/sort.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergManifests, splitManifestEntries } from '../../src/manifest.js'
import { icebergRead } from '../../src/read.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema, SortOrder, TableMetadata} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'v', required: false, type: 'int' },
    { id: 3, name: 'name', required: false, type: 'string' },
  ],
}

describe('buildSortComparator', () => {
  it('returns undefined for an empty sort order', () => {
    expect(buildSortComparator({ 'order-id': 0, fields: [] }, schema)).toBeUndefined()
    expect(buildSortComparator(undefined, schema)).toBeUndefined()
  })

  it('sorts ascending by a single field', () => {
    const cmp = buildSortComparator(
      { 'order-id': 1, fields: [{ transform: 'identity', 'source-id': 2, direction: 'asc', 'null-order': 'nulls-last' }] },
      schema
    )
    const rows = [{ v: 3 }, { v: 1 }, { v: 2 }]
    expect([...rows].sort(cmp).map(r => r.v)).toEqual([1, 2, 3])
  })

  it('sorts descending', () => {
    const cmp = buildSortComparator(
      { 'order-id': 1, fields: [{ transform: 'identity', 'source-id': 2, direction: 'desc', 'null-order': 'nulls-last' }] },
      schema
    )
    const rows = [{ v: 3 }, { v: 1 }, { v: 2 }]
    expect([...rows].sort(cmp).map(r => r.v)).toEqual([3, 2, 1])
  })

  it('honors null ordering independent of direction', () => {
    const rows = [{ v: 2 }, { v: null }, { v: 1 }]
    const ascFirst = buildSortComparator(
      { 'order-id': 1, fields: [{ transform: 'identity', 'source-id': 2, direction: 'asc', 'null-order': 'nulls-first' }] },
      schema
    )
    expect([...rows].sort(ascFirst).map(r => r.v)).toEqual([null, 1, 2])

    const descFirst = buildSortComparator(
      { 'order-id': 1, fields: [{ transform: 'identity', 'source-id': 2, direction: 'desc', 'null-order': 'nulls-first' }] },
      schema
    )
    expect([...rows].sort(descFirst).map(r => r.v)).toEqual([null, 2, 1])

    const ascLast = buildSortComparator(
      { 'order-id': 1, fields: [{ transform: 'identity', 'source-id': 2, direction: 'asc', 'null-order': 'nulls-last' }] },
      schema
    )
    expect([...rows].sort(ascLast).map(r => r.v)).toEqual([1, 2, null])
  })

  it('breaks ties on a second field and is stable', () => {
    const cmp = buildSortComparator(
      {
        'order-id': 1,
        fields: [
          { transform: 'identity', 'source-id': 2, direction: 'asc', 'null-order': 'nulls-last' },
          { transform: 'identity', 'source-id': 1, direction: 'desc', 'null-order': 'nulls-last' },
        ],
      },
      schema
    )
    const rows = [
      { v: 1, id: 10n }, { v: 1, id: 20n }, { v: 0, id: 5n },
    ]
    expect([...rows].sort(cmp)).toEqual([
      { v: 0, id: 5n }, { v: 1, id: 20n }, { v: 1, id: 10n },
    ])
  })

  it('sorts on a transform key (truncate)', () => {
    const cmp = buildSortComparator(
      { 'order-id': 1, fields: [{ transform: 'truncate[2]', 'source-id': 3, direction: 'asc', 'null-order': 'nulls-last' }] },
      schema
    )
    // truncate[2] keys: zo, ap, an, ap. Sorted asc: an < ap < zo; within the
    // 'ap' tie, stable input order (apple before apricot).
    const rows = [{ name: 'zoo' }, { name: 'apple' }, { name: 'ant' }, { name: 'apricot' }]
    expect([...rows].sort(cmp).map(r => r.name)).toEqual(['ant', 'apple', 'apricot', 'zoo'])
  })
})

describe('sort-on-append integration', () => {
  /**
   * @param {SortOrder} sortOrder
   * @param {number} [sortOrderId]
   * @returns {Promise<{ rows: any[], sortOrderIds: number[] }>}
   */
  async function appendAndRead(sortOrder, sortOrderId) {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://sorted'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema, sortOrder })
    const records = [
      { id: 3n, v: 30, name: 'c' },
      { id: 1n, v: 10, name: 'a' },
      { id: 2n, v: 20, name: 'b' },
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: created, records, resolver, sortOrderId })
    const committed = await fileCatalogCommit({ tableUrl, metadata: created, staged, resolver })
    const rows = await icebergRead({ tableUrl, metadata: committed, resolver })
    const manifests = await icebergManifests({ metadata: committed, resolver })
    const { dataEntries } = splitManifestEntries(manifests)
    return { rows, sortOrderIds: dataEntries.map(e => e.data_file.sort_order_id) }
  }

  it('writes records ordered by the declared sort order and records sort_order_id', async () => {
    const { rows, sortOrderIds } = await appendAndRead({
      'order-id': 1,
      fields: [{ transform: 'identity', 'source-id': 1, direction: 'desc', 'null-order': 'nulls-last' }],
    })
    expect(rows.map(r => r.id)).toEqual([3n, 2n, 1n])
    expect(sortOrderIds).toEqual([1])
  })

  it('leaves input order and sort_order_id 0 for an empty sort order', async () => {
    const { rows, sortOrderIds } = await appendAndRead({ 'order-id': 0, fields: [] })
    expect(rows.map(r => r.id)).toEqual([3n, 1n, 2n])
    expect(sortOrderIds).toEqual([0])
  })

  it('throws for an unknown sortOrderId override', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'mem://sorted-bad'
    const { resolver } = memResolver()
    const created = await icebergCreate({ tableUrl, resolver, schema })
    await expect(() => icebergStageAppend({
      tableUrl, metadata: created, records: [{ id: 1n, v: 1, name: 'a' }], resolver, sortOrderId: 99,
    })).rejects.toThrow('sort order 99 not found')
  })
})
