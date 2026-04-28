import { describe, expect, it, vi } from 'vitest'
import { fileCatalogCommit } from '../src/write/commit.js'
import { icebergCreate } from '../src/create.js'
import { icebergRead } from '../src/read.js'
import { icebergStageAppend, icebergStageSetRef } from '../src/write/stage.js'
import { memResolver } from './helpers.js'

/**
 * @import {Schema, TableMetadata} from '../src/types.js'
 */

describe('icebergCreate + icebergStageAppend + icebergRead round-trip', () => {
  it('round-trips every primitive type the writer supports', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/all-types'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'i32', required: true, type: 'int' },
        { id: 2, name: 'i64', required: true, type: 'long' },
        { id: 3, name: 'f32', required: false, type: 'float' },
        { id: 4, name: 'f64', required: false, type: 'double' },
        { id: 5, name: 'b', required: false, type: 'boolean' },
        { id: 6, name: 's', required: false, type: 'string' },
        { id: 7, name: 'bin', required: false, type: 'binary' },
        { id: 8, name: 'ts', required: false, type: 'timestamp' },
        { id: 9, name: 'tz', required: false, type: 'timestamptz' },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema })

    const ts = new Date('2024-06-15T12:34:56.789Z')
    const records = [
      {
        i32: 1, i64: 1n, f32: 0.5, f64: 1.25, b: true,
        s: 'alpha', bin: new Uint8Array([1, 2, 3]), ts, tz: ts,
      },
      {
        i32: -2, i64: -2n, f32: -0.25, f64: -3.5, b: false,
        s: 'β👋', bin: new Uint8Array([255, 0, 7]), ts, tz: ts,
      },
    ]

    const staged = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const committed = await fileCatalogCommit({ tableUrl, metadata: created, staged, resolver })

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('round-trips a fixed[N] column', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/fixed'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'sig', required: false, type: /** @type {const} */ ('fixed[4]') },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const records = [
      { id: 1n, sig: new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]) },
      { id: 2n, sig: new Uint8Array([0, 0, 0, 0]) },
      { id: 3n, sig: null },
    ]

    const staged = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const committed = await fileCatalogCommit({ tableUrl, metadata: created, staged, resolver })

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('partitions a decimal column by bucket and round-trips', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/decimal-bucket'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'price', required: true, type: 'decimal(9,2)' },
      ],
    }
    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const partitioned = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{
          'source-id': 2, 'field-id': 1000, name: 'price_bucket',
          transform: /** @type {const} */ ('bucket[4]'),
        }],
      }],
      'last-partition-id': 1000,
    }

    const records = [
      { id: 1n, price: 9.99 },
      { id: 2n, price: 12.34 },
      { id: 3n, price: -1.23 },
      { id: 4n, price: 0.5 },
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: partitioned, records, resolver })
    expect(staged.snapshot.summary['added-records']).toBe('4')

    const committed = await fileCatalogCommit({ tableUrl, metadata: partitioned, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('partitions a decimal column by truncate[W] and round-trips', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/decimal-truncate'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'price', required: true, type: 'decimal(9,2)' },
      ],
    }
    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const partitioned = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{
          'source-id': 2, 'field-id': 1000, name: 'price_trunc',
          transform: /** @type {const} */ ('truncate[100]'), // groups by whole-dollar buckets
        }],
      }],
      'last-partition-id': 1000,
    }

    const records = [
      { id: 1n, price: 12.34 }, // → 12.00
      { id: 2n, price: 12.99 }, // → 12.00 (same bucket)
      { id: 3n, price: 13.00 }, // → 13.00 (new bucket)
      { id: 4n, price: -0.5 }, // → -1.00
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: partitioned, records, resolver })
    // 3 groups: 12.00, 13.00, -1.00
    expect(staged.writtenFiles.filter(p => p.endsWith('.parquet'))).toHaveLength(3)

    const committed = await fileCatalogCommit({ tableUrl, metadata: partitioned, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('round-trips decimal columns at multiple precisions', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/decimal'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'price', required: false, type: 'decimal(10, 2)' },
        { id: 3, name: 'tiny', required: false, type: 'decimal(4, 1)' },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const records = [
      { id: 1n, price: 9.99, tiny: 0.5 },
      { id: 2n, price: -5, tiny: -99.9 },
      { id: 3n, price: 99999999.99, tiny: null },
      { id: 4n, price: 0, tiny: 12.3 },
    ]

    const staged = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const committed = await fileCatalogCommit({ tableUrl, metadata: created, staged, resolver })

    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('round-trips null values in optional fields', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/nulls'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'string' },
        { id: 3, name: 'score', required: false, type: 'double' },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const records = [
      { id: 1n, name: 'alice', score: 1.5 },
      { id: 2n, name: null, score: null },
      { id: 3n, name: 'carol', score: null },
    ]

    const staged = await icebergStageAppend({ tableUrl, metadata: created, records, resolver })
    const committed = await fileCatalogCommit({ tableUrl, metadata: created, staged, resolver })
    const read = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(read).toEqual(records)
  })

  it('reads old rows as null after a new optional column is added', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/evolve-add'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const v0 = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'string' },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema: v0 })
    const stagedA = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n, name: 'alice' }, { id: 2n, name: 'bob' }],
    })
    const afterA = await fileCatalogCommit({ tableUrl, metadata: created, staged: stagedA, resolver })

    /** @type {Schema} */
    const v1 = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...v0.fields,
        { id: 3, name: 'tag', required: false, type: 'string' },
      ],
    }
    const evolveStaged = {
      snapshot: /** @type {any} */ (null),
      requirements: [{ type: /** @type {const} */ ('assert-table-uuid'), uuid: created['table-uuid'] }],
      updates: [
        { action: /** @type {const} */ ('add-schema'), schema: v1 },
        { action: /** @type {const} */ ('set-current-schema'), 'schema-id': -1 },
      ],
      writtenFiles: [],
    }
    const evolved = await fileCatalogCommit({ tableUrl, metadata: afterA, staged: evolveStaged, resolver })
    expect(evolved['current-schema-id']).toBe(1)

    const stagedB = await icebergStageAppend({
      tableUrl, metadata: evolved, resolver,
      records: [{ id: 3n, name: 'carol', tag: 'vip' }],
    })
    const afterB = await fileCatalogCommit({ tableUrl, metadata: evolved, staged: stagedB, resolver })

    const read = await icebergRead({ tableUrl, metadata: afterB, resolver })
    expect(read).toEqual([
      { id: 1n, name: 'alice', tag: null },
      { id: 2n, name: 'bob', tag: null },
      { id: 3n, name: 'carol', tag: 'vip' },
    ])
  })

  it('reads a v3 column initial-default for rows written before the column existed', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/evolve-default'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const v0 = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema: v0, formatVersion: 3 })
    const stagedA = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n }, { id: 2n }],
    })
    const afterA = await fileCatalogCommit({ tableUrl, metadata: created, staged: stagedA, resolver })

    /** @type {Schema} */
    const v1 = {
      type: 'struct',
      'schema-id': -1,
      fields: [
        ...v0.fields,
        { id: 2, name: 'tag', required: false, type: 'string', 'initial-default': 'unknown' },
      ],
    }
    const evolveStaged = {
      snapshot: /** @type {any} */ (null),
      requirements: [{ type: /** @type {const} */ ('assert-table-uuid'), uuid: created['table-uuid'] }],
      updates: [
        { action: /** @type {const} */ ('add-schema'), schema: v1 },
        { action: /** @type {const} */ ('set-current-schema'), 'schema-id': -1 },
      ],
      writtenFiles: [],
    }
    const evolved = await fileCatalogCommit({ tableUrl, metadata: afterA, staged: evolveStaged, resolver })

    const read = await icebergRead({ tableUrl, metadata: evolved, resolver })
    // Pre-existing rows should fall back to the column's initial-default
    expect(read.map(r => ({ id: r.id, tag: r.tag }))).toEqual([
      { id: 1n, tag: 'unknown' },
      { id: 2n, tag: 'unknown' },
    ])
  })

  it('time-travels by reading an explicit prior current-snapshot-id', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/time-travel'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'string' },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const s1 = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })
    const m1 = await fileCatalogCommit({ tableUrl, metadata: created, staged: s1, resolver })
    const s2 = await icebergStageAppend({
      tableUrl, metadata: m1, resolver,
      records: [{ id: 2n, name: 'bob' }],
    })
    const m2 = await fileCatalogCommit({ tableUrl, metadata: m1, staged: s2, resolver })

    // tip: both rows
    expect(await icebergRead({ tableUrl, metadata: m2, resolver }))
      .toEqual([{ id: 1n, name: 'alice' }, { id: 2n, name: 'bob' }])

    // time-travel: read at the first snapshot by overriding current-snapshot-id
    /** @type {TableMetadata} */
    const atV1 = { ...m2, 'current-snapshot-id': s1.snapshot['snapshot-id'] }
    expect(await icebergRead({ tableUrl, metadata: atV1, resolver }))
      .toEqual([{ id: 1n, name: 'alice' }])
  })

  it('rolls back via icebergStageSetRef and reads the older table contents', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/rollback'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'string' },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const s1 = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })
    const m1 = await fileCatalogCommit({ tableUrl, metadata: created, staged: s1, resolver })
    const s2 = await icebergStageAppend({
      tableUrl, metadata: m1, resolver,
      records: [{ id: 2n, name: 'bob' }],
    })
    const m2 = await fileCatalogCommit({ tableUrl, metadata: m1, staged: s2, resolver })

    // roll main back to the first snapshot
    const rollback = icebergStageSetRef({
      metadata: m2, ref: 'main', snapshotId: s1.snapshot['snapshot-id'],
    })
    const rolled = await fileCatalogCommit({ tableUrl, metadata: m2, staged: rollback, resolver })

    expect(rolled['current-snapshot-id']).toBe(s1.snapshot['snapshot-id'])
    expect(await icebergRead({ tableUrl, metadata: rolled, resolver }))
      .toEqual([{ id: 1n, name: 'alice' }])

    // and we can still read bob's snapshot directly via metadata override
    /** @type {TableMetadata} */
    const atV2 = { ...rolled, 'current-snapshot-id': s2.snapshot['snapshot-id'] }
    expect(await icebergRead({ tableUrl, metadata: atV2, resolver }))
      .toEqual([{ id: 1n, name: 'alice' }, { id: 2n, name: 'bob' }])
  })

  it('reads row ranges across a partitioned multi-file table', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/range-partitioned'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'country', required: true, type: 'string' },
      ],
    }
    const created = await icebergCreate({ tableUrl, resolver, schema })
    /** @type {TableMetadata} */
    const partitioned = {
      ...created,
      'partition-specs': [{
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'country', transform: 'identity' }],
      }],
      'last-partition-id': 1000,
    }

    // 3 partitions => 3 files
    const records = [
      { id: 1n, country: 'us' },
      { id: 2n, country: 'fr' },
      { id: 3n, country: 'us' },
      { id: 4n, country: 'jp' },
      { id: 5n, country: 'us' },
    ]
    const staged = await icebergStageAppend({ tableUrl, metadata: partitioned, records, resolver })
    expect(staged.writtenFiles.filter(p => p.endsWith('.parquet'))).toHaveLength(3)
    const committed = await fileCatalogCommit({ tableUrl, metadata: partitioned, staged, resolver })

    const all = await icebergRead({ tableUrl, metadata: committed, resolver })
    expect(all).toHaveLength(5)

    // a range that spans across data files
    const middle = await icebergRead({ tableUrl, metadata: committed, resolver, rowStart: 1, rowEnd: 4 })
    expect(middle).toEqual(all.slice(1, 4))

    const tail = await icebergRead({ tableUrl, metadata: committed, resolver, rowStart: 3 })
    expect(tail).toEqual(all.slice(3))

    const head = await icebergRead({ tableUrl, metadata: committed, resolver, rowEnd: 2 })
    expect(head).toEqual(all.slice(0, 2))
  })

  it('preserves v3 row lineage across two appends', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/lineage-two'
    const { resolver } = memResolver()

    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
      ],
    }

    const created = await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })
    const sA = await icebergStageAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n }, { id: 2n }],
    })
    const mA = await fileCatalogCommit({ tableUrl, metadata: created, staged: sA, resolver })
    expect(mA['next-row-id']).toBe(2)

    const sB = await icebergStageAppend({
      tableUrl, metadata: mA, resolver,
      records: [{ id: 3n }, { id: 4n }, { id: 5n }],
    })
    const mB = await fileCatalogCommit({ tableUrl, metadata: mA, staged: sB, resolver })
    expect(mB['next-row-id']).toBe(5)

    const read = await icebergRead({ tableUrl, metadata: mB, resolver })
    expect(read.map(r => ({ id: r.id, _row_id: r._row_id }))).toEqual([
      { id: 1n, _row_id: 0n },
      { id: 2n, _row_id: 1n },
      { id: 3n, _row_id: 2n },
      { id: 4n, _row_id: 3n },
      { id: 5n, _row_id: 4n },
    ])
  })
})
