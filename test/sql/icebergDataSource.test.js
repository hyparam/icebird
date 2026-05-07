import { collect, executeSql } from 'squirreling'
import { describe, expect, it } from 'vitest'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { localResolver } from '../helpers.js'

/**
 * @import {ExprNode} from 'squirreling'
 * @import {Resolver} from '../../src/types.js'
 */

describe.concurrent('icebergDataSource', () => {
  const tableUrl = 's3://hyperparam-iceberg/java/bunnies'
  const resolver = localResolver('test/files')

  it('throws for missing tableUrl', async () => {
    await expect(() => icebergDataSource({ tableUrl: '' }))
      .rejects.toThrow('tableUrl is required')
  })

  it('exposes columns and a row count for a v2 table', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    expect(source.numRows).toBe(21)
    expect(source.columns).toEqual([
      'Breed Name',
      'Average Weight',
      'Fur Length',
      'Lifespan',
      'Origin Country',
      'Ear Type',
      'Temperament',
      'Popularity Rank',
    ])
  })

  it('streams all rows via scan()', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    const { rows, appliedWhere, appliedLimitOffset } = source.scan({})
    expect(appliedWhere).toBe(false)
    expect(appliedLimitOffset).toBe(true)

    const out = []
    for await (const row of rows()) {
      out.push(row.resolved)
    }
    expect(out).toHaveLength(21)
    expect(out[0]).toMatchObject({
      'Breed Name': 'Holland Lop',
      'Popularity Rank': 1n,
    })
  })

  it('pushes down LIMIT/OFFSET when there is no WHERE', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    const { rows, appliedLimitOffset } = source.scan({ limit: 3, offset: 2 })
    expect(appliedLimitOffset).toBe(true)
    const collected = []
    for await (const row of rows()) collected.push(row.resolved)
    expect(collected).toHaveLength(3)
    expect(collected[0]?.['Breed Name']).toBe('Flemish Giant')
  })

  it('does not push limit/offset down when a WHERE is supplied', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    const where = /** @type {ExprNode} */ ({ type: 'literal', value: true })
    const { appliedWhere, appliedLimitOffset } = source.scan({ where, limit: 5 })
    expect(appliedWhere).toBe(false)
    expect(appliedLimitOffset).toBe(false)
  })

  it('respects row-level deletes (v4 has 15 rows after deletes)', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v4.metadata.json',
    })
    // numRows is undefined when deletes apply (the manifest record_count
    // sum is pre-delete and would overstate the visible count).
    expect(source.numRows).toBeUndefined()
    const { rows } = source.scan({})
    let count = 0
    for await (const row of rows()) {
      expect(row).toBeDefined()
      count++
    }
    expect(count).toBe(15)
  })

  it('OFFSET produces correct rows when deletes are present', async () => {
    // Property test: whether or not the data source pushes LIMIT/OFFSET
    // down, the rows it yields for `scan({offset, limit})` must agree with
    // the corresponding slice of the full post-delete scan. LIMIT/OFFSET
    // are in post-delete coordinates; the manifest's record_count is
    // pre-delete, so naive pushdown using record_count miscounts whenever
    // a file has applicable deletes.
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v4.metadata.json',
    })

    const all = []
    for await (const row of source.scan({}).rows()) all.push(row.resolved)
    expect(all).toHaveLength(15)

    for (const offset of [0, 5, 8, 14]) {
      const plan = source.scan({ offset, limit: 2 })
      const collected = []
      for await (const row of plan.rows()) collected.push(row.resolved)
      // The data-source contract: whatever the source returns, after the
      // engine applies LIMIT/OFFSET (when not already applied), the final
      // result must equal the same slice of the full scan.
      const engineFinal = plan.appliedLimitOffset
        ? collected
        : collected.slice(offset, offset + 2)
      expect(engineFinal).toEqual(all.slice(offset, offset + 2))
    }
  })

  it('streams rows lazily without reading every data file', async () => {
    // Wrap the resolver to count parquet file reads. Iceberg metadata files
    // (.json/.avro) go through the same `reader` API but we only care about
    // counting `.parquet` opens here.
    const realResolver = localResolver('test/files')
    let parquetOpens = 0
    /** @type {Resolver} */
    const countingResolver = {
      reader(path, byteLength) {
        if (path.endsWith('.parquet')) parquetOpens++
        return realResolver.reader(path, byteLength)
      },
    }

    const source = await icebergDataSource({
      tableUrl,
      resolver: countingResolver,
      metadataFileName: 'v2.metadata.json',
    })
    const opensAfterConstruct = parquetOpens

    // Pull just one row: the generator must not pre-read all data files.
    const { rows } = source.scan({})
    const iter = rows()[Symbol.asyncIterator]()
    const first = await iter.next()
    expect(first.done).toBe(false)
    expect(first.value?.resolved?.['Breed Name']).toBe('Holland Lop')

    // After getting one row, at most one data file should have been opened
    // (parquet reads happen on iteration, not on scan() call).
    expect(parquetOpens - opensAfterConstruct).toBeLessThanOrEqual(1)
    await iter.return?.()
  })

  it('pushes column projection into the parquet read', async () => {
    // Wrap parquetReadObjects-via-resolver: hyparquet uses the resolver's
    // reader to fetch byte ranges, so we instead spy on the underlying call
    // by intercepting parquetReadObjects via column extraction. Easiest
    // observable signal: when only a subset of columns is requested, the
    // emitted rows must only contain those keys.
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    const { rows } = source.scan({ columns: ['Breed Name', 'Popularity Rank'] })
    const collected = []
    for await (const row of rows()) collected.push(row.resolved)
    expect(collected).toHaveLength(21)
    for (const r of collected) {
      expect(Object.keys(r ?? {}).sort()).toEqual(['Breed Name', 'Popularity Rank'])
    }
  })

  it('column projection still applies row-level deletes correctly', async () => {
    // Equality predicate columns must be read from parquet even when the
    // caller projects them away, otherwise the predicate can't be evaluated
    // and the delete silently fails.
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v4.metadata.json',
    })
    const { rows } = source.scan({ columns: ['Breed Name'] })
    const collected = []
    for await (const row of rows()) collected.push(row.resolved)
    expect(collected).toHaveLength(15)
    for (const r of collected) {
      expect(Object.keys(r ?? {})).toEqual(['Breed Name'])
    }
  })

  it('LIMIT pushes through deletes - source yields at most offset+limit', async () => {
    // With deletes, OFFSET cannot be pushed (record_count is pre-delete) so
    // appliedLimitOffset is false, but the source still caps emission at
    // offset+limit so the engine does not have to drain the full scan.
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v4.metadata.json',
    })
    const all = []
    for await (const row of source.scan({}).rows()) all.push(row.resolved)
    expect(all).toHaveLength(15)

    for (const [offset, limit] of [[0, 1], [0, 3], [4, 2], [10, 4]]) {
      const plan = source.scan({ offset, limit })
      expect(plan.appliedLimitOffset).toBe(false)
      const got = []
      for await (const row of plan.rows()) got.push(row.resolved)
      // Source yields the first offset+limit visible rows of the full scan;
      // engine then slices [offset, offset+limit] for the final result.
      expect(got).toEqual(all.slice(0, offset + limit))
      expect(got.slice(offset, offset + limit)).toEqual(all.slice(offset, offset + limit))
    }
  })

  it('runs a SQL query through squirreling', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    const result = await collect(executeSql({
      tables: { bunnies: source },
      query: 'SELECT "Breed Name", "Popularity Rank" FROM bunnies WHERE "Popularity Rank" <= 3 ORDER BY "Popularity Rank"',
    }))
    expect(result).toEqual([
      { 'Breed Name': 'Holland Lop', 'Popularity Rank': 1n },
      { 'Breed Name': 'Netherland Dwarf', 'Popularity Rank': 2n },
      { 'Breed Name': 'Flemish Giant', 'Popularity Rank': 3n },
    ])
  })
})
