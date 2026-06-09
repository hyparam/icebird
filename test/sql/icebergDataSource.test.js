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

  // spark/bunnies has the snapshot history we need for time travel.
  const sparkTableUrl = 's3://hyperparam-iceberg/spark/bunnies'

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

  it('does not push limit/offset down when a WHERE is unpushable', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    // A bare literal WHERE has no parquet-filter translation.
    const where = /** @type {ExprNode} */ ({ type: 'literal', value: true })
    const { appliedWhere, appliedLimitOffset } = source.scan({ where, limit: 5 })
    expect(appliedWhere).toBe(false)
    expect(appliedLimitOffset).toBe(false)
  })

  it('pushes a comparison WHERE down to the parquet read', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    // SELECT * WHERE "Popularity Rank" <= 3
    const where = /** @type {ExprNode} */ ({
      type: 'binary',
      op: '<=',
      left: { type: 'identifier', name: 'Popularity Rank' },
      right: { type: 'literal', value: 3n },
    })
    const { rows, appliedWhere, appliedLimitOffset } = source.scan({ where })
    expect(appliedWhere).toBe(true)
    expect(appliedLimitOffset).toBe(true)

    const collected = []
    for await (const row of rows()) collected.push(row.resolved)
    expect(collected).toHaveLength(3)
    for (const r of collected) {
      const rank = /** @type {bigint} */ (r?.['Popularity Rank'])
      expect(rank <= 3n).toBe(true)
    }
  })

  it('pushes WHERE down and combines with LIMIT/OFFSET when no deletes', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    const where = /** @type {ExprNode} */ ({
      type: 'binary',
      op: '<=',
      left: { type: 'identifier', name: 'Popularity Rank' },
      right: { type: 'literal', value: 10n },
    })
    const { rows, appliedWhere, appliedLimitOffset } = source.scan({ where, limit: 2, offset: 1 })
    expect(appliedWhere).toBe(true)
    expect(appliedLimitOffset).toBe(true)
    const collected = []
    for await (const row of rows()) collected.push(row.resolved)
    expect(collected).toHaveLength(2)
  })

  it('pushed WHERE still respects row-level deletes', async () => {
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v4.metadata.json',
    })
    const where = /** @type {ExprNode} */ ({
      type: 'binary',
      op: '<=',
      left: { type: 'identifier', name: 'Popularity Rank' },
      right: { type: 'literal', value: 21n },
    })
    const { rows, appliedWhere, appliedLimitOffset } = source.scan({ where })
    expect(appliedWhere).toBe(true)
    // With deletes, offset pushdown is unsafe even when WHERE is resolved.
    expect(appliedLimitOffset).toBe(false)
    const collected = []
    for await (const row of rows()) collected.push(row.resolved)
    // v4 has 15 rows after deletes; the predicate covers them all.
    expect(collected).toHaveLength(15)
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

  it('time-travels to a prior snapshot via snapshotId', async () => {
    // spark/bunnies v5 has 20 rows at the current snapshot but 21 at the
    // first snapshot. Passing snapshotId pins the source to that earlier
    // point in time.
    const source = await icebergDataSource({
      tableUrl: sparkTableUrl,
      resolver,
      metadataFileName: 'v5.metadata.json',
      snapshotId: 7505300640432048841n,
    })
    expect(source.numRows).toBe(21)
    const collected = []
    for await (const row of source.scan({}).rows()) collected.push(row.resolved)
    expect(collected).toHaveLength(21)
  })

  it('uses the pinned snapshot schema, not current-schema-id', async () => {
    // v5's current schema (id 1) adds a `breed_name_length` column; the
    // pinned snapshot was written under schema 0 which did not have it.
    const source = await icebergDataSource({
      tableUrl: sparkTableUrl,
      resolver,
      metadataFileName: 'v5.metadata.json',
      snapshotId: 7505300640432048841n,
    })
    expect(source.columns).not.toContain('breed_name_length')
    expect(source.columns).toHaveLength(8)
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

describe.concurrent('icebergDataSource partition pruning', () => {
  // spark/rename_column spec 0: id_bucket = bucket[4](id), date = identity(date).
  // Three data files: id_bucket=0 (ids 1,2), id_bucket=2 (id 4), id_bucket=3 (id 3).
  const tableUrl = 's3://hyperparam-iceberg/spark/rename_column'

  /**
   * Wrap a resolver to count distinct data-file (.parquet under /data/) reads.
   * @param {Resolver} inner
   * @returns {Resolver & { dataFilesRead: () => number }}
   */
  function countingResolver(inner) {
    const files = new Set()
    return {
      /** @type {Resolver['reader']} */
      reader(url, byteLength) {
        if (/\/data\/.*\.parquet$/.test(url)) files.add(url)
        return inner.reader(url, byteLength)
      },
      dataFilesRead: () => files.size,
    }
  }

  /**
   * @param {string} column
   * @param {bigint} value
   * @returns {ExprNode}
   */
  function eq(column, value) {
    return /** @type {ExprNode} */ ({
      type: 'binary', op: '=', left: { type: 'identifier', name: column }, right: { type: 'literal', value },
    })
  }

  it('reads only the bucket-matching data file for an equality predicate', async () => {
    const resolver = countingResolver(localResolver('test/files'))
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })

    // bucket[4](1) === 0, so only the id_bucket=0 file (ids 1 and 2) is read.
    const out = []
    for await (const row of source.scan({ where: eq('id', 1n) }).rows()) out.push(row.resolved)

    expect(out).toEqual([{ id: 1, name: 'Flopsy 🐇', date: new Date('2022-01-01'), price: 9.99, active: true }])
    expect(resolver.dataFilesRead()).toBe(1)
  })

  it('reads zero data files when no partition can match', async () => {
    const resolver = countingResolver(localResolver('test/files'))
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })

    // bucket[4](6) === 1, and no data file lives in bucket 1.
    const out = []
    for await (const row of source.scan({ where: eq('id', 6n) }).rows()) out.push(row.resolved)

    expect(out).toEqual([])
    expect(resolver.dataFilesRead()).toBe(0)
  })

  it('prunes a non-partition predicate via manifest column bounds', async () => {
    const resolver = countingResolver(localResolver('test/files'))
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })

    // `price` is not a partition source, so partition pruning keeps every file.
    // File-level bounds pruning (#20) still skips the data file whose price
    // lower/upper bounds prove no row can be > 0, without changing the result.
    const where = /** @type {ExprNode} */ ({
      type: 'binary', op: '>', left: { type: 'identifier', name: 'price' }, right: { type: 'literal', value: 0n },
    })
    const out = []
    for await (const row of source.scan({ where }).rows()) out.push(row.resolved)

    expect(out.map(r => r?.id).sort()).toEqual([1, 4])
    // The id_bucket=3 file (id 3) has price <= 0, so its bounds prune it; the
    // other two files are read.
    expect(resolver.dataFilesRead()).toBe(2)
  })
})
