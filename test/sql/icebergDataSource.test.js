import { collect, executeSql } from 'squirreling'
import { describe, expect, it } from 'vitest'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { localResolver } from '../helpers.js'

/**
 * @import {AsyncDataSource, ExprNode} from 'squirreling'
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
    // A pushed-down WHERE disables position-based LIMIT/OFFSET pushdown, so the
    // engine owns the final slice even though none was requested here.
    expect(appliedLimitOffset).toBe(false)

    const collected = []
    for await (const row of rows()) collected.push(row.resolved)
    expect(collected).toHaveLength(3)
    for (const r of collected) {
      const rank = /** @type {bigint} */ (r?.['Popularity Rank'])
      expect(rank <= 3n).toBe(true)
    }
  })

  it('pushes WHERE down but lets the engine apply LIMIT/OFFSET', async () => {
    // A pushed-down WHERE is matched per row, so OFFSET cannot be pushed by
    // physical position (the first N physical rows are not the first N
    // matches). appliedLimitOffset must be false; the source emits up to
    // offset+limit matched rows and the engine slices the final window.
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
    // Full matched set in physical order, for the slice oracle.
    const matched = []
    for await (const row of source.scan({ where }).rows()) matched.push(row.resolved)

    const offset = 1
    const limit = 2
    const plan = source.scan({ where, limit, offset })
    expect(plan.appliedWhere).toBe(true)
    expect(plan.appliedLimitOffset).toBe(false)
    const collected = []
    for await (const row of plan.rows()) collected.push(row.resolved)
    // Source emits the first offset+limit matches; engine slices [offset, offset+limit).
    expect(collected).toEqual(matched.slice(0, offset + limit))
    expect(collected.slice(offset, offset + limit)).toEqual(matched.slice(offset, offset + limit))
  })

  it('pushed WHERE + LIMIT returns matches that sort after the LIMIT window', async () => {
    // Regression: with a pushed-down filter, bounding the per-file read at
    // `offset + limit` physical rows silently dropped any match positioned
    // after that window. Pick the LAST physical row and query for it with
    // LIMIT 1: a position bound would read only row 0 and return nothing.
    const source = await icebergDataSource({
      tableUrl,
      resolver,
      metadataFileName: 'v2.metadata.json',
    })
    const all = []
    for await (const row of source.scan({}).rows()) all.push(row.resolved)
    expect(all.length).toBeGreaterThan(1)
    const target = /** @type {Record<string, any>} */ (all[all.length - 1])
    const targetRank = /** @type {bigint} */ (target['Popularity Rank'])

    // Equality on a unique column → pushable, matches exactly the last row.
    const where = /** @type {ExprNode} */ ({
      type: 'binary',
      op: '=',
      left: { type: 'identifier', name: 'Popularity Rank' },
      right: { type: 'literal', value: targetRank },
    })
    const plan = source.scan({ where, limit: 1 })
    expect(plan.appliedWhere).toBe(true)
    const collected = []
    for await (const row of plan.rows()) collected.push(row.resolved)
    expect(collected).toEqual([target])
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

describe.concurrent('icebergDataSource scanColumn', () => {
  const tableUrl = 's3://hyperparam-iceberg/java/bunnies'
  const resolver = localResolver('test/files')

  // bunnies v2/v4 each resolve to a single data file, so the cross-file
  // OFFSET/LIMIT and between-file abort branches need a multi-file table.
  // spark/rename_column has three data files walked in record-count order
  // (two single-row files then the two-row file; oracle id order [3,4,1,2]).
  const renameTableUrl = 's3://hyperparam-iceberg/spark/rename_column'

  /**
   * Flatten a scanColumn stream into a single array of values, asserting each
   * yielded chunk is an array-like batch (the streaming/bounded-memory shape).
   *
   * @param {AsyncIterable<ArrayLike<any>>} stream
   * @returns {Promise<{ values: any[], chunks: number }>}
   */
  async function drain(stream) {
    /** @type {any[]} */
    const values = []
    let chunks = 0
    for await (const chunk of stream) {
      expect(typeof chunk.length).toBe('number')
      chunks++
      for (let i = 0; i < chunk.length; i++) values.push(chunk[i])
    }
    return { values, chunks }
  }

  /**
   * Read a single column's full value list via the row `scan()` path, the
   * oracle scanColumn must agree with.
   *
   * @param {AsyncDataSource} source
   * @param {string} column
   * @returns {Promise<any[]>}
   */
  async function scanColumnOracle(source, column) {
    /** @type {any[]} */
    const out = []
    for await (const row of source.scan({ columns: [column] }).rows()) {
      out.push((row.resolved ?? {})[column])
    }
    return out
  }

  it('streams a column in row order, matching scan() for the same column', async () => {
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    const expected = await scanColumnOracle(source, 'Popularity Rank')

    const { scanColumn } = source
    if (!scanColumn) throw new Error('scanColumn not implemented')
    const { values, chunks } = await drain(scanColumn({ column: 'Popularity Rank' }))

    expect(values).toEqual(expected)
    expect(values).toHaveLength(21)
    // At least one chunk was yielded (values arrive incrementally, not as one
    // pre-materialized array the source built internally).
    expect(chunks).toBeGreaterThanOrEqual(1)
  })

  it('respects LIMIT and OFFSET (no deletes)', async () => {
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    const full = await scanColumnOracle(source, 'Popularity Rank')

    const { scanColumn } = source
    if (!scanColumn) throw new Error('scanColumn not implemented')
    for (const [offset, limit] of [[0, 5], [2, 3], [18, 10], [0, 0], [5, 0]]) {
      const { values } = await drain(scanColumn({ column: 'Popularity Rank', offset, limit }))
      expect(values).toEqual(full.slice(offset, offset + limit))
    }
    // OFFSET past the end yields nothing.
    const { values: past } = await drain(scanColumn({ column: 'Popularity Rank', offset: 100, limit: 5 }))
    expect(past).toEqual([])
  })

  it('honors cross-file LIMIT/OFFSET against a multi-data-file table', async () => {
    // The only fixture that exercises scanColumn's cross-file OFFSET whole-file
    // skip-and-resume and cross-file LIMIT early-break (the off-by-one-prone
    // sites). Asserted against the scan() oracle slice in every case.
    const source = await icebergDataSource({ tableUrl: renameTableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    const full = await scanColumnOracle(source, 'id')
    expect(full).toHaveLength(4)

    const { scanColumn } = source
    if (!scanColumn) throw new Error('scanColumn not implemented')

    // Full-column read streams all three files, one chunk per row group/file.
    const { values: all, chunks: allChunks } = await drain(scanColumn({ column: 'id' }))
    expect(all).toEqual(full)
    expect(allChunks).toBe(3)

    // (a) OFFSET that crosses a data-file boundary: offset 1 skips the whole
    // first file and resumes in the second; offset 2 skips the first two files;
    // offset 3 lands inside the final file; offset+limit may overshoot the end.
    for (const [offset, limit] of [[1, 2], [2, 2], [1, 10], [3, 1]]) {
      const { values } = await drain(scanColumn({ column: 'id', offset, limit }))
      expect(values).toEqual(full.slice(offset, offset + limit))
    }

    // (b) LIMIT satisfied before the last file: limit 2 is filled by the first
    // two single-row files, so the final (two-row) file is never opened —
    // proven by the chunk count dropping below the full-read 3.
    const { values: capped, chunks: cappedChunks } = await drain(scanColumn({ column: 'id', limit: 2 }))
    expect(capped).toEqual(full.slice(0, 2))
    expect(cappedChunks).toBe(2)
  })

  it('applies LIMIT/OFFSET over post-delete values when deletes are present', async () => {
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v4.metadata.json' })
    const full = await scanColumnOracle(source, 'Breed Name')
    expect(full).toHaveLength(15)

    const { scanColumn } = source
    if (!scanColumn) throw new Error('scanColumn not implemented')
    // Whole column matches the post-delete scan.
    const { values: all } = await drain(scanColumn({ column: 'Breed Name' }))
    expect(all).toEqual(full)

    // LIMIT/OFFSET are post-delete coordinates (record_count is pre-delete), so
    // they must align with slices of the post-delete oracle, not raw files.
    for (const [offset, limit] of [[0, 1], [4, 2], [8, 4], [13, 5]]) {
      const { values } = await drain(scanColumn({ column: 'Breed Name', offset, limit }))
      expect(values).toEqual(full.slice(offset, offset + limit))
    }
  })

  it('aborts promptly on a pre-aborted signal', async () => {
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    const { scanColumn } = source
    if (!scanColumn) throw new Error('scanColumn not implemented')

    const controller = new AbortController()
    controller.abort()
    await expect(drain(scanColumn({ column: 'Popularity Rank', signal: controller.signal })))
      .rejects.toThrow('Aborted')
  })

  it('aborts mid-stream after consuming the first chunk', async () => {
    // The pre-aborted case above only covers the entry guard. Use the
    // multi-file fixture so there is more to read after the first chunk:
    // consume the first file's chunk successfully, then abort, and the next
    // pull must reject (the same error scan raises) at the between-file guard,
    // honoring the JSDoc's "aborts between chunks" promise.
    const source = await icebergDataSource({ tableUrl: renameTableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    const { scanColumn } = source
    if (!scanColumn) throw new Error('scanColumn not implemented')

    const controller = new AbortController()
    const iterator = scanColumn({ column: 'id', signal: controller.signal })[Symbol.asyncIterator]()

    const first = await iterator.next()
    expect(first.done).toBe(false)
    expect(first.value.length).toBeGreaterThanOrEqual(1)

    controller.abort()
    await expect(iterator.next()).rejects.toThrow('Aborted')
  })

  it('lights squirreling\'s streaming scalar-aggregate fast path', async () => {
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    const baseScanColumn = source.scanColumn
    if (!baseScanColumn) throw new Error('scanColumn not implemented')

    // Spy on scanColumn so we can prove the engine took the streaming path
    // (tryColumnScanAggregate) rather than the buffering aggregate fallback.
    let scanColumnCalls = 0
    /** @type {AsyncDataSource} */
    const spied = {
      ...source,
      /** @type {NonNullable<AsyncDataSource['scanColumn']>} */
      scanColumn(options) {
        scanColumnCalls++
        return baseScanColumn(options)
      },
    }

    const result = await collect(executeSql({
      tables: { bunnies: spied },
      query: 'SELECT COUNT("Popularity Rank") AS c, MIN("Popularity Rank") AS mn, MAX("Popularity Rank") AS mx, SUM("Popularity Rank") AS s, AVG("Popularity Rank") AS a FROM bunnies',
    }))

    expect(scanColumnCalls).toBeGreaterThanOrEqual(1)
    expect(result).toHaveLength(1)
    const row = result[0]
    expect(Number(row.c)).toBe(21)
    expect(Number(row.mn)).toBe(1)
    expect(Number(row.mx)).toBe(21)
    expect(Number(row.s)).toBe(231)
    expect(Number(row.a)).toBe(11)
  })

  it('serves a plain single-column SELECT with LIMIT/OFFSET through the hook', async () => {
    // execute.js takes the scanColumn fast path for any single-column,
    // WHERE-free scan, not just aggregates. Prove the hook is invoked and the
    // streamed rows match the ordinary scan() path (hook removed).
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    const baseScanColumn = source.scanColumn
    if (!baseScanColumn) throw new Error('scanColumn not implemented')

    let scanColumnCalls = 0
    /** @type {AsyncDataSource} */
    const spied = {
      ...source,
      /** @type {NonNullable<AsyncDataSource['scanColumn']>} */
      scanColumn(options) {
        scanColumnCalls++
        return baseScanColumn(options)
      },
    }
    // Same source with the hook removed forces the ordinary row scan() path.
    /** @type {AsyncDataSource} */
    const noHook = { ...source, scanColumn: undefined }

    const query = 'SELECT "Popularity Rank" FROM bunnies LIMIT 5 OFFSET 2'
    const viaHook = await collect(executeSql({ tables: { bunnies: spied }, query }))
    const viaScan = await collect(executeSql({ tables: { bunnies: noHook }, query }))

    expect(scanColumnCalls).toBe(1)
    expect(viaHook).toEqual(viaScan)
    expect(viaHook).toHaveLength(5)
  })

  it('characterizes: N aggregates on one column re-scan it N times (no coalescing)', async () => {
    // Current behavior, pinned for visibility — NOT an endorsement. squirreling
    // runs the streaming-aggregate fast path once per aggregate, so five
    // aggregates over one column re-scan the column five times. Coalescing them
    // into one pass is an upstream squirreling opportunity, not an icebird bug;
    // this characterization makes a future fix a visible diff here.
    const source = await icebergDataSource({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    const baseScanColumn = source.scanColumn
    if (!baseScanColumn) throw new Error('scanColumn not implemented')

    let scanColumnCalls = 0
    /** @type {AsyncDataSource} */
    const spied = {
      ...source,
      /** @type {NonNullable<AsyncDataSource['scanColumn']>} */
      scanColumn(options) {
        scanColumnCalls++
        return baseScanColumn(options)
      },
    }

    await collect(executeSql({
      tables: { bunnies: spied },
      query: 'SELECT COUNT("Popularity Rank") AS c, MIN("Popularity Rank") AS mn, MAX("Popularity Rank") AS mx, SUM("Popularity Rank") AS s, AVG("Popularity Rank") AS a FROM bunnies',
    }))

    expect(scanColumnCalls).toBe(5)
  })
})
