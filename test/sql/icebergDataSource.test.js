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
    /** @type {unknown} */
    const sentinel = { type: 'Literal', value: true }
    const where = /** @type {ExprNode} */ (sentinel)
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
    // numRows is the manifest record_count sum and overstates when deletes
    // are present; actual streamed rows reflect the post-delete count.
    const { rows } = source.scan({})
    let count = 0
    for await (const row of rows()) {
      expect(row).toBeDefined()
      count++
    }
    expect(count).toBe(15)
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
