import { collect } from 'squirreling'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { restCatalogConnect } from '../../src/catalog/rest.js'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { icebergQuery } from '../../src/sql/icebergQuery.js'
import { makeFetch } from '../catalog.rest.helpers.js'
import { fileToJson, localResolver } from '../helpers.js'

/**
 * @import {Resolver} from '../../src/types.js'
 */

describe('icebergQuery', () => {
  const resolver = localResolver('test/files')
  const bunniesMetadata = fileToJson('test/files/hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json')
  const renameColumnMetadata = fileToJson('test/files/hyperparam-iceberg/spark/rename_column/metadata/v3.metadata.json')

  /** @type {ReturnType<typeof makeFetch>} */
  let mock

  beforeEach(() => { mock = makeFetch({}) })
  afterEach(() => { vi.unstubAllGlobals() })

  it('throws when neither catalog nor tables is provided', async () => {
    await expect(() => icebergQuery({ query: 'SELECT 1' }))
      .rejects.toThrow('catalog or tables is required')
  })

  it('throws when query is missing', async () => {
    mock = makeFetch({ 'https://cat/v1/config': {} })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })
    await expect(() => icebergQuery({ catalog, query: '' }))
      .rejects.toThrow('query is required')
  })

  it('resolves a bare table name as a root-namespace reference', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces//tables/bunnies': {
        'metadata-location': 's3a://hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json',
        metadata: bunniesMetadata,
        config: {},
      },
    })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    const result = await icebergQuery({
      catalog,
      query: 'SELECT COUNT(*) AS n FROM bunnies',
      resolver,
    })
    const rows = await collect(result)
    expect(rows).toEqual([{ n: 21 }])
  })

  it('resolves a single table from the catalog and runs the query', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/java/tables/bunnies': {
        'metadata-location': 's3a://hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json',
        metadata: bunniesMetadata,
        config: {},
      },
    })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    const result = await icebergQuery({
      catalog,
      query: 'SELECT "Breed Name", "Popularity Rank" FROM "java.bunnies" WHERE "Popularity Rank" <= 3 ORDER BY "Popularity Rank"',
      resolver,
    })
    expect(result.columns).toEqual(['Breed Name', 'Popularity Rank'])
    const rows = await collect(result)
    expect(rows).toEqual([
      { 'Breed Name': 'Holland Lop', 'Popularity Rank': 1n },
      { 'Breed Name': 'Netherland Dwarf', 'Popularity Rank': 2n },
      { 'Breed Name': 'Flemish Giant', 'Popularity Rank': 3n },
    ])
  })

  it('resolves multi-segment namespaces (a.b.table)', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/prod%1Fjava/tables/bunnies': {
        'metadata-location': 's3a://hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json',
        metadata: bunniesMetadata,
        config: {},
      },
    })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    const result = await icebergQuery({
      catalog,
      query: 'SELECT COUNT(*) AS n FROM "prod.java.bunnies"',
      resolver,
    })
    const rows = await collect(result)
    expect(rows).toEqual([{ n: 21 }])
  })

  it('does not try to resolve CTE names against the catalog', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/java/tables/bunnies': {
        'metadata-location': 's3a://hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json',
        metadata: bunniesMetadata,
        config: {},
      },
    })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    const result = await icebergQuery({
      catalog,
      query: 'WITH top3 AS (SELECT "Breed Name", "Popularity Rank" FROM "java.bunnies" WHERE "Popularity Rank" <= 3) SELECT "Breed Name" FROM top3 ORDER BY "Popularity Rank"',
      resolver,
    })
    const rows = await collect(result)
    expect(rows).toEqual([
      { 'Breed Name': 'Holland Lop' },
      { 'Breed Name': 'Netherland Dwarf' },
      { 'Breed Name': 'Flemish Giant' },
    ])
    const tableCalls = mock.calls.filter(c => c.url.includes('/tables/'))
    expect(tableCalls.map(c => c.url)).toEqual([
      'https://cat/v1/namespaces/java/tables/bunnies',
    ])
  })

  it('loads multiple tables for a JOIN', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/java/tables/bunnies': {
        'metadata-location': 's3a://hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json',
        metadata: bunniesMetadata,
        config: {},
      },
      'https://cat/v1/namespaces/archive/tables/bunnies': {
        'metadata-location': 's3a://hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json',
        metadata: bunniesMetadata,
        config: {},
      },
    })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    const result = await icebergQuery({
      catalog,
      query: 'SELECT a."Breed Name" FROM "java.bunnies" a JOIN "archive.bunnies" b ON a."Breed Name" = b."Breed Name" WHERE a."Popularity Rank" = 1',
      resolver,
    })
    const rows = await collect(result)
    expect(rows).toEqual([{ 'Breed Name': 'Holland Lop' }])
    const tableCalls = mock.calls.filter(c => c.url.includes('/tables/')).map(c => c.url)
    expect(tableCalls).toHaveLength(2)
    expect(tableCalls).toContain('https://cat/v1/namespaces/java/tables/bunnies')
    expect(tableCalls).toContain('https://cat/v1/namespaces/archive/tables/bunnies')
  })

  it('walks compound (UNION ALL) branches when collecting refs', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/java/tables/bunnies': {
        'metadata-location': 's3a://hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json',
        metadata: bunniesMetadata,
        config: {},
      },
    })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    const result = await icebergQuery({
      catalog,
      query: 'SELECT "Breed Name" FROM "java.bunnies" WHERE "Popularity Rank" = 1 UNION ALL SELECT "Breed Name" FROM "java.bunnies" WHERE "Popularity Rank" = 2',
      resolver,
    })
    const rows = await collect(result)
    expect(rows).toEqual([
      { 'Breed Name': 'Holland Lop' },
      { 'Breed Name': 'Netherland Dwarf' },
    ])
  })

  it('throws AbortError when signal is already aborted', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/java/tables/bunnies': {
        'metadata-location': 's3a://hyperparam-iceberg/java/bunnies/metadata/v2.metadata.json',
        metadata: bunniesMetadata,
        config: {},
      },
    })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    const controller = new AbortController()
    controller.abort()
    await expect(() => icebergQuery({
      catalog,
      query: 'SELECT "Breed Name" FROM "java.bunnies"',
      resolver,
      signal: controller.signal,
    })).rejects.toThrow(/Aborted/)
    const tableCalls = mock.calls.filter(c => c.url.includes('/tables/'))
    expect(tableCalls).toHaveLength(0)
  })

  it('streams rows lazily - pulling one row opens fewer data files than the full scan', async () => {
    mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/spark/tables/rename_column': {
        'metadata-location': 's3a://hyperparam-iceberg/spark/rename_column/metadata/v3.metadata.json',
        metadata: renameColumnMetadata,
        config: {},
      },
    })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    /** @type {Set<string>} */
    const lazyOpened = new Set()
    /** @type {Resolver} */
    const lazyResolver = {
      reader(url) {
        if (url.includes('/data/') && url.endsWith('.parquet')) lazyOpened.add(url)
        return resolver.reader(url)
      },
    }

    const result = await icebergQuery({
      catalog,
      query: 'SELECT id FROM "spark.rename_column"',
      resolver: lazyResolver,
    })
    expect(lazyOpened.size).toBe(0)

    const iter = result.rows()[Symbol.asyncIterator]()
    const first = await iter.next()
    expect(first.done).toBe(false)
    expect(lazyOpened.size).toBe(1)
    await iter.return?.(undefined)

    // Sanity check: a full scan opens strictly more data files than the lazy
    // single-row pull, so the size === 1 assertion above isn't trivial.
    /** @type {Set<string>} */
    const fullOpened = new Set()
    /** @type {Resolver} */
    const fullResolver = {
      reader(url) {
        if (url.includes('/data/') && url.endsWith('.parquet')) fullOpened.add(url)
        return resolver.reader(url)
      },
    }
    await collect(await icebergQuery({
      catalog,
      query: 'SELECT id FROM "spark.rename_column"',
      resolver: fullResolver,
    }))
    expect(fullOpened.size).toBeGreaterThan(lazyOpened.size)
  })

  it('accepts a pre-built AsyncDataSource in the tables map', async () => {
    // Time-travel via a pre-built source: spark/bunnies has 21 rows at the
    // first snapshot and 20 at the current snapshot of v5. Pinning the
    // snapshot through the source carries through icebergQuery.
    const source = await icebergDataSource({
      tableUrl: 's3://hyperparam-iceberg/spark/bunnies',
      resolver,
      metadataFileName: 'v5.metadata.json',
      snapshotId: 7505300640432048841n,
    })
    const result = await icebergQuery({
      query: 'SELECT COUNT(*) AS n FROM bunnies',
      tables: { bunnies: source },
    })
    const rows = await collect(result)
    expect(rows).toEqual([{ n: 21 }])
  })

  it('runs without a catalog using a tables → tableUrl map', async () => {
    const result = await icebergQuery({
      query: 'SELECT COUNT(*) AS n FROM bunnies',
      tables: { bunnies: 's3a://hyperparam-iceberg/java/bunnies' },
      resolver,
    })
    const rows = await collect(result)
    expect(rows).toEqual([{ n: 15 }])
  })

  it('resolves multi-segment refs via the tables map', async () => {
    const result = await icebergQuery({
      query: 'SELECT COUNT(*) AS n FROM "java.bunnies"',
      tables: { 'java.bunnies': 's3a://hyperparam-iceberg/java/bunnies' },
      resolver,
    })
    const rows = await collect(result)
    expect(rows).toEqual([{ n: 15 }])
  })

  it('queries a file catalog when every ref is mapped via tables', async () => {
    const catalog = fileCatalog({ resolver })
    const result = await icebergQuery({
      catalog,
      query: 'SELECT COUNT(*) AS n FROM bunnies',
      tables: { bunnies: 's3a://hyperparam-iceberg/java/bunnies' },
    })
    const rows = await collect(result)
    expect(rows).toEqual([{ n: 15 }])
  })

  it('tables map wins over the catalog for the same ref', async () => {
    mock = makeFetch({ 'https://cat/v1/config': {} })
    vi.stubGlobal('fetch', mock.fn)
    const catalog = await restCatalogConnect({ url: 'https://cat' })

    const result = await icebergQuery({
      catalog,
      query: 'SELECT COUNT(*) AS n FROM "java.bunnies"',
      tables: { 'java.bunnies': 's3a://hyperparam-iceberg/java/bunnies' },
      resolver,
    })
    const rows = await collect(result)
    expect(rows).toEqual([{ n: 15 }])
    // Catalog was never consulted for the table.
    const tableCalls = mock.calls.filter(c => c.url.includes('/tables/'))
    expect(tableCalls).toHaveLength(0)
  })

  it('throws when a ref is unmapped and there is no catalog', async () => {
    await expect(() => icebergQuery({
      query: 'SELECT * FROM bunnies JOIN other ON 1=1',
      tables: { bunnies: 's3a://hyperparam-iceberg/java/bunnies' },
      resolver,
    })).rejects.toThrow(/no source for table "other"/)
  })

  it('errors with a helpful message when a SQL ref hits an unmapped file catalog', async () => {
    const catalog = fileCatalog({ resolver })
    await expect(() => icebergQuery({
      catalog,
      query: 'SELECT 1 FROM bunnies',
      resolver,
    })).rejects.toThrow(/tableUrl is required for file catalogs/)
  })
})
