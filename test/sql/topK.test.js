import { collect, executeSql } from 'squirreling'
import { describe, expect, it, vi } from 'vitest'
import { icebergCreate } from '../../src/create.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergStageDeletionVector } from '../../src/write/stage-deletion-vector.js'
import { icebergStagePositionDelete } from '../../src/write/stage-position-delete.js'
import { computeColumnStats } from '../../src/write/stats.js'
import { serializeValue } from '../../src/write/serde.js'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { icebergQuery } from '../../src/sql/icebergQuery.js'
import { pruneFilteredTopKFiles, pruneTopKFiles } from '../../src/sql/topK.js'
import { localResolver, memResolver } from '../helpers.js'

/**
 * @import {AsyncDataSource} from 'squirreling'
 * @import {ManifestEntry, Resolver, Schema} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct', 'schema-id': 0,
  fields: [
    { id: 1, name: 'id', type: 'int', required: true },
    { id: 2, name: 'date', type: 'date', required: false },
    { id: 3, name: 'ts', type: 'timestamptz', required: false },
    { id: 4, name: 'iso', type: 'string', required: false },
  ],
}

/**
 * @param {{deleted?: number, vector?: boolean, duplicate?: boolean}} [options]
 * @returns {Promise<{ source: Awaited<ReturnType<typeof icebergDataSource>>, opened: Set<string>, tableUrl: string, resolver: Resolver }>}
 */
async function fixture({ deleted = 0, vector = false, duplicate = false } = {}) {
  const tableUrl = 'mem://top-k'
  const { resolver } = memResolver()
  let metadata = await icebergCreate({ tableUrl, resolver, schema, formatVersion: vector ? 3 : 2 })
  let lastPath = ''
  const dataPaths = new Set()
  for (const base of [0, 1000, 2000]) {
    const records = Array.from({ length: 150 }, (_, i) => {
      const id = base + i
      const date = new Date(Date.UTC(2020, 0, 1) + id * 86400000)
      return { id, date, ts: date, iso: date.toISOString() }
    })
    const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    lastPath = staged.writtenFiles[0]
    dataPaths.add(lastPath)
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  for (const count of deleted ? duplicate ? [50, deleted] : [deleted] : []) {
    const stage = vector ? icebergStageDeletionVector : icebergStagePositionDelete
    const staged = await stage({
      tableUrl, metadata, resolver,
      deletes: Array.from({ length: count }, (_, pos) => ({ file_path: lastPath, pos })),
    })
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  const opened = new Set()
  /** @type {Resolver} */
  const counting = {
    reader(path, length) {
      if (dataPaths.has(path)) opened.add(path)
      return resolver.reader(path, length)
    },
  }
  const source = await icebergDataSource({ tableUrl, metadata, resolver: counting })
  return { source, opened, tableUrl, resolver: counting }
}

/**
 * @param {Awaited<ReturnType<typeof icebergDataSource>>} source
 * @returns {AsyncDataSource}
 */
function withoutTopK(source) {
  return {
    ...source,
    prepareScan(request) {
      return source.prepareScan({ ...request, topK: undefined })
    },
  }
}

describe('metadata Top-K', () => {
  it.each(['id', 'date', 'ts', 'iso'])('reads one file for ORDER BY %s DESC LIMIT 100', async column => {
    const { source, opened } = await fixture()
    const query = `SELECT * FROM t ORDER BY ${column} DESC LIMIT 100`
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(opened.size).toBe(3)
    opened.clear()
    const actual = await collect(await icebergQuery({ query, tables: { t: source } }))
    expect(actual).toEqual(expected)
    expect(opened.size).toBe(1)
    // Specializing a query must not change a reusable source.
    expect((await collect(executeSql({ query: 'SELECT COUNT(*) AS n FROM t', tables: { t: source } })))[0].n).toBe(450)
  })

  it('supports URL sources, ASC, aliases and OFFSET across files', async () => {
    const { tableUrl, resolver, opened } = await fixture()
    const result = await icebergQuery({
      query: 'SELECT id AS d FROM t ORDER BY d ASC LIMIT 100 OFFSET 100',
      tables: { t: tableUrl }, resolver,
    })
    expect((await collect(result)).map(r => r.d)).toEqual([
      ...Array.from({ length: 50 }, (_, i) => i + 100),
      ...Array.from({ length: 50 }, (_, i) => i + 1000),
    ])
    expect(opened.size).toBe(2)
  })

  it('prunes through direct squirreling queries with computed projections', async () => {
    const { source, opened } = await fixture()
    const rows = await collect(executeSql({
      query: 'SELECT id + 1 AS next FROM t ORDER BY date DESC LIMIT 100',
      tables: { t: source },
    }))
    expect(rows.map(r => r.next)).toEqual(Array.from({ length: 100 }, (_, i) => 2150 - i))
    expect(opened.size).toBe(1)
  })

  it.each([
    'SELECT id FROM t WHERE id < 2000 ORDER BY id DESC LIMIT 100',
    'SELECT DISTINCT date FROM t ORDER BY date DESC LIMIT 100',
    'SELECT COUNT(*) AS n FROM t ORDER BY n DESC LIMIT 100',
    'SELECT id FROM t ORDER BY -id DESC LIMIT 100',
    'SELECT id FROM t ORDER BY id DESC',
    'SELECT id, ROW_NUMBER() OVER () AS n FROM t ORDER BY id DESC LIMIT 100',
    'WITH a AS (SELECT id FROM t) SELECT id FROM a ORDER BY id DESC LIMIT 100',
  ])('preserves fallback results: %s', async query => {
    const { source } = await fixture()
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(await collect(await icebergQuery({ query, tables: { t: source } }))).toEqual(expected)
  })

  it.each([false, true])('skips a fully deleted file (deletion vector: %s)', async vector => {
    const { source, opened } = await fixture({ deleted: 150, vector })
    const query = 'SELECT id FROM t ORDER BY id DESC LIMIT 100'
    const rows = await collect(await icebergQuery({ query, tables: { t: source } }))
    expect(rows.map(r => r.id)).toEqual(Array.from({ length: 100 }, (_, i) => 1149 - i))
    expect(opened.size).toBe(1)
  })

  it.each([false, true])('counts survivors, merging overlapping deletes (deletion vector: %s)', async vector => {
    const { source, opened } = await fixture({ deleted: 100, vector, duplicate: true })
    const query = 'SELECT id FROM t ORDER BY date DESC LIMIT 100'
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(expected).toHaveLength(100)
    opened.clear()
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
    expect(opened.size).toBe(2)
  })

  it('returns fewer than K survivors without opening fully deleted files', async () => {
    const { source, opened } = await fixture({ deleted: 150 })
    const query = 'SELECT id FROM t ORDER BY id DESC LIMIT 500'
    const rows = await collect(executeSql({ query, tables: { t: source } }))
    expect(rows).toHaveLength(300)
    expect(opened.size).toBe(2)
  })

  it('preserves equality-delete results with a Top-K hint', async () => {
    const source = await icebergDataSource({
      tableUrl: 's3://hyperparam-iceberg/java/bunnies',
      metadataFileName: 'v5.metadata.json',
      resolver: localResolver('test/files'),
    })
    const query = 'SELECT "Breed Name", "Popularity Rank" FROM t ORDER BY "Popularity Rank" DESC LIMIT 10'
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(expected).toHaveLength(10)
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
  })

  it('resolves chained aliases in projection order', async () => {
    const { source, opened } = await fixture()
    const query = 'SELECT id AS a, a AS b FROM t ORDER BY b DESC LIMIT 100'
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    opened.clear()
    expect(await collect(await icebergQuery({ query, tables: { t: source } }))).toEqual(expected)
    expect(opened.size).toBe(1)
  })

  it('keeps sub-millisecond timestamp ties at SQL Date precision', () => {
    /** @type {ManifestEntry[]} */
    const entries = [1001n, 1999n, 999n].map(micros => ({
      status: 1,
      data_file: {
        content: 0, file_path: '', file_format: 'parquet', partition: {},
        record_count: 1n, file_size_in_bytes: 0n,
        value_counts: { 3: 1n }, null_value_counts: { 3: 0n },
        lower_bounds: { 3: serializeValue(micros, 'timestamptz') },
        upper_bounds: { 3: serializeValue(micros, 'timestamptz') },
      },
    }))
    expect(pruneTopKFiles(entries, schema, { orderBy: [{ field: 3, direction: 'DESC', nulls: 'FIRST' }], limit: 1 }))
      .toEqual(entries.slice(0, 2))
  })

  it('keeps tied, overlapping, null-bearing and unknown files', () => {
    const entries = [[1, 2], [2, 3], [3, 4], [null], [99]].map(values => ({
      status: /** @type {const} */ (1), sequence_number: 1n,
      data_file: {
        content: /** @type {const} */ (0), file_path: '', file_format: /** @type {const} */ ('parquet'), partition: {},
        record_count: BigInt(values.length), file_size_in_bytes: 0n,
        ...computeColumnStats(values.map(id => ({ id })), schema),
      },
    }))
    const missing = entries[4].data_file.lower_bounds
    if (missing) delete missing[1]
    delete entries[4].data_file.upper_bounds[1]
    const retained = pruneTopKFiles(entries, schema, { orderBy: [{ field: 1, direction: 'DESC', nulls: 'FIRST' }], limit: 2 })
    expect(retained).toEqual(entries.slice(1))
    expect(pruneTopKFiles(entries, schema, { orderBy: [{ field: 1, direction: 'DESC', nulls: 'FIRST' }], limit: 100 })).toEqual(entries)
  })
})


/**
 * @param {(number | null)[][]} groups
 * @returns {ManifestEntry[]}
 */
function entriesFor(groups) {
  return groups.map((values, index) => ({
    status: 1,
    data_file: {
      content: 0, file_path: String(index), file_format: 'parquet', partition: {},
      record_count: BigInt(values.length), file_size_in_bytes: 0n,
      ...computeColumnStats(values.map(id => ({ id })), schema),
    },
  }))
}

describe('filtered Top-K', () => {
  it('preserves whole-value IN and NOT IN matching on list columns', async () => {
    const tableUrl = 'mem://top-k-list'
    const { resolver } = memResolver()
    /** @type {Schema} */
    const listSchema = {
      type: 'struct', 'schema-id': 0,
      fields: [
        { id: 1, name: 'id', type: 'int', required: true },
        { id: 2, name: 's', required: false, type: { type: 'list', 'element-id': 3, 'element-required': true, element: 'int' } },
      ],
    }
    const records = [{ id: 1, s: [1] }, { id: 10, s: [1, 2] }, { id: 20, s: null }]
    let metadata = await icebergCreate({ tableUrl, resolver, schema: listSchema })
    for (const record of records) {
      const staged = await icebergStageAppend({ tableUrl, metadata, records: [record], resolver })
      metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
    }
    const source = await icebergDataSource({ tableUrl, metadata, resolver })
    for (const predicate of ['s IN (1)', 's IN (1, 3)', 's NOT IN (1)', 's IN (1, NULL)', 's NOT IN (1, NULL)']) {
      const query = `SELECT id FROM t WHERE ${predicate} ORDER BY id DESC LIMIT 1`
      const expected = await collect(executeSql({ query, tables: { t: records } }))
      for (const candidate of [source, withoutTopK(source), { columns: source.columns, scan: source.scan }]) {
        expect(await collect(executeSql({ query, tables: { t: candidate } }))).toEqual(expected)
      }
    }
  })

  it.each(['id', 'date', 'ts', 'iso'])('certifies filtered matches ordered by %s', async column => {
    const { source, opened } = await fixture()
    const query = `SELECT id FROM t WHERE id != 2149 ORDER BY ${column} DESC LIMIT 100`
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    opened.clear()
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
    expect(opened.size).toBe(1)
  })

  it.each(['ASC', 'DESC'])('counts sparse matches across files including OFFSET (%s)', async direction => {
    const { source, opened } = await fixture()
    const query = `SELECT t.id AS value FROM t WHERE t.id IN (149, 1149, 2149) ORDER BY value ${direction} LIMIT 1 OFFSET 1`
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    opened.clear()
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
    expect(opened.size).toBe(2)
  })

  it.each([false, true])('counts only surviving matches with position deletes (vector: %s)', async vector => {
    const { source, opened } = await fixture({ deleted: 100, vector, duplicate: true })
    const query = 'SELECT id FROM t WHERE id != 2149 ORDER BY id DESC LIMIT 100'
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    opened.clear()
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
    expect(opened.size).toBe(2)
  })

  it('does not certify rows removed by equality deletes', async () => {
    const source = await icebergDataSource({
      tableUrl: 's3://hyperparam-iceberg/java/bunnies',
      metadataFileName: 'v5.metadata.json',
      resolver: localResolver('test/files'),
    })
    const query = 'SELECT "Breed Name" FROM t WHERE "Breed Name" IS NOT NULL ORDER BY "Popularity Rank" DESC LIMIT 10'
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
  })

  it.each([
    'SELECT id FROM t WHERE id >= 0 AND CAST(id AS TEXT) LIKE \'%49\' ORDER BY id DESC LIMIT 4',
    'SELECT id FROM t WHERE id IN (149, 1149, 2149) ORDER BY id DESC LIMIT 10',
    'SELECT id FROM t WHERE id = -1 ORDER BY id DESC LIMIT 10',
    'SELECT id FROM t WHERE id >= 0 ORDER BY id DESC, date ASC LIMIT 10',
  ])('preserves fallback results: %s', async query => {
    const { source } = await fixture()
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
  })

  it.each(['FIRST', 'LAST'])('preserves boundary ties and NULLS %s in query results', async nulls => {
    const tableUrl = 'mem://filtered-ties'
    const { resolver } = memResolver()
    const nullableSchema = { ...schema, fields: schema.fields.map(field => ({ ...field, required: false })) }
    let metadata = await icebergCreate({ tableUrl, resolver, schema: nullableSchema, formatVersion: 2 })
    for (const records of [
      [{ id: 1, iso: 'older' }],
      [{ id: 3, iso: 'first tie' }],
      [{ id: 3, iso: 'second tie' }],
      [{ id: null, iso: 'null' }],
    ]) {
      const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
      metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
    }
    const source = await icebergDataSource({ tableUrl, metadata, resolver })
    const query = `SELECT id, iso FROM t WHERE iso IS NOT NULL ORDER BY id DESC NULLS ${nulls} LIMIT 2`
    const expected = await collect(executeSql({ query, tables: { t: withoutTopK(source) } }))
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual(expected)
  })

  it('keeps ties, nulls, unknown bounds and original file order', async () => {
    const entries = entriesFor([[1], [3], [3, 4], [null], [99]])
    const missing = entries[4].data_file.lower_bounds
    if (missing) delete missing[1]
    const count = vi.fn(() => Promise.resolve(1))
    const actual = await pruneFilteredTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'FIRST' }], limit: 1,
    }, count)
    expect(actual).toEqual(entries.slice(1))
    expect(count).toHaveBeenCalledTimes(1)
  })

  it('bounds unsuccessful probing to four files', async () => {
    const entries = entriesFor(Array.from({ length: 10 }, (_, i) => [i]))
    const count = vi.fn(() => Promise.resolve(0))
    expect(await pruneFilteredTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'LAST' }], limit: 1,
    }, count)).toEqual(entries)
    expect(count).toHaveBeenCalledTimes(4)
  })

  it('does no probing when all bounds overlap', async () => {
    const entries = entriesFor([[1, 9], [2, 8], [3, 7]])
    const count = vi.fn(() => Promise.resolve(1))
    expect(await pruneFilteredTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'FIRST' }], limit: 1,
    }, count)).toEqual(entries)
    expect(count).not.toHaveBeenCalled()
  })

  it('propagates cancellation during a probe', async () => {
    const entries = entriesFor([[1], [2]])
    const controller = new AbortController()
    const count = vi.fn(() => {
      controller.abort(new Error('cancelled probe'))
      return Promise.resolve(1)
    })
    await expect(pruneFilteredTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'FIRST' }], limit: 1,
    }, count, controller.signal)).rejects.toThrow('cancelled probe')
  })
})
