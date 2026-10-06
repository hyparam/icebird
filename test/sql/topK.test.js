import { collect, executeSql, readBatchColumn, selectedRowCount, valueAt } from 'squirreling'
import { describe, expect, it } from 'vitest'
import { icebergCreate } from '../../src/create.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergStageDeletionVector } from '../../src/write/stage-deletion-vector.js'
import { icebergStagePositionDelete } from '../../src/write/stage-position-delete.js'
import { computeColumnStats } from '../../src/write/stats.js'
import { serializeValue } from '../../src/write/serde.js'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { icebergQuery } from '../../src/sql/icebergQuery.js'
import { pruneTopKFiles, scanTopKFiles } from '../../src/sql/topK.js'
import { localResolver, memResolver } from '../helpers.js'

/**
 * @import {AsyncDataSource, AsyncBatch, Field, SqlPrimitive} from 'squirreling'
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

  it('reads more than four small files without rereading them', async () => {
    const { source, visits } = await filteredFixture(Array.from({ length: 10 }, (_, i) => [
      { id: i * 10, iso: 'keep' }, { id: i * 10 + 1, iso: 'drop' },
    ]))
    const query = 'SELECT id FROM t WHERE iso = \'keep\' ORDER BY id DESC LIMIT 5'
    expect(await collect(executeSql({ query, tables: { t: source } })))
      .toEqual([90, 80, 70, 60, 50].map(id => ({ id })))
    expect(visits).toEqual([9, 8, 7, 6, 5])
  })

  it('tightens the cutoff from actual matches when file bounds overlap', async () => {
    const { source, visits } = await filteredFixture([
      [{ id: 0, iso: 'keep' }, { id: 100, iso: 'drop' }],
      [{ id: 50, iso: 'keep' }, { id: 99, iso: 'drop' }],
      [{ id: 40, iso: 'keep' }],
    ])
    expect(await collect(executeSql({
      query: 'SELECT id FROM t WHERE iso = \'keep\' ORDER BY id DESC LIMIT 1', tables: { t: source },
    }))).toEqual([{ id: 50 }])
    expect(visits).toEqual([0, 1])
  })

  it('restores stable ties after visiting a later file first', async () => {
    const early = new Date('2020-01-01')
    const late = new Date('2021-01-01')
    const { source, visits } = await filteredFixture([
      [{ id: 5, iso: 'keep', ts: early }, { id: 6, iso: 'drop' }],
      [{ id: 5, iso: 'keep', ts: late }, { id: 10, iso: 'drop' }],
    ])
    expect(await collect(executeSql({
      query: 'SELECT id, ts FROM t WHERE iso = \'keep\' ORDER BY id DESC LIMIT 1', tables: { t: source },
    }))).toEqual([{ id: 5, ts: early }])
    expect(visits).toEqual([1, 0, 0, 1])
  })

  it('reads every file only once when the predicate finds no matches', async () => {
    const { source, visits } = await filteredFixture(Array.from({ length: 7 }, (_, i) => [
      { id: i, iso: 'a' }, { id: i, iso: 'z' },
    ]))
    expect(await collect(executeSql({
      query: 'SELECT id FROM t WHERE iso = \'missing\' ORDER BY id DESC LIMIT 2', tables: { t: source },
    }))).toEqual([])
    expect(visits).toEqual([6, 5, 4, 3, 2, 1, 0])
  })

  it('scans unknown bounds and uses matches to preserve ties and null order', async () => {
    const entries = entriesFor([[3], [3, 4], [null], [99]])
    if (entries[3].data_file.upper_bounds) delete entries[3].data_file.upper_bounds[1]
    const fields = [{ id: 1, name: 'id', dataType: { type: /** @type {const} */ ('unknown') }, nullable: true }]
    for (const nulls of [/** @type {const} */ ('FIRST'), /** @type {const} */ ('LAST')]) {
      /** @type {number[]} */
      const visits = []
      const scan = scanTopKFiles(entries, schema, {
        orderBy: [{ field: 1, direction: 'DESC', nulls }], limit: 2,
      }, fields, async function* (entry) {
        const index = Number(entry.data_file.file_path)
        visits.push(index)
        const values = [[3], [3, 4], [null], [99]][index]
        yield { selection: { type: 'all', length: values.length }, columns: [{ type: 'values', values, length: values.length }] }
      })
      const actual = []
      if (!scan) throw new Error('expected supported Top-K')
      for await (const batch of scan) {
        const column = batch.columns[0]
        if ('type' in column && column.type === 'values') actual.push(...column.values)
      }
      expect(actual).toEqual(nulls === 'FIRST' ? [null, 99] : [4, 99])
      expect(visits).toContain(3)
      expect(new Set(visits).size).toBe(visits.length)
    }
  })

  it.each(['ASC', 'DESC'])('matches a full-sort oracle across batches and selections (%s)', async direction => {
    // Deterministic permutation exercises repeated heap insertions/evictions.
    for (const nullable of [false, true]) {
      const groups = Array.from({ length: 8 }, (_, file) => Array.from({ length: 70 }, (_, i) => {
        const ordinal = file * 70 + i
        return { id: nullable && ordinal % 31 === 0 ? null : (ordinal * 193 + 73) % 563, ordinal }
      }))
      const entries = entriesFor(groups.map(rows => rows.map(row => row.id)))
      delete entries[2].data_file.upper_bounds
      /** @type {Field[]} */
      const fields = [
        { id: 1, name: 'id', dataType: { type: 'unknown' }, nullable: true },
        { id: 4, name: 'iso', dataType: { type: 'unknown' }, nullable: false },
      ]
      for (const nulls of [/** @type {const} */ ('FIRST'), /** @type {const} */ ('LAST')]) {
        for (const limit of [1, 17, 140, 600]) {
          const scan = scanTopKFiles(entries, schema, {
            orderBy: [{ field: 1, direction: direction === 'ASC' ? 'ASC' : 'DESC', nulls }], limit,
          }, fields, async function* (entry) {
            const rows = groups[Number(entry.data_file.file_path)]
            for (let start = 0; start < rows.length; start += 10) {
              const chunk = rows.slice(start, start + 10)
              const indices = Uint32Array.from([1, 3, 5, 7, 9])
              yield {
                selection: { type: 'indices', indices, length: chunk.length },
                columns: [
                  { type: 'values', values: chunk.map(row => row.id), length: chunk.length },
                  { type: 'values', values: chunk.map(row => row.ordinal), length: chunk.length },
                ],
              }
            }
          })
          if (!scan) throw new Error('expected supported Top-K')
          const actual = await scanRows(scan)
          const all = groups.flat().filter(row => row.ordinal % 2 === 1)
          const sorted = [...all].sort((a, b) => {
            if (a.id === null || b.id === null) {
              if (a.id === b.id) return a.ordinal - b.ordinal
              return (a.id === null ? -1 : 1) * (nulls === 'FIRST' ? 1 : -1)
            }
            return (a.id - b.id) * (direction === 'ASC' ? 1 : -1) || a.ordinal - b.ordinal
          })
          const winners = sorted.slice(0, limit)
          const threshold = winners.at(-1)?.id
          const required = new Set([...winners, ...sorted.filter(row => row.id === threshold)].map(row => row.ordinal))
          const ordinals = actual.map(row => Number(row[1]))
          expect(ordinals).toEqual([...ordinals].sort((a, b) => a - b))
          expect(ordinals.filter(id => required.has(id))).toEqual([...required].sort((a, b) => a - b))
          expect(sorted.filter(row => ordinals.includes(row.ordinal)).slice(0, limit)).toEqual(winners)
        }
      }
    }
  })

  it('streams every boundary tie with bounded retained state', async () => {
    const groups = [[5, 5, 5], [5, 5, 5], [1]]
    const entries = entriesFor(groups)
    const fields = [{ id: 1, name: 'id', dataType: { type: /** @type {const} */ ('unknown') }, nullable: true }]
    const scan = scanTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'LAST' }], limit: 1,
    }, fields, async function* (entry) {
      const values = groups[Number(entry.data_file.file_path)]
      yield { selection: { type: 'all', length: values.length }, columns: [{ type: 'values', values, length: values.length }] }
    })
    if (!scan) throw new Error('expected supported Top-K')
    expect(await scanRows(scan)).toEqual(Array.from({ length: 6 }, () => [5]))
  })

  it('propagates cancellation during execution', async () => {
    const entries = entriesFor([[1], [2]])
    const controller = new AbortController()
    const scan = scanTopKFiles(entries, schema, {
      orderBy: [{ field: 1, direction: 'DESC', nulls: 'FIRST' }], limit: 1,
    }, [], async function* () {
      controller.abort(new Error('cancelled scan'))
      yield { selection: { type: 'all', length: 1 }, columns: [{ type: 'values', values: [2], length: 1 }] }
    }, controller.signal)
    if (!scan) throw new Error('expected supported Top-K')
    await expect(scan.next()).rejects.toThrow('cancelled scan')
  })
})

/**
 * @param {Array<Array<{id: number | null, iso: string, ts?: Date}>>} groups
 * @returns {Promise<{source: Awaited<ReturnType<typeof icebergDataSource>>, visits: number[]}>}
 */
async function filteredFixture(groups) {
  const tableUrl = 'mem://best-first'
  const { resolver } = memResolver()
  const nullableSchema = { ...schema, fields: schema.fields.map(field => ({ ...field, required: false })) }
  let metadata = await icebergCreate({ tableUrl, resolver, schema: nullableSchema })
  /** @type {string[]} */
  const paths = []
  for (const records of groups) {
    const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    paths.push(staged.writtenFiles[0])
    metadata = await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
  }
  /** @type {number[]} */
  const visits = []
  const source = await icebergDataSource({ tableUrl, metadata, resolver: {
    reader(path, length) {
      const index = paths.indexOf(path)
      if (index >= 0) visits.push(index)
      return resolver.reader(path, length)
    },
  } })
  return { source, visits }
}

/**
 * @param {AsyncIterable<AsyncBatch>} scan
 * @returns {Promise<SqlPrimitive[][]>}
 */
async function scanRows(scan) {
  const rows = []
  for await (const batch of scan) {
    const columns = await Promise.all(batch.columns.map((_, columnIndex) => readBatchColumn({ batch, columnIndex })))
    for (let i = 0; i < selectedRowCount(batch.selection); i++) rows.push(columns.map(column => valueAt(column, i)))
  }
  return rows
}
