import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergRead } from '../../src/read.js'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { icebergStageUpsert } from '../../src/write/upsert.js'
import { icebergAppend, icebergCreateTable, icebergUpdateSchema, icebergUpsert } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  'identifier-field-ids': [1],
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
}

/**
 * @param {Record<string, any>[]} rows
 * @returns {Record<string, any>[]}
 */
function byId(rows) {
  return [...rows].sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0)
}

describe('icebergUpsert', () => {
  it('replaces matching rows and inserts the rest in one commit', async () => {
    const tableUrl = 'http://test/upsert'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }, { id: 2n, name: 'b' }, { id: 3n, name: 'c' }] })

    // joinCols defaults to the identifier fields.
    const first = await icebergUpsert({ catalog, tableUrl, records: [{ id: 2n, name: 'B' }, { id: 4n, name: 'd' }] })
    expect(first.snapshots?.map(s => s.summary.operation)).toEqual(['append', 'delete', 'append'])
    expect(first['metadata-log']).toHaveLength(2)

    // id 3 is at physical position 2 of the first file but logical index 1.
    const second = await icebergUpsert({ catalog, tableUrl, joinCols: ['id'], records: [{ id: 3n, name: 'C' }] })
    expect(byId(await icebergRead({ tableUrl, metadata: second, resolver }))).toEqual([
      { id: 1n, name: 'a' },
      { id: 2n, name: 'B' },
      { id: 3n, name: 'C' },
      { id: 4n, name: 'd' },
    ])
  })

  it('matches composite keys by field id across partitions', async () => {
    const tableUrl = 'http://test/upsert-composite'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    /** @type {Schema} */
    const composite = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'region', required: true, type: 'string' },
        { id: 2, name: 'id', required: true, type: 'int' },
        { id: 3, name: 'name', required: false, type: 'string' },
      ],
    }
    await icebergCreateTable({
      catalog, tableUrl, schema: composite,
      partitionSpec: { 'spec-id': 0, fields: [{ 'source-id': 1, 'field-id': 1000, name: 'region', transform: 'identity' }] },
    })
    const joinCols = ['region', 'id']
    await icebergUpsert({
      catalog, tableUrl, joinCols,
      records: [{ region: 'eu', id: 1, name: 'eu1' }, { region: 'us', id: 1, name: 'us1' }],
    })
    const update = [{ region: 'eu', id: 1, name: 'eu2' }]
    await icebergUpsert({ catalog, tableUrl, joinCols, records: update })
    const redelivered = await icebergUpsert({ catalog, tableUrl, joinCols, records: update })
    expect(await icebergRead({ tableUrl, metadata: redelivered, resolver })).toEqual([
      { region: 'us', id: 1, name: 'us1' },
      { region: 'eu', id: 1, name: 'eu2' },
    ])

    await icebergUpdateSchema({
      catalog, tableUrl,
      schema: { ...composite, fields: composite.fields.map(f => f.id === 2 ? { ...f, name: 'key' } : f) },
    })
    const renamed = await icebergUpsert({
      catalog, tableUrl, joinCols: ['region', 'key'], records: [{ region: 'us', key: 1, name: 'us2' }],
    })
    expect(await icebergRead({ tableUrl, metadata: renamed, resolver })).toEqual([
      { region: 'eu', key: 1, name: 'eu2' },
      { region: 'us', key: 1, name: 'us2' },
    ])
  })

  it('rejects invalid input before writing any file', async () => {
    const tableUrl = 'http://test/upsert-invalid'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver })
    const metadata = await icebergCreateTable({
      catalog, tableUrl,
      schema: { ...schema, 'identifier-field-ids': [], fields: [...schema.fields, { id: 3, name: 'score', required: false, type: 'double' }] },
    })
    const before = new Set(files.keys())

    /** @type {[any, RegExp][]} */
    const cases = [
      [{ records: [{ id: 1n }] }, /joinCols is required/],
      [{ joinCols: ['missing'], records: [{ id: 1n }] }, /join column not found in current schema: missing/],
      [{ joinCols: ['score'], records: [{ score: 1 }] }, /unsupported join column type for score/],
      [{ joinCols: ['id', 'name'], records: [{ id: 1n, name: null }] }, /missing join column name/],
      [{ joinCols: ['id'], records: [{ id: 1 }] }, /invalid long value for join column id/],
      [{ joinCols: ['id'], records: [{ id: 5n, name: 'x' }, { id: 5n, name: 'y' }] }, /duplicate join key/],
    ]
    for (const [options, error] of cases) {
      await expect(icebergUpsert({ catalog, tableUrl, ...options })).rejects.toThrow(error)
    }
    await expect(icebergStageUpsert({
      tableUrl, metadata: { ...metadata, 'format-version': 3 }, resolver, joinCols: ['id'], records: [],
    })).rejects.toThrow(/format-version 2 only/)
    expect(new Set(files.keys())).toEqual(before)
  })

  it('throws on a concurrent commit rather than duplicating rows', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/upsert-conflict'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver })
    await icebergCreateTable({ catalog, tableUrl, schema })
    const m1 = await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })

    // Stage an upsert against m1, then let a concurrent append commit first.
    const upsert = await icebergStageUpsert({ tableUrl, metadata: m1, resolver, joinCols: ['id'], records: [{ id: 1n, name: 'b' }] })
    const a2 = await icebergStageAppend({ tableUrl, metadata: m1, records: [{ id: 2n, name: 'c' }], resolver })
    const m2 = await fileCatalogCommit({ tableUrl, metadata: m1, staged: a2, resolver })

    await expect(fileCatalogCommit({ tableUrl, metadata: m2, staged: upsert, resolver }))
      .rejects.toThrow(/ref main expected snapshot/)
    expect(await icebergRead({ tableUrl, metadata: m2, resolver })).toEqual([{ id: 1n, name: 'a' }, { id: 2n, name: 'c' }])
  })

  it('keeps staged files when the commit outcome is unknown', async () => {
    const tableUrl = 'http://test/upsert-ambiguous'
    const { resolver, files, lister } = memResolver()
    const deleter = vi.fn(resolver.deleter)
    const { writer } = resolver
    if (!writer) throw new Error('writer required')
    let lost = false
    const catalog = fileCatalog({
      lister,
      conditionalCommits: true,
      resolver: {
        ...resolver,
        deleter,
        // Persist the metadata file, then lose the response.
        writer(path, options) {
          const w = writer(path, options)
          if (!lost || !path.endsWith('.metadata.json')) return w
          const finish = w.finish.bind(w)
          w.finish = async () => {
            await finish()
            throw new Error('connection lost')
          }
          return w
        },
      },
    })
    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    const before = new Set(files.keys())

    lost = true
    await expect(icebergUpsert({ catalog, tableUrl, records: [{ id: 1n, name: 'b' }] })).rejects.toThrow('connection lost')
    expect(deleter).not.toHaveBeenCalled()
    lost = false
    const metadata = await icebergAppend({ catalog, tableUrl, records: [] })
    expect(metadata.snapshots).toHaveLength(4)
    expect([...before].every(path => files.has(path))).toBe(true)
    expect(await icebergRead({ tableUrl, metadata, resolver })).toEqual([{ id: 1n, name: 'b' }])
  })
})
