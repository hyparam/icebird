import { collect, executeSql } from 'squirreling'
import { describe, expect, it } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergDataSource } from '../../src/sql/icebergDataSource.js'
import { icebergQuery } from '../../src/sql/icebergQuery.js'
import { icebergAppend, icebergCreateTable, icebergRewriteManifests } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/** @import {Schema} from '../../src/types.js' */

/** @type {Schema} */
const schema = {
  type: 'struct', 'schema-id': 0,
  fields: [{ id: 1, name: 'name', type: 'string', required: true }],
}

// UTF-8 orders supplementary characters above the BMP; JavaScript places
// their surrogate pairs below U+E000. Prefixes exercise truncate projection.
const values = ['a', 'z', '\uD7FF', '\uE000', '\uFFFF', '\u{10000}', '😀', 'a\uE000', 'a😀']
const records = values.map(name => ({ name }))

describe('Unicode pruning matches SQL string comparisons', () => {
  it.each(['unpartitioned', 'identity', 'truncate[1]'])('preserves filtered rows and counts with %s partitions', async transform => {
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    const tableUrl = `http://test/unicode-${transform}`
    await icebergCreateTable({
      catalog, tableUrl, schema,
      partitionSpec: {
        'spec-id': 0,
        fields: transform === 'unpartitioned' ? [] : [{ 'source-id': 1, 'field-id': 1000, name: 'part', transform }],
      },
      properties: { 'commit.manifest-merge.enabled': 'false' },
    })
    // Exercise both singleton and mixed Unicode bounds in data files,
    // then combine the manifests to exercise mixed partition summaries too.
    await icebergAppend({ catalog, tableUrl, records: [records[3]] })
    const before = await icebergAppend({ catalog, tableUrl, records: records.filter((_, i) => i !== 3) })
    const after = await icebergRewriteManifests({ catalog, tableUrl })
    for (const metadata of [before, after]) {
      const source = await icebergDataSource({ tableUrl, metadata, resolver })
      for (const value of values) {
        for (const op of ['<', '<=', '>', '>=', '=', 'IN']) {
          const literal = `'${value}'`
          const where = `WHERE name ${op} ${op === 'IN' ? `(${literal})` : literal}`
          for (const select of ['name', 'COUNT(*) AS n']) {
            const query = `SELECT ${select} FROM t ${where}`
            const expected = await collect(await executeSql({ query, tables: { t: records } }))
            const legacy = { columns: source.columns, scan: source.scan, scanColumn: source.scanColumn }
            for (const t of [source, legacy]) {
              const actual = await collect(await icebergQuery({ query, tables: { t } }))
              expect(actual.sort((a, b) => String(a.name).localeCompare(String(b.name))), query)
                .toEqual(expected.sort((a, b) => String(a.name).localeCompare(String(b.name))))
            }
          }
        }
      }
    }
  })
})
