import { collect, executeSql } from 'squirreling'
import { beforeAll, describe, expect, it } from 'vitest'
import { fileCatalogCommit } from '../../src/write/commit.js'
import { icebergCreate } from '../../src/create.js'
import { icebergQuery } from '../../src/sql/icebergQuery.js'
import { icebergStageAppend } from '../../src/write/stage.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Resolver, Schema} from '../../src/types.js'
 */

/**
 * A pushed-down filter replaces engine-side WHERE rather than pre-filtering
 * for it, so it must select exactly the rows the engine selects. These tests
 * run each predicate both ways over the same rows: through icebergQuery (which
 * pushes the filter into the parquet reader) and through squirreling alone
 * (the engine's own answer), then compare the row sets.
 */
describe('pushed-down WHERE matches the engine on nullable columns', () => {
  /** @type {Schema} */
  const schema = {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'n', required: false, type: 'int' },
      { id: 3, name: 'ts', required: false, type: 'timestamptz' },
      { id: 4, name: 's', required: false, type: 'string' },
    ],
  }

  const records = [
    { id: 1n, n: 5, ts: new Date('2026-08-11T00:00:00Z'), s: 'a' },
    { id: 2n, n: null, ts: null, s: null },
    { id: 3n, n: 9, ts: new Date('2026-08-13T00:00:00Z'), s: 'b' },
  ]

  const tableUrl = 'mem://where-null-equivalence'
  /** @type {Resolver} */
  let resolver

  beforeAll(async () => {
    const { resolver: memR } = memResolver()
    const metadata = await icebergCreate({ tableUrl, resolver: memR, schema })
    const staged = await icebergStageAppend({ tableUrl, metadata, records, resolver: memR })
    await fileCatalogCommit({ tableUrl, metadata, staged, resolver: memR })
    resolver = memR
  })

  /**
   * @param {string} predicate
   * @returns {Promise<{pushed: number[], engine: number[]}>}
   */
  async function bothWays(predicate) {
    const query = `SELECT id FROM t WHERE ${predicate}`
    const results = await icebergQuery({ query, tables: { t: tableUrl }, resolver })
    const pushed = (await collect(results)).map(row => Number(row.id))
    const engine = (await collect(await executeSql({ query, tables: { t: records } })))
      .map(row => Number(row.id))
    return { pushed, engine }
  }

  const predicates = [
    // relational operators: a null cell coerces to 0 under hyparquet's raw
    // JS comparison and can satisfy the bound
    'n < 7', 'n <= 5', 'n > 3', 'n >= 9', 'n > -1', 'n < -3',
    'ts <= TIMESTAMP \'2026-08-12T00:00:00Z\'',
    'ts >= TIMESTAMP \'2026-08-11T00:00:00Z\'',
    'ts > TIMESTAMP \'2026-08-12T00:00:00Z\'',
    // equality and inequality
    'n = 5', 'n != 5', 'n <> 5',
    // negation: the engine's NOT over a false comparison keeps null rows
    'NOT (n = 5)', 'NOT (n != 5)', 'NOT (n < 7)', 'NOT (n >= 7)', 'NOT (n > 3)',
    // literal on the left
    '5 > n', '5 = n', 'NOT (5 < n)',
    // NULL literal operands
    'n = NULL', 'n != NULL', 'n < NULL', 'NOT (n = NULL)',
    // explicit null tests, which the engine and hyparquet already agree on
    'n IS NULL', 'n IS NOT NULL', 'NOT (n IS NULL)',
    // IN lists
    'n IN (5, 9)', 'n NOT IN (5, 9)', 'n IN (5, NULL)', 'n NOT IN (5, NULL)',
    'NOT (n IN (5, 9))',
    // compound
    'n = 5 AND ts IS NOT NULL', 'n < 7 OR n > 8', 'NOT (n < 7 AND ts IS NOT NULL)',
    // TEXT cast of a non-primitive literal
    's = CAST(TIMESTAMP \'2026-08-11T00:00:00Z\' AS TEXT)',
    's = CAST(5 AS TEXT)',
  ]

  for (const predicate of predicates) {
    it(`agrees for ${predicate}`, async () => {
      const { pushed, engine } = await bothWays(predicate)
      expect(pushed).toEqual(engine)
    })
  }
})
