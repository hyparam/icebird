import { describe, expect, it } from 'vitest'
import { whereToParquetFilter } from '../../src/sql/whereFilter.js'

/**
 * @import {ExprNode} from 'squirreling'
 */

/**
 * @param {string} name
 * @returns {ExprNode}
 */
function id(name) {
  return /** @type {ExprNode} */ ({ type: 'identifier', name })
}

/**
 * @param {any} value
 * @returns {ExprNode}
 */
function lit(value) {
  return /** @type {ExprNode} */ ({ type: 'literal', value })
}

/**
 * @param {string} op
 * @param {ExprNode} left
 * @param {ExprNode} right
 * @returns {ExprNode}
 */
function bin(op, left, right) {
  return /** @type {ExprNode} */ ({ type: 'binary', op, left, right })
}

/**
 * @param {string} op
 * @param {ExprNode} argument
 * @returns {ExprNode}
 */
function un(op, argument) {
  return /** @type {ExprNode} */ ({ type: 'unary', op, argument })
}

/**
 * @param {ExprNode} expr
 * @param {ExprNode[]} values
 * @returns {ExprNode}
 */
function inList(expr, values) {
  return /** @type {ExprNode} */ ({ type: 'in valuelist', expr, values })
}

/**
 * @param {string} toType
 * @param {ExprNode} expr
 * @returns {ExprNode}
 */
function cast(toType, expr) {
  return /** @type {ExprNode} */ ({ type: 'cast', toType, expr })
}

describe.concurrent('whereToParquetFilter', () => {
  it('returns undefined for missing where', () => {
    expect(whereToParquetFilter(undefined)).toBeUndefined()
  })

  it('converts identifier = literal', () => {
    const where = bin('=', id('rank'), lit(1))
    expect(whereToParquetFilter(where)).toEqual({ rank: { $eq: 1 } })
  })

  it('flips literal = identifier into a column-first predicate', () => {
    const where = bin('<', lit(5), id('rank'))
    expect(whereToParquetFilter(where)).toEqual({ rank: { $gt: 5 } })
  })

  it('maps all comparison operators', () => {
    /** @type {Array<[string, string]>} */
    const cases = [
      ['=', '$eq'], ['==', '$eq'],
      ['<', '$lt'], ['<=', '$lte'],
      ['>', '$gt'], ['>=', '$gte'],
    ]
    for (const [op, mongo] of cases) {
      const where = bin(op, id('x'), lit(3))
      expect(whereToParquetFilter(where)).toEqual({ x: { [mongo]: 3 } })
    }
    // $ne is true on a null cell (mongodb semantics), so it is guarded
    expect(whereToParquetFilter(bin('!=', id('x'), lit(3)))).toEqual({
      $and: [{ x: { $ne: null } }, { x: { $ne: 3 } }],
    })
    expect(whereToParquetFilter(bin('<>', id('x'), lit(3)))).toEqual({
      $and: [{ x: { $ne: null } }, { x: { $ne: 3 } }],
    })
  })

  it('combines AND/OR', () => {
    const where = bin('AND', bin('=', id('a'), lit(1)), bin('>', id('b'), lit(2)))
    expect(whereToParquetFilter(where)).toEqual({
      $and: [{ a: { $eq: 1 } }, { b: { $gt: 2 } }],
    })
  })

  it('negates by inverting operator under NOT', () => {
    // NOT (a < 5) is a >= 5 in three-valued logic too: a null cell is UNKNOWN
    // either way and stays excluded, so the flipped operator pushes bare
    const where = un('NOT', bin('<', id('a'), lit(5)))
    expect(whereToParquetFilter(where)).toEqual({ a: { $gte: 5 } })
  })

  it('guards comparisons so null cells match SQL', () => {
    // A null cell satisfies no comparison in three-valued logic, negated or
    // not. hyparquet agrees for every operator except $ne, which is true on a
    // null cell (mongodb semantics), so $ne carries a $ne: null guard however
    // it was spelled.
    expect(whereToParquetFilter(bin('<=', id('a'), lit(5)))).toEqual({ a: { $lte: 5 } })
    expect(whereToParquetFilter(bin('!=', id('a'), lit(5)))).toEqual({
      $and: [{ a: { $ne: null } }, { a: { $ne: 5 } }],
    })
    expect(whereToParquetFilter(un('NOT', bin('!=', id('a'), lit(5))))).toEqual({ a: { $eq: 5 } })
    expect(whereToParquetFilter(un('NOT', bin('=', id('a'), lit(5))))).toEqual({
      $and: [{ a: { $ne: null } }, { a: { $ne: 5 } }],
    })
  })

  it('falls back for a comparison against a NULL literal', () => {
    // The engine answers `a = NULL` false for every row; {$eq: null} would
    // instead return exactly the null rows.
    expect(whereToParquetFilter(bin('=', id('a'), lit(null)))).toBeUndefined()
    expect(whereToParquetFilter(bin('!=', id('a'), lit(null)))).toBeUndefined()
    expect(whereToParquetFilter(bin('<', id('a'), lit(null)))).toBeUndefined()
    expect(whereToParquetFilter(un('NOT', bin('=', id('a'), lit(null))))).toBeUndefined()
  })

  it('converts IS NULL and IS NOT NULL, including negation', () => {
    expect(whereToParquetFilter(un('IS NULL', id('a')))).toEqual({ a: { $eq: null } })
    expect(whereToParquetFilter(un('IS NOT NULL', id('a')))).toEqual({ a: { $ne: null } })
    expect(whereToParquetFilter(un('NOT', un('IS NULL', id('a'))))).toEqual({ a: { $ne: null } })
    expect(whereToParquetFilter(un('NOT', un('IS NOT NULL', id('a'))))).toEqual({ a: { $eq: null } })
  })

  it('keeps a session lookup pushable when its chain predicate tests nulls', () => {
    const where = bin(
      'AND',
      bin('=', id('session_id'), lit('session-1')),
      bin(
        'AND',
        bin('OR', un('IS NULL', id('agent_id')), bin('=', id('agent_id'), lit(''))),
        un('IS NOT NULL', id('provider_uuid'))
      )
    )
    expect(whereToParquetFilter(where)).toEqual({
      $and: [
        { session_id: { $eq: 'session-1' } },
        {
          $and: [
            { $or: [{ agent_id: { $eq: null } }, { agent_id: { $eq: '' } }] },
            { provider_uuid: { $ne: null } },
          ],
        },
      ],
    })
  })

  it('turns NOT (a AND b) into ($or) with guarded leaves', () => {
    const where = un('NOT', bin('AND', bin('=', id('a'), lit(1)), bin('=', id('b'), lit(2))))
    expect(whereToParquetFilter(where)).toEqual({
      $or: [
        { $and: [{ a: { $ne: null } }, { a: { $ne: 1 } }] },
        { $and: [{ b: { $ne: null } }, { b: { $ne: 2 } }] },
      ],
    })
  })

  it('turns NOT (a OR b) into ($and) of negated children, never $nor', () => {
    // hyparquet evaluates $nor as a two-valued complement, so a row that is
    // UNKNOWN for every disjunct would match; De Morgan keeps negation at the
    // leaves where the null guards live, and $and prunes where $nor cannot
    const where = un('NOT', bin('OR', bin('<', id('a'), lit(1)), bin('>', id('a'), lit(2))))
    expect(whereToParquetFilter(where)).toEqual({
      $and: [{ a: { $gte: 1 } }, { a: { $lte: 2 } }],
    })
  })

  it('converts IN and NOT IN', () => {
    const where = inList(id('a'), [lit(1), lit(2), lit(3)])
    expect(whereToParquetFilter(where)).toEqual({ a: { $in: [1, 2, 3] } })

    // $nin, like $ne, is true on a null cell; SQL's NOT IN is UNKNOWN there
    const negated = un('NOT', where)
    expect(whereToParquetFilter(negated)).toEqual({
      $and: [{ a: { $ne: null } }, { a: { $nin: [1, 2, 3] } }],
    })
  })

  it('handles NULL members of an IN list', () => {
    // NOT IN over a list holding NULL matches no row: FALSE on a listed
    // value, UNKNOWN everywhere else. $in: [] is hyparquet's never-match.
    const withNull = inList(id('a'), [lit(1), lit(null)])
    expect(whereToParquetFilter(un('NOT', withNull))).toEqual({ a: { $in: [] } })
    // A NULL member of a plain IN can never make the disjunction TRUE, and
    // dropping it keeps row-group statistics pruning decidable
    expect(whereToParquetFilter(withNull)).toEqual({ a: { $in: [1] } })
    expect(whereToParquetFilter(inList(id('a'), [lit(null)]))).toEqual({ a: { $in: [] } })
  })

  it('passes CAST(expr) through', () => {
    const where = /** @type {ExprNode} */ ({
      type: 'cast',
      toType: 'INT',
      expr: bin('=', id('a'), lit(1)),
    })
    expect(whereToParquetFilter(where)).toEqual({ a: { $eq: 1 } })
  })

  it('falls back for a truthiness-changing cast at boolean position', () => {
    // CAST(a = 1 AS TEXT) yields 'false', which is truthy, so unwrapping the
    // cast would filter rows the engine keeps.
    expect(whereToParquetFilter(cast('TEXT', bin('=', id('a'), lit(1))))).toBeUndefined()
    expect(whereToParquetFilter(cast('TIMESTAMP', bin('=', id('a'), lit(1))))).toBeUndefined()
  })

  it('converts a TIMESTAMP typed literal into a Date predicate', () => {
    const where = bin('>=', id('message_created_at'), cast('TIMESTAMP', lit('2026-08-06T00:00:00Z')))
    expect(whereToParquetFilter(where)).toEqual({
      message_created_at: { $gte: new Date('2026-08-06T00:00:00Z') },
    })
  })

  it('flips a TIMESTAMP literal on the left of the comparison', () => {
    const where = bin('>', cast('TIMESTAMP', lit('2026-08-06T00:00:00Z')), id('ts'))
    expect(whereToParquetFilter(where)).toEqual({
      ts: { $lt: new Date('2026-08-06T00:00:00Z') },
    })
  })

  it('negates a TIMESTAMP comparison under NOT', () => {
    const where = un('NOT', bin('<', id('ts'), cast('TIMESTAMP', lit('2026-08-06T00:00:00Z'))))
    expect(whereToParquetFilter(where)).toEqual({
      ts: { $gte: new Date('2026-08-06T00:00:00Z') },
    })
  })

  it('casts numeric epoch literals to TIMESTAMP', () => {
    const where = bin('=', id('ts'), cast('TIMESTAMP', lit(86400000)))
    expect(whereToParquetFilter(where)).toEqual({ ts: { $eq: new Date('1970-01-02T00:00:00Z') } })
  })

  it('folds numeric, boolean, and text casts of literals', () => {
    expect(whereToParquetFilter(bin('=', id('a'), cast('INT', lit('5'))))).toEqual({ a: { $eq: 5 } })
    expect(whereToParquetFilter(bin('=', id('a'), cast('INT', lit(5.7))))).toEqual({ a: { $eq: 5 } })
    expect(whereToParquetFilter(bin('=', id('a'), cast('BIGINT', lit(5))))).toEqual({ a: { $eq: 5n } })
    expect(whereToParquetFilter(bin('=', id('a'), cast('DOUBLE', lit('2.5'))))).toEqual({ a: { $eq: 2.5 } })
    expect(whereToParquetFilter(bin('=', id('a'), cast('BOOL', lit(1))))).toEqual({ a: { $eq: true } })
    expect(whereToParquetFilter(bin('=', id('a'), cast('TEXT', lit(5))))).toEqual({ a: { $eq: '5' } })
  })

  it('falls back for a TEXT cast of a non-primitive literal', () => {
    // The engine JSON-stringifies objects for a TEXT cast, so a Date folds to
    // '"2026-08-06T00:00:00.000Z"', not its String() form.
    const where = bin('=', id('s'), cast('TEXT', cast('TIMESTAMP', lit('2026-08-06T00:00:00Z'))))
    expect(whereToParquetFilter(where)).toBeUndefined()
  })

  it('folds nested casts', () => {
    const where = bin('=', id('a'), cast('TEXT', cast('INT', lit('5.7'))))
    expect(whereToParquetFilter(where)).toEqual({ a: { $eq: '5' } })
  })

  it('falls back for casts the engine would evaluate to null', () => {
    expect(whereToParquetFilter(bin('>=', id('ts'), cast('TIMESTAMP', lit('not a date'))))).toBeUndefined()
    expect(whereToParquetFilter(bin('>=', id('ts'), cast('TIMESTAMP', lit(null))))).toBeUndefined()
    expect(whereToParquetFilter(bin('=', id('a'), cast('INT', lit('abc'))))).toBeUndefined()
    expect(whereToParquetFilter(bin('=', id('a'), cast('INT', lit(null))))).toBeUndefined()
  })

  it('pushes TIMESTAMP literals inside IN lists', () => {
    // Needs hyparquet >= 1.28.0, whose $in/$nin compare Dates by time.
    const where = inList(id('ts'), [cast('TIMESTAMP', lit('2026-08-06T00:00:00Z')), lit(1)])
    expect(whereToParquetFilter(where)).toEqual({
      ts: { $in: [new Date('2026-08-06T00:00:00Z'), 1] },
    })
  })

  it('falls back when an IN value cast is unparseable', () => {
    const where = inList(id('ts'), [cast('TIMESTAMP', lit('not a date'))])
    expect(whereToParquetFilter(where)).toBeUndefined()
  })

  it('folds casts inside IN lists', () => {
    const where = inList(id('a'), [cast('INT', lit('1')), lit(2)])
    expect(whereToParquetFilter(where)).toEqual({ a: { $in: [1, 2] } })
  })

  it('returns undefined for LIKE (not pushable)', () => {
    const where = bin('LIKE', id('a'), lit('foo%'))
    expect(whereToParquetFilter(where)).toBeUndefined()
  })

  it('returns undefined when any AND branch is unpushable', () => {
    const where = bin('AND', bin('=', id('a'), lit(1)), bin('LIKE', id('b'), lit('x%')))
    expect(whereToParquetFilter(where)).toBeUndefined()
  })

  it('returns undefined for identifier-vs-identifier comparisons', () => {
    const where = bin('=', id('a'), id('b'))
    expect(whereToParquetFilter(where)).toBeUndefined()
  })

  it('returns undefined for a null test over a non-identifier expression', () => {
    const where = un('IS NULL', bin('+', id('a'), lit(1)))
    expect(whereToParquetFilter(where)).toBeUndefined()
  })
})
