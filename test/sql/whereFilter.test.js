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
      ['!=', '$ne'], ['<>', '$ne'],
      ['<', '$lt'], ['<=', '$lte'],
      ['>', '$gt'], ['>=', '$gte'],
    ]
    for (const [op, mongo] of cases) {
      const where = bin(op, id('x'), lit(3))
      expect(whereToParquetFilter(where)).toEqual({ x: { [mongo]: 3 } })
    }
  })

  it('combines AND/OR', () => {
    const where = bin('AND', bin('=', id('a'), lit(1)), bin('>', id('b'), lit(2)))
    expect(whereToParquetFilter(where)).toEqual({
      $and: [{ a: { $eq: 1 } }, { b: { $gt: 2 } }],
    })
  })

  it('negates by inverting operator under NOT', () => {
    const where = un('NOT', bin('<', id('a'), lit(5)))
    expect(whereToParquetFilter(where)).toEqual({ a: { $gte: 5 } })
  })

  it('turns NOT (a AND b) into ($or)', () => {
    const where = un('NOT', bin('AND', bin('=', id('a'), lit(1)), bin('=', id('b'), lit(2))))
    expect(whereToParquetFilter(where)).toEqual({
      $or: [{ a: { $ne: 1 } }, { b: { $ne: 2 } }],
    })
  })

  it('converts IN and NOT IN', () => {
    const where = inList(id('a'), [lit(1), lit(2), lit(3)])
    expect(whereToParquetFilter(where)).toEqual({ a: { $in: [1, 2, 3] } })

    const negated = un('NOT', where)
    expect(whereToParquetFilter(negated)).toEqual({ a: { $nin: [1, 2, 3] } })
  })

  it('passes CAST(expr) through', () => {
    const where = /** @type {ExprNode} */ ({
      type: 'cast',
      toType: 'INT',
      expr: bin('=', id('a'), lit(1)),
    })
    expect(whereToParquetFilter(where)).toEqual({ a: { $eq: 1 } })
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
})
