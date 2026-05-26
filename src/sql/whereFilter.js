/**
 * Convert a squirreling WHERE clause AST to a hyparquet ParquetQueryFilter.
 * Returns undefined when any sub-expression can't be converted, so callers
 * can fall back to engine-side filtering.
 *
 * Filter keys are the SQL identifier names (iceberg field names as exposed by
 * the data source). Per-file mapping to physical parquet column names happens
 * downstream in `readDataFile`.
 *
 * @import {ExprNode} from 'squirreling'
 * @import {BinaryNode, InValuesNode} from 'squirreling/src/ast.js'
 * @import {ParquetQueryFilter} from 'hyparquet'
 * @param {ExprNode | undefined} where
 * @returns {ParquetQueryFilter | undefined}
 */
export function whereToParquetFilter(where) {
  if (!where) return undefined
  return convertExpr(where, false)
}

/**
 * @param {ExprNode} node
 * @param {boolean} negate
 * @returns {ParquetQueryFilter | undefined}
 */
function convertExpr(node, negate) {
  if (node.type === 'unary' && node.op === 'NOT') {
    return convertExpr(node.argument, !negate)
  }
  if (node.type === 'binary') {
    return convertBinary(node, negate)
  }
  if (node.type === 'in valuelist') {
    return convertInValues(node, negate)
  }
  if (node.type === 'cast') {
    return convertExpr(node.expr, negate)
  }
  return undefined
}

/**
 * @param {BinaryNode} node
 * @param {boolean} negate
 * @returns {ParquetQueryFilter | undefined}
 */
function convertBinary({ op, left, right }, negate) {
  if (op === 'AND') {
    const l = convertExpr(left, negate)
    const r = convertExpr(right, negate)
    if (!l || !r) return undefined
    return negate ? { $or: [l, r] } : { $and: [l, r] }
  }
  if (op === 'OR') {
    const l = convertExpr(left, false)
    const r = convertExpr(right, false)
    if (!l || !r) return undefined
    return negate ? { $nor: [l, r] } : { $or: [l, r] }
  }
  if (op === 'LIKE') return undefined

  const { column, value, flipped } = extractColumnAndValue(left, right)
  if (!column || value === undefined) return undefined

  const mongoOp = mapOperator(op, flipped, negate)
  if (!mongoOp) return undefined
  return { [column]: { [mongoOp]: value } }
}

/**
 * @param {ExprNode} left
 * @param {ExprNode} right
 * @returns {{column: string | undefined, value: any, flipped: boolean}}
 */
function extractColumnAndValue(left, right) {
  if (left.type === 'identifier' && right.type === 'literal') {
    return { column: left.name, value: right.value, flipped: false }
  }
  if (left.type === 'literal' && right.type === 'identifier') {
    return { column: right.name, value: left.value, flipped: true }
  }
  return { column: undefined, value: undefined, flipped: false }
}

const COMP_OPS = new Set(['=', '==', '!=', '<>', '<', '>', '<=', '>='])

/**
 * @param {string} op
 * @param {boolean} flipped
 * @param {boolean} negate
 * @returns {string | undefined}
 */
function mapOperator(op, flipped, negate) {
  if (!COMP_OPS.has(op)) return undefined
  let mapped = op
  if (negate) mapped = neg(mapped)
  if (flipped) mapped = flip(mapped)
  switch (mapped) {
  case '=':
  case '==': return '$eq'
  case '!=':
  case '<>': return '$ne'
  case '<': return '$lt'
  case '<=': return '$lte'
  case '>': return '$gt'
  case '>=': return '$gte'
  }
  return undefined
}

/**
 * @param {string} op
 * @returns {string}
 */
function neg(op) {
  switch (op) {
  case '<': return '>='
  case '<=': return '>'
  case '>': return '<='
  case '>=': return '<'
  case '=':
  case '==': return '!='
  case '!=':
  case '<>': return '='
  }
  return op
}

/**
 * @param {string} op
 * @returns {string}
 */
function flip(op) {
  if (op === '<') return '>'
  if (op === '<=') return '>='
  if (op === '>') return '<'
  if (op === '>=') return '<='
  return op
}

/**
 * @param {InValuesNode} node
 * @param {boolean} negate
 * @returns {ParquetQueryFilter | undefined}
 */
function convertInValues(node, negate) {
  if (node.expr.type !== 'identifier') return undefined
  const values = []
  for (const val of node.values) {
    if (val.type !== 'literal') return undefined
    values.push(val.value)
  }
  return { [node.expr.name]: { [negate ? '$nin' : '$in']: values } }
}
