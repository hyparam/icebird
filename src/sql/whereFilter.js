/**
 * Convert a squirreling WHERE clause AST to a hyparquet ParquetQueryFilter.
 * Returns undefined when any sub-expression can't be converted, so callers
 * can fall back to engine-side filtering.
 *
 * A converted filter replaces engine-side WHERE rather than pre-filtering for
 * it, so it must select exactly the rows the engine selects - including on
 * null cells, where the two evaluators disagree by default (see `guardNull`).
 *
 * Filter keys are the SQL identifier names (iceberg field names as exposed by
 * the data source). Per-file mapping to physical parquet column names happens
 * downstream in `readDataFile`.
 *
 * @import {ExprNode} from 'squirreling'
 * @import {BinaryNode, CastType, InValuesNode} from 'squirreling/src/ast.js'
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
  if (node.type === 'unary' && (node.op === 'IS NULL' || node.op === 'IS NOT NULL')) {
    if (node.argument.type !== 'identifier') return undefined
    const isNull = negate ? node.op === 'IS NOT NULL' : node.op === 'IS NULL'
    return { [node.argument.name]: { [isNull ? '$eq' : '$ne']: null } }
  }
  if (node.type === 'binary') {
    return convertBinary(node, negate)
  }
  if (node.type === 'in valuelist') {
    return convertInValues(node, negate)
  }
  if (node.type === 'cast' && TRUTHINESS_PRESERVING_CASTS.has(node.toType)) {
    // A cast at boolean position (WHERE CAST(a = 1 AS INT)) keeps the operand's
    // truthiness only for boolean/numeric targets; TEXT ('false' is truthy) and
    // TIMESTAMP (any Date is truthy) do not, so those fall back to the engine.
    return convertExpr(node.expr, negate)
  }
  return undefined
}

/** @type {Set<CastType>} */
const TRUTHINESS_PRESERVING_CASTS = new Set(
  ['BOOLEAN', 'BOOL', 'INTEGER', 'INT', 'BIGINT', 'FLOAT', 'REAL', 'DOUBLE']
)

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
    // De Morgan: NOT (a OR b) === (NOT a) AND (NOT b), which also holds in
    // SQL's three-valued logic. `$nor` does not: hyparquet evaluates it as a
    // two-valued complement, so a row that is UNKNOWN for every disjunct
    // (null cell) would match. Pushing the negation into the children keeps
    // it at the leaves, where each operator handles null cells itself, and
    // `$and` prunes on row-group statistics where `$nor` never can.
    const l = convertExpr(left, negate)
    const r = convertExpr(right, negate)
    if (!l || !r) return undefined
    return negate ? { $and: [l, r] } : { $or: [l, r] }
  }
  if (op === 'LIKE') return undefined

  const { column, value, flipped } = extractColumnAndValue(left, right)
  if (!column || value === undefined) return undefined
  // A comparison against a NULL literal is UNKNOWN for every row in SQL, so
  // it matches nothing, but {$eq: null} means "is null" to hyparquet. Let the
  // engine (squirreling >= 0.15.3, three-valued) answer it.
  if (value === null) return undefined

  const mongoOp = mapOperator(op, flipped, negate)
  if (!mongoOp) return undefined
  return guardNull({ [column]: { [mongoOp]: value } }, column, mongoOp)
}

/**
 * Make a comparison agree with SQL three-valued logic on null cells: a null
 * cell satisfies no comparison, negated or not (negation was already folded
 * into the operator by `mapOperator`, so by this point there are only bare
 * operators). hyparquet >= 1.28.2 agrees for every operator except `$ne`,
 * which is MongoDB semantics rather than a bug: `$ne` is true on a null
 * cell, so a bare `col != value` would return null rows SQL excludes. Guard
 * it with `col $ne null`. (`$nin` has the same shape; `convertInValues`
 * guards it the same way.)
 *
 * @param {ParquetQueryFilter} predicate
 * @param {string} column
 * @param {string} mongoOp
 * @returns {ParquetQueryFilter}
 */
function guardNull(predicate, column, mongoOp) {
  if (mongoOp === '$ne') return { $and: [{ [column]: { $ne: null } }, predicate] }
  return predicate
}

/**
 * @param {ExprNode} left
 * @param {ExprNode} right
 * @returns {{column: string | undefined, value: any, flipped: boolean}}
 */
function extractColumnAndValue(left, right) {
  if (left.type === 'identifier') {
    const lit = staticLiteral(right)
    if (lit) return { column: left.name, value: lit.value, flipped: false }
  } else if (right.type === 'identifier') {
    const lit = staticLiteral(left)
    if (lit) return { column: right.name, value: lit.value, flipped: true }
  }
  return { column: undefined, value: undefined, flipped: false }
}

/**
 * Statically evaluate an expression to a constant. Handles plain literals and
 * casts of literals, including CAST(string AS TIMESTAMP), which is how
 * squirreling parses typed literals like TIMESTAMP '2026-08-06T00:00:00Z'. The
 * result is wrapped in {value} so an undefined-valued literal isn't confused
 * with "not constant".
 *
 * @param {ExprNode} node
 * @returns {{value: any} | undefined}
 */
function staticLiteral(node) {
  if (node.type === 'literal') return { value: node.value }
  if (node.type === 'cast') {
    const inner = staticLiteral(node.expr)
    if (!inner) return undefined
    return foldCast(node.toType, inner.value)
  }
  return undefined
}

/**
 * Mirror of squirreling's CAST evaluation over primitive literals. Must stay
 * in lockstep with the engine: a pushed-down filter replaces engine-side
 * WHERE, so a folded value that compares differently would change results.
 * A cast the engine would evaluate to null (unparseable date, NaN) returns
 * undefined: null comparisons match no rows, and falling back to the engine
 * preserves that without needing a filter for it.
 *
 * @param {CastType} toType
 * @param {any} val
 * @returns {{value: any} | undefined}
 */
function foldCast(toType, val) {
  if (val === null || val === undefined) return undefined
  if (toType === 'TEXT' || toType === 'STRING' || toType === 'VARCHAR') {
    // The engine JSON-stringifies objects here rather than using String(), so
    // CAST(TIMESTAMP '...' AS TEXT) would fold to a different string. Its
    // stringify helper is not exported, so fall back rather than copy it.
    if (typeof val === 'object') return undefined
    return { value: String(val) }
  }
  if (toType === 'INTEGER' || toType === 'INT') {
    const num = Number(val)
    return isNaN(num) ? undefined : { value: Math.trunc(num) }
  }
  if (toType === 'BIGINT') {
    if (typeof val === 'bigint') return { value: val }
    const num = Number(val)
    return isNaN(num) ? undefined : { value: BigInt(Math.trunc(num)) }
  }
  if (toType === 'FLOAT' || toType === 'REAL' || toType === 'DOUBLE') {
    const num = Number(val)
    return isNaN(num) ? undefined : { value: num }
  }
  if (toType === 'BOOLEAN' || toType === 'BOOL') {
    return { value: Boolean(val) }
  }
  if (toType === 'TIMESTAMP') {
    const date = castTimestamp(val)
    return date ? { value: date } : undefined
  }
  return undefined
}

/**
 * Mirror of squirreling's TIMESTAMP cast: numbers as epoch millis, strings via
 * its `toDate` parse, which requires a YYYY-MM-DD prefix. (`toDate` is not in
 * squirreling's public exports, hence the copy.)
 *
 * @param {any} val
 * @returns {Date | undefined}
 */
function castTimestamp(val) {
  if (val instanceof Date) return val
  if (typeof val === 'number' || typeof val === 'bigint') {
    const date = new Date(Number(val))
    return isNaN(date.getTime()) ? undefined : date
  }
  if (typeof val === 'string' && /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?/.test(val)) {
    const date = new Date(val)
    if (!isNaN(date.getTime())) return date
  }
  return undefined
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
  const column = node.expr.name
  const values = []
  for (const val of node.values) {
    const lit = staticLiteral(val)
    if (!lit) return undefined
    values.push(lit.value)
  }
  // `col NOT IN (…, NULL)` matches no row in SQL: FALSE for a row equal to a
  // listed value, UNKNOWN for every other (nothing is provably distinct from
  // NULL). `$in: []` is hyparquet's never-match, and it prunes every row
  // group on statistics alone.
  if (negate && values.some(value => value === null)) return { [column]: { $in: [] } }
  // A NULL member of a non-negated list can never make the disjunction TRUE,
  // so dropping it is row-identical, and carrying it would cost pruning:
  // stats skipping bails when any member fails to compare against the chunk
  // bounds. An all-NULL list drops to `$in: []`, which is what it means.
  const pushed = values.filter(value => value !== null)
  if (!negate) return { [column]: { $in: pushed } }
  // `$nin`, like `$ne`, is true on a null cell in hyparquet; SQL's NOT IN is
  // UNKNOWN there. Guard the same way guardNull does.
  return { $and: [{ [column]: { $ne: null } }, { [column]: { $nin: pushed } }] }
}
