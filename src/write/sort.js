import { compare } from './serde.js'
import { applyTransform, transformResultType } from './transform.js'

/**
 * @import {IcebergType, Schema, SortOrder} from '../../src/types.js'
 */

/**
 * Build a record comparator from a table sort order. Records are ordered by
 * each sort field in turn, applying the field's transform to the source value
 * and comparing in the transform's result type. Honors direction (asc/desc)
 * and null ordering (nulls-first/last, independent of direction). Returns
 * `undefined` for an empty sort order so callers can skip sorting entirely.
 *
 * The comparator returns 0 for records with equal sort keys, so a stable sort
 * (`Array.prototype.sort`) preserves input order among ties.
 *
 * @param {SortOrder | undefined} sortOrder
 * @param {Schema} schema
 * @returns {((a: Record<string, any>, b: Record<string, any>) => number) | undefined}
 */
export function buildSortComparator(sortOrder, schema) {
  if (!sortOrder?.fields?.length) return undefined

  const fields = sortOrder.fields.map(sf => {
    const sourceId = sf['source-id'] ?? sf['source-ids']?.[0]
    const sourceField = schema.fields.find(f => f.id === sourceId)
    if (!sourceField) throw new Error(`sort source field id ${sourceId} not found in schema`)
    return {
      name: sourceField.name,
      transform: sf.transform,
      sourceType: sourceField.type,
      resultType: transformResultType(sf.transform, sourceField.type),
      desc: sf.direction === 'desc',
      nullsFirst: sf['null-order'] === 'nulls-first',
    }
  })

  return (a, b) => {
    for (const f of fields) {
      const ka = sortKey(a[f.name], f.transform, f.sourceType)
      const kb = sortKey(b[f.name], f.transform, f.sourceType)
      const c = compareKeys(ka, kb, f.resultType, f.desc, f.nullsFirst)
      if (c !== 0) return c
    }
    return 0
  }
}

/**
 * Project a source value to its sort key under the field's transform. Null /
 * undefined pass through as null.
 *
 * @param {any} value
 * @param {string} transform
 * @param {IcebergType} sourceType
 * @returns {any}
 */
function sortKey(value, transform, sourceType) {
  if (value === null || value === undefined) return null
  if (transform === 'identity') return value
  return applyTransform(/** @type {any} */ (transform), value, sourceType)
}

/**
 * Compare two sort keys honoring null ordering, NaN placement (greatest), and
 * direction. Null ordering is independent of direction; direction reverses the
 * comparison of present, non-NaN values.
 *
 * @param {any} ka
 * @param {any} kb
 * @param {IcebergType} resultType
 * @param {boolean} desc
 * @param {boolean} nullsFirst
 * @returns {number}
 */
function compareKeys(ka, kb, resultType, desc, nullsFirst) {
  const aNull = ka === null || ka === undefined
  const bNull = kb === null || kb === undefined
  if (aNull && bNull) return 0
  if (aNull) return nullsFirst ? -1 : 1
  if (bNull) return nullsFirst ? 1 : -1

  // NaN is ordered greatest (Iceberg); direction still reverses.
  const aNaN = typeof ka === 'number' && Number.isNaN(ka)
  const bNaN = typeof kb === 'number' && Number.isNaN(kb)
  if (aNaN || bNaN) {
    if (aNaN && bNaN) return 0
    const c = aNaN ? 1 : -1
    return desc ? -c : c
  }

  const c = compare(ka, kb, resultType)
  return desc ? -c : c
}
