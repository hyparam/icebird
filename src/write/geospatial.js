/**
 * @import {BoundingBox} from 'hyparquet'
 * @import {Field} from '../../src/types.js'
 */

/**
 * @param {string} name
 * @returns {boolean}
 */
export function isGeoType(name) {
  return name.startsWith('geometry') || name.startsWith('geography')
}

/**
 * Compute Iceberg lower/upper bounds for a geometry or geography column.
 * Bounds are encoded as a single point per Iceberg's geo bound serialization
 * (Appendix D): concatenated 8-byte little-endian IEEE 754 doubles for X, Y,
 * and (when present in any record) Z and M. If only Z is unset but M is set,
 * the Z slot is encoded as NaN. Null/NaN coordinates are skipped per Appendix
 * G; if either X or Y is missing across the column, no bounds are produced.
 *
 * @param {Record<string, any>[]} records
 * @param {Field} field
 * @returns {{ value_count: bigint, null_count: bigint, lower?: Uint8Array, upper?: Uint8Array }}
 */
export function computeGeoBounds(records, field) {
  /** @type {Partial<BoundingBox> | undefined} */
  let partial
  let nulls = 0n
  const writeDefault = field['write-default']
  for (const record of records) {
    let v = record[field.name]
    if (v === undefined && writeDefault !== undefined) v = writeDefault
    if (v === null || v === undefined) {
      nulls++
      continue
    }
    if (typeof v !== 'object') {
      throw new Error('geospatial column expects GeoJSON geometries')
    }
    partial = extendBoundsFromGeometry(partial, v)
  }
  const result = {
    value_count: BigInt(records.length),
    null_count: nulls,
  }
  // If either the X or Y dimension has no finite values, no bounding box is produced.
  const { xmin, ymin, xmax, ymax, zmin, zmax, mmin, mmax } = partial ?? {}
  if (xmin === undefined || ymin === undefined || xmax === undefined || ymax === undefined) {
    return result
  }
  const hasZ = zmin !== undefined
  const hasM = mmin !== undefined
  return {
    ...result,
    lower: encodeGeoPoint(xmin, ymin, zmin, mmin, hasZ, hasM),
    upper: encodeGeoPoint(xmax, ymax, zmax, mmax, hasZ, hasM),
  }
}

/**
 * @param {Partial<BoundingBox> | undefined} bbox
 * @param {any} geometry
 * @returns {Partial<BoundingBox> | undefined}
 */
function extendBoundsFromGeometry(bbox, geometry) {
  if (geometry.type === 'GeometryCollection') {
    for (const child of geometry.geometries || []) {
      bbox = extendBoundsFromGeometry(bbox, child)
    }
    return bbox
  }
  return extendBoundsFromCoordinates(bbox, geometry.coordinates)
}

/**
 * Recurse through nested coordinate arrays. At a leaf position [x,y,(z),(m)],
 * each dimension is filtered independently — a NaN/non-finite value in one
 * dimension does not skip the others.
 * @param {Partial<BoundingBox> | undefined} bbox
 * @param {any[]} coordinates
 * @returns {Partial<BoundingBox> | undefined}
 */
function extendBoundsFromCoordinates(bbox, coordinates) {
  if (typeof coordinates[0] === 'number') {
    // Expand bbox
    bbox = updateAxis(bbox, 'xmin', 'xmax', coordinates[0])
    bbox = updateAxis(bbox, 'ymin', 'ymax', coordinates[1])
    if (coordinates.length > 2) bbox = updateAxis(bbox, 'zmin', 'zmax', coordinates[2])
    if (coordinates.length > 3) bbox = updateAxis(bbox, 'mmin', 'mmax', coordinates[3])
    return bbox
  }
  for (const child of coordinates) {
    bbox = extendBoundsFromCoordinates(bbox, child)
  }
  return bbox
}

/**
 * @param {Partial<BoundingBox> | undefined} bbox
 * @param {'xmin' | 'ymin' | 'zmin' | 'mmin'} minKey
 * @param {'xmax' | 'ymax' | 'zmax' | 'mmax'} maxKey
 * @param {number | undefined} value
 * @returns {Partial<BoundingBox> | undefined}
 */
function updateAxis(bbox, minKey, maxKey, value) {
  if (value === undefined || !Number.isFinite(value)) return bbox
  if (!bbox) bbox = {}
  const min = bbox[minKey]
  const max = bbox[maxKey]
  if (min === undefined || value < min) bbox[minKey] = value
  if (max === undefined || value > max) bbox[maxKey] = value
  return bbox
}

/**
 * Encode a geo bound point per Iceberg Appendix D:
 * - x:y       (16 bytes) when Z and M are both unset
 * - x:y:z     (24 bytes) when M is unset
 * - x:y:NaN:m (32 bytes) when only Z is unset
 * - x:y:z:m   (32 bytes) when both are set
 *
 * @param {number} x
 * @param {number} y
 * @param {number|undefined} z
 * @param {number|undefined} m
 * @param {boolean} hasZ
 * @param {boolean} hasM
 * @returns {Uint8Array}
 */
function encodeGeoPoint(x, y, z, m, hasZ, hasM) {
  const len = !hasZ && !hasM ? 16 : hasZ && !hasM ? 24 : 32
  const buf = new ArrayBuffer(len)
  const view = new DataView(buf)
  view.setFloat64(0, x, true)
  view.setFloat64(8, y, true)
  if (len === 24) {
    view.setFloat64(16, /** @type {number} */ (z), true)
  } else if (len === 32) {
    view.setFloat64(16, hasZ ? /** @type {number} */ (z) : NaN, true)
    view.setFloat64(24, /** @type {number} */ (m), true)
  }
  return new Uint8Array(buf)
}
