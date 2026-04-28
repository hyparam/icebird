import { describe, expect, it } from 'vitest'
import { computeGeoBounds, isGeoType } from '../../src/write/geospatial.js'

/**
 * @import {Field} from '../../src/types.js'
 */

/**
 * Decode a geo bound point: little-endian f64 per coordinate.
 * @param {Uint8Array} bytes
 * @returns {number[]}
 */
function decodePoint(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const dims = bytes.byteLength / 8
  const out = []
  for (let i = 0; i < dims; i++) out.push(view.getFloat64(i * 8, true))
  return out
}

/** @type {Field} */
const geom = { id: 1, name: 'geom', required: false, type: 'geometry(srid:4326)' }
/** @type {Field} */
const geog = { id: 2, name: 'geog', required: false, type: 'geography(srid:4326,spherical)' }

describe('isGeoType', () => {
  it('matches geometry/geography (with or without parameters)', () => {
    expect(isGeoType('geometry')).toBe(true)
    expect(isGeoType('geometry(srid:4326)')).toBe(true)
    expect(isGeoType('geography')).toBe(true)
    expect(isGeoType('geography(srid:4326,spherical)')).toBe(true)
  })

  it('does not match other types', () => {
    expect(isGeoType('string')).toBe(false)
    expect(isGeoType('binary')).toBe(false)
    expect(isGeoType('variant')).toBe(false)
  })
})

describe('computeGeoBounds', () => {
  it('produces 2D bounds (x:y) from GeoJSON points and lines', () => {
    const records = [
      { geom: { type: 'Point', coordinates: [30, 10] }, geog: { type: 'Point', coordinates: [-5, 60] } },
      {
        geom: { type: 'LineString', coordinates: [[0, -1], [40, 25]] },
        geog: null,
      },
      { geom: null, geog: { type: 'Point', coordinates: [3, 7] } },
    ]

    const geomStats = computeGeoBounds(records, geom)
    expect(geomStats.value_count).toBe(3n)
    expect(geomStats.null_count).toBe(1n)
    expect(geomStats.lower?.length).toBe(16)
    expect(decodePoint(/** @type {Uint8Array} */ (geomStats.lower))).toEqual([0, -1])
    expect(decodePoint(/** @type {Uint8Array} */ (geomStats.upper))).toEqual([40, 25])

    const geogStats = computeGeoBounds(records, geog)
    expect(geogStats.value_count).toBe(3n)
    expect(geogStats.null_count).toBe(1n)
    expect(decodePoint(/** @type {Uint8Array} */ (geogStats.lower))).toEqual([-5, 7])
    expect(decodePoint(/** @type {Uint8Array} */ (geogStats.upper))).toEqual([3, 60])
  })

  it('produces XYZ bounds (x:y:z, 24 bytes)', () => {
    const records = [
      { geom: { type: 'Point', coordinates: [1, 2, 3] } },
      { geom: { type: 'Point', coordinates: [4, 5, 6] } },
    ]

    const stats = computeGeoBounds(records, geom)
    expect(stats.lower?.length).toBe(24)
    expect(decodePoint(/** @type {Uint8Array} */ (stats.lower))).toEqual([1, 2, 3])
    expect(decodePoint(/** @type {Uint8Array} */ (stats.upper))).toEqual([4, 5, 6])
  })

  it('encodes XYM bounds with NaN in the Z slot (x:y:NaN:m, 32 bytes)', () => {
    // 4-element coordinates with NaN in the Z slot — XYM points.
    const records = [
      { geom: { type: 'Point', coordinates: [1, 2, NaN, 100] } },
      { geom: { type: 'Point', coordinates: [3, 4, NaN, 200] } },
    ]

    const stats = computeGeoBounds(records, geom)
    expect(stats.lower?.length).toBe(32)
    const lo = decodePoint(/** @type {Uint8Array} */ (stats.lower))
    const hi = decodePoint(/** @type {Uint8Array} */ (stats.upper))
    expect(lo[0]).toBe(1)
    expect(lo[1]).toBe(2)
    expect(Number.isNaN(lo[2])).toBe(true)
    expect(lo[3]).toBe(100)
    expect(hi[0]).toBe(3)
    expect(hi[1]).toBe(4)
    expect(Number.isNaN(hi[2])).toBe(true)
    expect(hi[3]).toBe(200)
  })

  it('produces XYZM bounds (x:y:z:m, 32 bytes)', () => {
    const records = [
      { geom: { type: 'Point', coordinates: [1, 2, 3, 10] } },
      { geom: { type: 'Point', coordinates: [4, 5, 6, 20] } },
    ]

    const stats = computeGeoBounds(records, geom)
    expect(stats.lower?.length).toBe(32)
    expect(decodePoint(/** @type {Uint8Array} */ (stats.lower))).toEqual([1, 2, 3, 10])
    expect(decodePoint(/** @type {Uint8Array} */ (stats.upper))).toEqual([4, 5, 6, 20])
  })

  it('walks GeometryCollection children for bounds', () => {
    const records = [{
      geom: {
        type: 'GeometryCollection',
        geometries: [
          { type: 'Point', coordinates: [-10, -20] },
          { type: 'Polygon', coordinates: [[[0, 0], [50, 0], [50, 30], [0, 30], [0, 0]]] },
        ],
      },
    }]

    const stats = computeGeoBounds(records, geom)
    expect(decodePoint(/** @type {Uint8Array} */ (stats.lower))).toEqual([-10, -20])
    expect(decodePoint(/** @type {Uint8Array} */ (stats.upper))).toEqual([50, 30])
  })

  it('omits bounds when all records are null', () => {
    const stats = computeGeoBounds([{ geom: null }, { geom: null }], geom)
    expect(stats.value_count).toBe(2n)
    expect(stats.null_count).toBe(2n)
    expect(stats.lower).toBeUndefined()
    expect(stats.upper).toBeUndefined()
  })
})
