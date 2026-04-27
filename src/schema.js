/**
 * @import {IcebergType, Schema} from '../src/types.js'
 */

/**
 * Reject schemas that use Iceberg types introduced in format-version 3
 * (`unknown`, `variant`, `timestamp_ns`, `timestamptz_ns`, `geometry`,
 * `geography`) when targeting an older format version. `geometry` and
 * `geography` carry an optional `(...)` CRS suffix, so we match by prefix.
 *
 * @param {Schema} schema
 * @param {number} formatVersion
 */
export function validateSchemaForVersion(schema, formatVersion) {
  if (formatVersion >= 3) return
  for (const field of schema.fields) {
    checkTypeForV2(field.type, field.name)
  }
}

/**
 * @param {IcebergType} type
 * @param {string} path
 */
function checkTypeForV2(type, path) {
  if (typeof type === 'string') {
    if (
      type === 'unknown' ||
      type === 'variant' ||
      type === 'timestamp_ns' ||
      type === 'timestamptz_ns' ||
      type === 'geometry' || type.startsWith('geometry(') ||
      type === 'geography' || type.startsWith('geography(')
    ) {
      throw new Error(`type ${type} requires format-version 3 (field: ${path})`)
    }
    return
  }
  if (type.type === 'struct') {
    for (const f of type.fields) checkTypeForV2(f.type, `${path}.${f.name}`)
  } else if (type.type === 'list') {
    checkTypeForV2(type.element, `${path}.element`)
  } else if (type.type === 'map') {
    checkTypeForV2(type.key, `${path}.key`)
    checkTypeForV2(type.value, `${path}.value`)
  }
}
