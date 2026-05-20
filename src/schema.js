/**
 * @import {Field, IcebergType, Schema} from '../src/types.js'
 */

/**
 * Reject schemas that use features introduced in format-version 3 when
 * targeting an older format version. Covers v3-only types (`unknown`,
 * `variant`, `timestamp_ns`, `timestamptz_ns`, `geometry`, `geography`) and
 * default-value attributes (`initial-default`, `write-default`). `geometry`
 * and `geography` carry an optional `(...)` CRS suffix, so we match by prefix.
 *
 * @param {Schema} schema
 * @param {number} formatVersion
 */
export function validateSchemaForVersion(schema, formatVersion) {
  for (const field of schema.fields) {
    validateFieldForVersion(field, formatVersion, field.name)
  }
}

/**
 * @param {Field[]} fields
 * @returns {number}
 */
export function maxFieldId(fields = []) {
  let max = 0
  for (const field of fields) {
    if (max < field.id) max = field.id
    const nested = maxNestedFieldId(field.type)
    if (max < nested) max = nested
  }
  return max
}

/**
 * @param {IcebergType} type
 * @returns {number}
 */
function maxNestedFieldId(type) {
  if (typeof type === 'string') return 0
  if (type.type === 'list') {
    const elementId = type['element-id'] ?? 0
    return Math.max(elementId, maxNestedFieldId(type.element))
  }
  if (type.type === 'map') {
    const keyId = type['key-id'] ?? 0
    const valueId = type['value-id'] ?? 0
    return Math.max(keyId, valueId, maxNestedFieldId(type.key), maxNestedFieldId(type.value))
  }
  if (type.type === 'struct') return maxFieldId(type.fields)
  return 0
}

/**
 * Spec v3 §"Reserved Field IDs": user schemas must not use field ids
 * greater than 2147483447 (`Integer.MAX_VALUE - 200`). The reserved range
 * holds engine-managed metadata columns (`_file`, `_pos`, `_deleted`,
 * `_spec_id`, `_partition`, the position-delete `file_path`/`pos`/`row`
 * fields, the changelog columns, and the v3 row-lineage `_row_id` and
 * `_last_updated_sequence_number`). A user field at a reserved id silently
 * shadows the corresponding engine column on read.
 */
const MAX_USER_FIELD_ID = 2147483447

/**
 * @param {Field} field
 * @param {number} formatVersion
 * @param {string} path
 */
function validateFieldForVersion(field, formatVersion, path) {
  if (typeof field.id === 'number' && field.id > MAX_USER_FIELD_ID) {
    throw new Error(
      `field id ${field.id} is in the reserved range (> ${MAX_USER_FIELD_ID}) (field: ${path})`
    )
  }
  if (formatVersion < 3) {
    checkTypeForV2(field.type, path)
    if (field['initial-default'] !== undefined) {
      throw new Error(`initial-default requires format-version 3 (field: ${path})`)
    }
    if (field['write-default'] !== undefined) {
      throw new Error(`write-default requires format-version 3 (field: ${path})`)
    }
  } else {
    checkV3Default(field, path)
  }
  checkNestedFieldsForVersion(field.type, formatVersion, path)
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

/**
 * @param {Field} field
 * @param {string} path
 */
function checkV3Default(field, path) {
  const type = typeName(field.type)
  if (!requiresNullDefault(type)) return
  for (const key of /** @type {const} */ (['initial-default', 'write-default'])) {
    if (field[key] != null) {
      throw new Error(`${key} for field ${path} of type ${type} must default to null`)
    }
  }
}

/**
 * @param {IcebergType} type
 * @param {number} formatVersion
 * @param {string} path
 */
function checkNestedFieldsForVersion(type, formatVersion, path) {
  if (typeof type === 'string') return
  if (type.type === 'struct') {
    for (const f of type.fields) validateFieldForVersion(f, formatVersion, `${path}.${f.name}`)
  } else if (type.type === 'list') {
    checkReservedFieldId(type['element-id'], `${path}.element`)
    checkNestedFieldsForVersion(type.element, formatVersion, `${path}.element`)
  } else if (type.type === 'map') {
    checkReservedFieldId(type['key-id'], `${path}.key`)
    checkReservedFieldId(type['value-id'], `${path}.value`)
    checkNestedFieldsForVersion(type.key, formatVersion, `${path}.key`)
    checkNestedFieldsForVersion(type.value, formatVersion, `${path}.value`)
  }
}

/**
 * @param {number | undefined} id
 * @param {string} path
 */
function checkReservedFieldId(id, path) {
  if (typeof id === 'number' && id > MAX_USER_FIELD_ID) {
    throw new Error(
      `field id ${id} is in the reserved range (> ${MAX_USER_FIELD_ID}) (field: ${path})`
    )
  }
}

/**
 * @param {IcebergType} type
 * @returns {string}
 */
export function typeName(type) {
  return typeof type === 'string' ? type : type.type
}

/**
 * @param {string} type
 * @returns {boolean}
 */
function requiresNullDefault(type) {
  return type === 'unknown' ||
    type === 'variant' ||
    type === 'geometry' || type.startsWith('geometry(') ||
    type === 'geography' || type.startsWith('geography(')
}
