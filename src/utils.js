
/**
 * Avro sanitization function.
 *
 * @param {string} name
 * @returns {string}
 */
export function sanitize(name) {
  let result = ''
  for (let i = 0; i < name.length; i++) {
    const ch = name.charAt(i)
    const isLetter = /^[A-Za-z]$/.test(ch)
    const isDigit = /^[0-9]$/.test(ch)
    if (i === 0) {
      if (isLetter || ch === '_') {
        result += ch
      } else {
        result += isDigit ? '_' + ch : '_x' + ch.charCodeAt(0).toString(16).toUpperCase()
      }
    } else {
      if (isLetter || isDigit || ch === '_') {
        result += ch
      } else {
        result += '_x' + ch.charCodeAt(0).toString(16).toUpperCase()
      }
    }
  }
  return result
}

/**
 * Helper to check if a row matches an equality delete predicate.
 *
 * @param {Record<string, any>} row - row from a data file
 * @param {Record<string|number, any>} deletePredicate - equality values keyed by field id or column name
 * @param {Record<number, string>} [columnNamesById] - data file parquet column name by Iceberg field id
 * @returns {boolean} true if row matches the predicate.
 */
export function equalityMatch(row, deletePredicate, columnNamesById) {
  for (const key of Object.keys(deletePredicate)) {
    const columnName = columnNamesById ? columnNamesById[Number(key)] : key
    if (columnName === 'file_path' || columnName === 'pos') continue
    if (!columnName) return false
    if (!valuesEqual(row[columnName], deletePredicate[key])) return false
  }
  return true
}

/**
 * @param {any} a
 * @param {any} b
 * @returns {boolean}
 */
export function valuesEqual(a, b) {
  if (typeof a === 'number' && typeof b === 'number' && Number.isNaN(a) && Number.isNaN(b)) return true
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime()
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    if (a.length !== b.length) return false
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false
    }
    return true
  }
  return a === b
}

/**
 * Generate a UUID v4.
 * @returns {string}
 */
export function uuid4() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID()
  // JS fallback
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0
    const v = c === 'x' ? r : r & 0x3 | 0x8
    return v.toString(16)
  })
}
