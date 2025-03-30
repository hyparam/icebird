
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
 * For simplicity, compares all fields (except file_path and pos) by strict equality.
 *
 * @param {Record<string, any>} row - row from a data file
 * @param {Record<string, any>} deletePredicate - row from an equality delete file
 * @returns {boolean} true if row matches the predicate.
 */
export function equalityMatch(row, deletePredicate) {
  for (const key in deletePredicate) {
    if (key === 'file_path' || key === 'pos') continue
    if (deletePredicate[key] !== null && row[key] !== deletePredicate[key]) return false
  }
  return true
}
