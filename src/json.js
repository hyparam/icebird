const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)

/**
 * Parse Iceberg metadata JSON, preserving integer literals that exceed
 * Number.MAX_SAFE_INTEGER (2^53-1) as BigInt. Plain `JSON.parse` would
 * truncate 64-bit snapshot ids and other longs to lossy doubles. The Iceberg
 * spec writes these as bare JSON numbers; languages with native int64 (Java,
 * Python) handle them losslessly. JS does not.
 *
 * @param {string} text
 * @returns {any}
 */
export function parseIcebergJson(text) {
  let i = 0
  function skipWs() {
    while (i < text.length) {
      const c = text.charCodeAt(i)
      if (c !== 0x20 && c !== 0x09 && c !== 0x0a && c !== 0x0d) break
      i++
    }
  }
  /** @returns {string} */
  function parseString() {
    if (text[i] !== '"') throw new Error(`expected " at ${i}`)
    i++
    let s = ''
    while (i < text.length) {
      const c = text[i++]
      if (c === '"') return s
      if (c !== '\\') { s += c; continue }
      const e = text[i++]
      if (e === 'u') { s += String.fromCharCode(parseInt(text.slice(i, i + 4), 16)); i += 4 }
      else if (e === 'n') s += '\n'
      else if (e === 't') s += '\t'
      else if (e === 'r') s += '\r'
      else if (e === 'b') s += '\b'
      else if (e === 'f') s += '\f'
      else s += e
    }
    throw new Error('unterminated string')
  }
  /** @returns {number | bigint} */
  function parseNumber() {
    const start = i
    if (text[i] === '-') i++
    while (text[i] >= '0' && text[i] <= '9') i++
    const intEnd = i
    let isFloat = false
    if (text[i] === '.') { isFloat = true; i++; while (text[i] >= '0' && text[i] <= '9') i++ }
    if (text[i] === 'e' || text[i] === 'E') {
      isFloat = true; i++
      if (text[i] === '+' || text[i] === '-') i++
      while (text[i] >= '0' && text[i] <= '9') i++
    }
    if (isFloat) return Number(text.slice(start, i))
    const intStr = text.slice(start, intEnd)
    // 16+ digits is the first length that can exceed 2^53-1; promote to
    // BigInt only when the value actually overflows safe int range.
    if (intStr.length >= 16) {
      const n = BigInt(intStr)
      if (n > MAX_SAFE || n < -MAX_SAFE) return n
    }
    return Number(intStr)
  }
  /**
   * @param {string} lit
   * @param {any} val
   * @returns {any}
   */
  function parseLiteral(lit, val) {
    if (text.slice(i, i + lit.length) !== lit) throw new Error(`bad literal at ${i}`)
    i += lit.length
    return val
  }
  /** @returns {any} */
  function parseValue() {
    skipWs()
    const ch = text[i]
    if (ch === '"') return parseString()
    if (ch === '{') return parseObject()
    if (ch === '[') return parseArray()
    if (ch === 't') return parseLiteral('true', true)
    if (ch === 'f') return parseLiteral('false', false)
    if (ch === 'n') return parseLiteral('null', null)
    return parseNumber()
  }
  /** @returns {Record<string, any>} */
  function parseObject() {
    i++; skipWs()
    /** @type {Record<string, any>} */
    const obj = {}
    if (text[i] === '}') { i++; return obj }
    while (true) {
      skipWs()
      const key = parseString()
      skipWs()
      if (text[i] !== ':') throw new Error(`expected : at ${i}`)
      i++
      obj[key] = parseValue()
      skipWs()
      if (text[i] === ',') { i++; continue }
      if (text[i] === '}') { i++; return obj }
      throw new Error(`expected , or } at ${i}`)
    }
  }
  /** @returns {any[]} */
  function parseArray() {
    i++; skipWs()
    /** @type {any[]} */
    const arr = []
    if (text[i] === ']') { i++; return arr }
    while (true) {
      arr.push(parseValue())
      skipWs()
      if (text[i] === ',') { i++; continue }
      if (text[i] === ']') { i++; return arr }
      throw new Error(`expected , or ] at ${i}`)
    }
  }
  const value = parseValue()
  skipWs()
  if (i !== text.length) throw new Error(`unexpected trailing input at ${i}`)
  return value
}

/**
 * Serialize Iceberg metadata to JSON, emitting BigInt values as bare JSON
 * number literals (the inverse of `parseIcebergJson`). Plain `JSON.stringify`
 * throws on BigInt, and a naive replacer that returns a string would corrupt
 * the metadata by quoting 64-bit snapshot ids the spec requires as numbers.
 * Values above 2^53 are never coerced through Number, so precision is kept.
 *
 * Output matches `JSON.stringify(value, null, indent)` for the JSON value
 * types Iceberg metadata uses (objects, arrays, strings, numbers, booleans,
 * null) and additionally handles BigInt.
 *
 * @param {any} value
 * @param {number} [indent] - Spaces of indentation per level. Default 2.
 * @returns {string}
 */
export function stringifyIcebergJson(value, indent = 2) {
  const pad = ' '.repeat(indent)
  /**
   * @param {any} val
   * @param {number} depth
   * @returns {string}
   */
  function serialize(val, depth) {
    if (typeof val === 'bigint') return val.toString()
    if (val === null || typeof val !== 'object') return JSON.stringify(val)
    const inner = pad.repeat(depth + 1)
    const outer = pad.repeat(depth)
    if (Array.isArray(val)) {
      if (val.length === 0) return '[]'
      const items = val.map(v => inner + serialize(v === undefined ? null : v, depth + 1))
      return `[\n${items.join(',\n')}\n${outer}]`
    }
    const keys = Object.keys(val).filter(k => val[k] !== undefined)
    if (keys.length === 0) return '{}'
    const items = keys.map(k => `${inner}${JSON.stringify(k)}: ${serialize(val[k], depth + 1)}`)
    return `{\n${items.join(',\n')}\n${outer}}`
  }
  return serialize(value, 0)
}
