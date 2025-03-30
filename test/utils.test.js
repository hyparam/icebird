import { describe, expect, it } from 'vitest'
import { equalityMatch, sanitize } from '../src/utils.js'

describe('sanitizes names', () => {
  it('keeps valid names unchanged', () => {
    expect(sanitize('')).toBe('')
    expect(sanitize('ColumnName')).toBe('ColumnName')
    expect(sanitize('Column_Name123')).toBe('Column_Name123')
  })

  it('replaces invalid first character', () => {
    expect(sanitize('1Name')).toBe('_1Name')
    expect(sanitize('$Name')).toBe('_x24Name')
  })

  it('replaces invalid characters in the rest of the string', () => {
    expect(sanitize('Name$')).toBe('Name_x24')
    expect(sanitize('Name With Space')).toBe('Name_x20With_x20Space')
    expect(sanitize('a%5')).toBe('a_x255')
    expect(sanitize('@')).toBe('_x40')
    expect(sanitize('@#')).toBe('_x40_x23')
  })

  it('preserves underscores and digits', () => {
    expect(sanitize('A_1')).toBe('A_1')
    expect(sanitize('A_1$')).toBe('A_1_x24')
  })
})

describe('equalityMatch', () => {
  it('returns true for a matching row', () => {
    const row = { id: 1, category: 'books', price: 9.99 }
    const deletePredicate = { id: 1, category: null }
    expect(equalityMatch(row, deletePredicate)).toBe(true)
  })

  it('returns false if one of the fields does not match', () => {
    const row = { id: 2, category: 'books', price: 9.99 }
    const deletePredicate = { id: 2, category: 'movies', price: 9.99 }
    expect(equalityMatch(row, deletePredicate)).toBe(false)
  })

  it('ignores file_path and pos fields', () => {
    const row = { id: 1, category: 'books', price: 9.99 }
    const deletePredicate = { id: 1, file_path: 'path/to/file', pos: 123 }
    expect(equalityMatch(row, deletePredicate)).toBe(true)
  })
})
