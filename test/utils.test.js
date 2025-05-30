import { afterEach, describe, expect, it, vi } from 'vitest'
import { equalityMatch, sanitize, uuid4 } from '../src/utils.js'

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

describe('uuid4', () => {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  it('delegates to crypto.randomUUID when it exists', () => {
    vi.stubGlobal('crypto', { randomUUID: vi.fn().mockReturnValue('native-uuid') })

    expect(uuid4()).toBe('native-uuid')
    expect(globalThis.crypto.randomUUID).toHaveBeenCalledOnce()
  })

  it('falls back when crypto.randomUUID is missing', () => {
    vi.stubGlobal('crypto', undefined)
    expect(uuid4()).toMatch(uuidRegex)
  })
})
