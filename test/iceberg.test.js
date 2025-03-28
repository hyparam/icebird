import { describe, expect, it } from 'vitest'
import { icebergRead, sanitize } from '../src/iceberg.js'

describe('icebergRead', () => {
  it('throws for invalid row range', () => {
    expect(() => icebergRead({ tableUrl: 'https://example.com', rowStart: 5, rowEnd: 3 }))
      .rejects.toThrow('rowStart must be less than rowEnd')

    expect(() => icebergRead({ tableUrl: 'https://example.com', rowStart: -1 }))
      .rejects.toThrow('rowStart must be positive')
  })
})

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
