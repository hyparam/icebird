import { describe, expect, it } from 'vitest'
import { icebergRead, sanitize } from '../src/iceberg.js'

describe.concurrent('icebergRead', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/warehouse/bunnies'

  it('reads data from Iceberg table with row limits', async () => {
    const data = await icebergRead({ tableUrl, rowStart: 0, rowEnd: 21, metadataFileName: 'v1.metadata.json' })

    // Verify we got correct number of rows
    expect(data).toBeInstanceOf(Array)
    expect(data.length).toBe(21)

    // Verify first row has expected structure
    expect(data[0]).toEqual({
      'Breed Name': 'Holland Lop',
      'Average Weight': 1.8,
      'Fur Length': 3,
      Lifespan: 7n,
      'Origin Country': 'The Netherlands',
      'Ear Type': 'Lop',
      Temperament: 'Friendly',
      'Popularity Rank': 1n,
    })

    // Check we have all expected properties
    const expectedProperties = [
      'Breed Name',
      'Average Weight',
      'Fur Length',
      'Lifespan',
      'Origin Country',
      'Ear Type',
      'Temperament',
      'Popularity Rank',
    ]
    data.forEach(row => {
      expectedProperties.forEach(prop => {
        expect(row).toHaveProperty(prop)
      })
    })
  })

  it('reads data v3 with deleted rows', async () => {
    const data = await icebergRead({ tableUrl, rowStart: 0, rowEnd: 21, metadataFileName: 'v3.metadata.json' })

    expect(data.length).toBe(20)
    expect(data[0]).toEqual({
      'Breed Name': 'Netherland Dwarf',
      'Average Weight': 0.9,
      'Fur Length': 2.5,
      Lifespan: 10n,
      'Origin Country': 'Netherlands',
      'Ear Type': 'Erect',
      Temperament: 'Shy',
      'Popularity Rank': 2n,
    })
  })

  it('reads data v5 with added column', async () => {
    const data = await icebergRead({ tableUrl, rowStart: 0, rowEnd: 21, metadataFileName: 'v5.metadata.json' })

    expect(data.length).toBe(20)
    expect(data[0]).toEqual({
      'Breed Name': 'Netherland Dwarf',
      'Average Weight': 0.9,
      'Fur Length': 2.5,
      Lifespan: 10n,
      'Origin Country': 'Netherlands',
      'Ear Type': 'Erect',
      Temperament: 'Shy',
      'Popularity Rank': 2n,
      breed_name_length: 16,
    })
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
