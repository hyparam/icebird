import { describe, expect, it } from 'vitest'
import { readIcebergData } from '../src/iceberg.js'

describe('readIcebergData', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/warehouse/bunnies'

  it('reads data from Iceberg table with row limits', async () => {
    const data = await readIcebergData({ tableUrl, rowStart: 0, rowEnd: 10 })

    // Verify we got correct number of rows
    expect(data).toBeInstanceOf(Array)
    expect(data.length).toBe(10)

    // Verify first row has expected structure
    expect(data[0]).toMatchObject({
      Breed_x20Name: 'Holland Lop',
      Average_x20Weight: 1.8,
      Fur_x20Length: 3,
      Lifespan: 7n,
      Origin_x20Country: 'The Netherlands',
      Ear_x20Type: 'Lop',
      Temperament: 'Friendly',
      Popularity_x20Rank: 1n,
    })

    // Check we have all expected properties
    const expectedProperties = [
      'Breed_x20Name',
      'Average_x20Weight',
      'Fur_x20Length',
      'Lifespan',
      'Origin_x20Country',
      'Ear_x20Type',
      'Temperament',
      'Popularity_x20Rank',
    ]
    data.forEach(row => {
      expectedProperties.forEach(prop => {
        expect(row).toHaveProperty(prop)
      })
    })
  })

  it('reads data v2 with deleted rows', async () => {
    const data = await readIcebergData({ tableUrl, rowStart: 0, rowEnd: 10, metadataFileName: 'v3.metadata.json' })

    // Verify first row has expected structure
    expect(data[0]).toMatchObject({
      Breed_x20Name: 'Netherland Dwarf',
      Average_x20Weight: 0.9,
      Fur_x20Length: 2.5,
      Lifespan: 10n,
      Origin_x20Country: 'Netherlands',
      Ear_x20Type: 'Erect',
      Temperament: 'Shy',
      Popularity_x20Rank: 2n,
    })
  })
})
