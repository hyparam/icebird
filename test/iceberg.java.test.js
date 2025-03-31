import { describe, expect, it, vi } from 'vitest'
import { icebergRead } from '../src/iceberg.js'

describe.concurrent('icebergRead from java iceberg table', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/java/bunnies'

  it('reads data from iceberg table', async () => {
    const data = await icebergRead({
      tableUrl,
      metadataFileName: 'v2.metadata.json',
    })

    expect(data).toBeInstanceOf(Array)
    expect(data.length).toBe(21)
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

  it('reads data v3 with added column', async () => {
    const data = await icebergRead({
      tableUrl,
      metadataFileName: 'v3.metadata.json',
    })

    expect(data.length).toBe(21)
    expect(data[2]).toEqual({
      'Breed Name': 'Flemish Giant',
      'Average Weight': 4.5,
      'Fur Length': 4,
      Lifespan: 5n,
      'Origin Country': 'Belgium',
      'Ear Type': 'Lop',
      Temperament: 'Calm',
      'Popularity Rank': 3n,
      __happy__: undefined,
    })
  })

  it('reads data v4 with deleted rows', async () => {
    const data = await icebergRead({
      tableUrl,
      metadataFileName: 'v4.metadata.json',
    })

    expect(data.length).toBe(15)
    expect(data[2]).toEqual({
      'Breed Name': 'American Fuzzy Lop',
      'Average Weight': 1.4,
      'Fur Length': 5,
      Lifespan: 8n,
      'Origin Country': 'USA',
      'Ear Type': 'Lop',
      Temperament: 'Sociable',
      'Popularity Rank': 8n,
    })
    const newZealandRow = data.find(row => row['Breed Name'] === 'New Zealand')
    expect(newZealandRow).toEqual({
      'Breed Name': 'New Zealand',
      'Average Weight': 4,
      'Fur Length': 2.7,
      Lifespan: 8n,
      'Origin Country': 'New Zealand',
      'Ear Type': 'Erect',
      Temperament: 'Affectionate',
      'Popularity Rank': 21n,
    })
  })

  it('reads data v5 with equality updated row', async () => {
    const fetchSpy = vi.spyOn(global, 'fetch')

    const data = await icebergRead({
      tableUrl,
      metadataFileName: 'v5.metadata.json',
    })

    expect(fetchSpy).toHaveBeenCalledTimes(21)

    expect(data.length).toBe(15)
    const newZealands = data.filter(row => row['Breed Name'] === 'New Zealand')
    expect(newZealands).toHaveLength(1)
    expect(newZealands[0]).toEqual({
      'Breed Name': 'New Zealand',
      'Average Weight': 4,
      'Fur Length': 2.7,
      Lifespan: 8n,
      'Origin Country': 'New Zealand',
      'Ear Type': 'Erect',
      Temperament: 'Affectionate',
      'Popularity Rank': 0n,
      __happy__: true,
    })
  })
})
