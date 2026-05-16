import { describe, expect, it } from 'vitest'
import { icebergRead } from '../src/read.js'
import { localResolver } from './helpers.js'

describe.concurrent('icebergRead from spark iceberg table', () => {
  const tableUrl = 's3://hyperparam-iceberg/spark/bunnies'
  const resolver = localResolver('test/files')

  it('reads data from iceberg', async () => {
    const data = await icebergRead({ tableUrl, metadataFileName: 'v1.metadata.json', resolver })

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

  it('reads data v3 with deleted row', async () => {
    const data = await icebergRead({ tableUrl, metadataFileName: 'v3.metadata.json', resolver })

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

  it('time-travels to a prior snapshot via snapshotId', async () => {
    // The first snapshot of spark/bunnies has 21 rows; the current snapshot
    // (post-delete + add-column) has 20. Pass the exact 64-bit id.
    const data = await icebergRead({
      tableUrl,
      metadataFileName: 'v5.metadata.json',
      resolver,
      snapshotId: 7505300640432048841n,
    })
    expect(data.length).toBe(21)
  })

  it('throws when snapshotId is not in metadata', async () => {
    await expect(() => icebergRead({
      tableUrl,
      metadataFileName: 'v5.metadata.json',
      resolver,
      snapshotId: 1234,
    })).rejects.toThrow('Snapshot 1234 not found in metadata')
  })

  it('reads data v5 with added column', async () => {
    const data = await icebergRead({ tableUrl, metadataFileName: 'v5.metadata.json', resolver })

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
