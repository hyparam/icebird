import { describe, expect, it } from 'vitest'
import { icebergRead } from '../src/read.js'
import { localResolver } from './helpers.js'

describe.concurrent('icebergRead from table with renamed column', () => {
  const tableUrl = 's3://hyperparam-iceberg/spark/rename_column'
  const resolver = localResolver('test/files')

  it('reads pre-renamed column', async () => {
    const data = await icebergRead({ tableUrl, resolver, metadataFileName: 'v2.metadata.json' })
    expect(data).toEqual([
      {
        id: 3,
        name: 'Cottontail',
        date: null,
        price: 0,
        active: true,
      },
      {
        id: 4,
        name: 'Thumper',
        date: new Date('2022-01-02'),
        price: 99999999.99,
        active: false,
      },
      {
        id: 1,
        name: 'Flopsy 🐇',
        date: new Date('2022-01-01'),
        price: 9.99,
        active: true,
      },
      {
        id: 2,
        name: 'Mopsy–Naïve',
        date: new Date('2022-01-01'),
        price: -5,
        active: false,
      },
    ])
  })

  it('reads renamed column', async () => {
    const data = await icebergRead({ tableUrl, resolver, metadataFileName: 'v3.metadata.json' })
    expect(data).toEqual([
      {
        id: 3,
        name: 'Cottontail',
        event_date: null,
        price: 0,
        active: true,
      },
      {
        id: 4,
        name: 'Thumper',
        event_date: new Date('2022-01-02'),
        price: 99999999.99,
        active: false,
      },
      {
        id: 1,
        name: 'Flopsy 🐇',
        event_date: new Date('2022-01-01'),
        price: 9.99,
        active: true,
      },
      {
        id: 2,
        name: 'Mopsy–Naïve',
        event_date: new Date('2022-01-01'),
        price: -5,
        active: false,
      },
    ])
  })

  it('reads a row range spanning multiple data files', async () => {
    const all = await icebergRead({ tableUrl, resolver, metadataFileName: 'v3.metadata.json' })

    // Skip the first file's row, take the next two — crosses a file boundary
    const middle = await icebergRead({ tableUrl, resolver, metadataFileName: 'v3.metadata.json', rowStart: 1, rowEnd: 3 })
    expect(middle).toEqual(all.slice(1, 3))

    // Trailing range starting mid-table
    const tail = await icebergRead({ tableUrl, resolver, metadataFileName: 'v3.metadata.json', rowStart: 2 })
    expect(tail).toEqual(all.slice(2))

    // Leading range stopping before the last file
    const head = await icebergRead({ tableUrl, resolver, metadataFileName: 'v3.metadata.json', rowEnd: 2 })
    expect(head).toEqual(all.slice(0, 2))
  })
})
