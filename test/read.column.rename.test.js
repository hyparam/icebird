import { describe, expect, it } from 'vitest'
import { icebergRead } from '../src/read.js'

describe.concurrent('icebergRead from table with renamed column', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/rename_column'

  it('reads pre-renamed column', async () => {
    const data = await icebergRead({ tableUrl, metadataFileName: 'v2.metadata.json' })
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
    const data = await icebergRead({ tableUrl, metadataFileName: 'v3.metadata.json' })
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
})
