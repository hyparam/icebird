import { describe, expect, it } from 'vitest'
import { icebergRead } from '../src/read.js'

describe.concurrent('icebergRead', () => {
  it('throws for missing tableUrl', async () => {
    await expect(() => icebergRead({ tableUrl: '' }))
      .rejects.toThrow('tableUrl is required')
  })

  it('throws for fetch errors', async () => {
    // not found
    await expect(() => icebergRead({ tableUrl: 'https://hyperparam.app' }))
      .rejects.toThrow('failed to determine latest iceberg version: 404 Not Found')

    // invalid dns
    await expect(() => icebergRead({ tableUrl: 'https://nope.hyperparam.app' }))
      .rejects.toThrow('failed to determine latest iceberg version: fetch failed')

    // with metadataFileName
    await expect(() => icebergRead({
      tableUrl: 'https://hyperparam.app',
      metadataFileName: 'invalid.metadata.json',
    })).rejects.toThrow('failed to get iceberg metadata: 404 Not Found')
  })

  it('throws for invalid row range', async () => {
    await expect(() => icebergRead({ tableUrl: 'https://example.com', rowStart: 5, rowEnd: 3 }))
      .rejects.toThrow('rowStart must be less than rowEnd')

    await expect(() => icebergRead({ tableUrl: 'https://example.com', rowStart: -1 }))
      .rejects.toThrow('rowStart must be positive')
  })
})
