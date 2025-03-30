import { describe, expect, it } from 'vitest'
import { icebergRead } from '../src/iceberg.js'

describe('icebergRead', () => {
  it('throws for missing tableUrl', async () => {
    await expect(() => icebergRead({ tableUrl: '' }))
      .rejects.toThrow('tableUrl is required')
  })

  it('throws for invalid row range', async () => {
    await expect(() => icebergRead({ tableUrl: 'https://example.com', rowStart: 5, rowEnd: 3 }))
      .rejects.toThrow('rowStart must be less than rowEnd')

    await expect(() => icebergRead({ tableUrl: 'https://example.com', rowStart: -1 }))
      .rejects.toThrow('rowStart must be positive')
  })
})
