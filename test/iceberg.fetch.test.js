import { describe, expect, it } from 'vitest'
import { fetchLatestSequenceNumber } from '../src/iceberg.metadata.js'

describe('Iceberg fetch utils', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/warehouse/bunnies'

  it('fetches the latest sequence number from version-hint.text', async () => {
    const version = await fetchLatestSequenceNumber(tableUrl)
    expect(version).toBe(5)
  })
})
