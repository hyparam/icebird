import { describe, expect, it } from 'vitest'
import { fetchSnapshotVersion } from '../src/iceberg.metadata.js'

describe('Iceberg fetch utils', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/warehouse/bunnies'

  it('fetches the latest snapshot from version-hint.text', async () => {
    const version = await fetchSnapshotVersion(tableUrl)
    expect(version).toBe(5)
  })
})
