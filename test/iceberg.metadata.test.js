import { describe, expect, it } from 'vitest'
import { icebergLatestVersion, icebergMetadata } from '../src/iceberg.metadata.js'

describe('Iceberg Metadata', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'

  it('fetches the latest sequence number from version-hint.text', async () => {
    const version = await icebergLatestVersion(tableUrl)
    expect(version).toBe(5)
  })

  it('fetches iceberg metadata', async () => {
    const metadata = await icebergMetadata(tableUrl, 'v5.metadata.json')
    expect(metadata).toMatchObject({
      'current-schema-id': 1,
    })
  })
})
