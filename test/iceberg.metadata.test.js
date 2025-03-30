import { describe, expect, it } from 'vitest'
import { icebergLatestVersion, icebergMetadata } from '../src/iceberg.metadata.js'

describe.concurrent('Iceberg Metadata', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'

  it('fetches the latest sequence number from version-hint.text', async () => {
    const version = await icebergLatestVersion(tableUrl)
    expect(version).toBe(5)
  })

  it('fetches latest iceberg metadata', async () => {
    const metadata = await icebergMetadata(tableUrl)
    expect(metadata).toMatchObject({
      'current-schema-id': 1,
      'format-version': 2,
      'last-sequence-number': 3,
      location: 's3a://hyperparam-iceberg/spark/bunnies',
    })
  })

  it('fetches previous iceberg metadata', async () => {
    const metadata = await icebergMetadata(tableUrl, 'v3.metadata.json')
    expect(metadata).toMatchObject({
      'current-schema-id': 0,
      'format-version': 2,
      'last-sequence-number': 2,
      location: 's3a://hyperparam-iceberg/spark/bunnies',
    })
  })
})
