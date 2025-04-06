import { describe, expect, it, vi } from 'vitest'
import { icebergLatestVersion, icebergListVersions, icebergMetadata } from '../src/iceberg.metadata.js'

describe.concurrent('Iceberg Metadata', () => {
  const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'

  it('fetches the latest sequence number from version-hint.text', async () => {
    const version = await icebergLatestVersion({ tableUrl })
    expect(version).toBe('v5')
  })

  it('fetches iceberg versions from version-hint.text', async () => {
    const versions = await icebergListVersions({ tableUrl })
    expect(versions).toEqual(['v1', 'v2', 'v3', 'v4', 'v5'])
  })

  it('fetches latest iceberg metadata with auth', async () => {
    const fetchSpy = vi.spyOn(global, 'fetch')
    const requestInit = {
      headers: {
        Dummy: 'Bearer my_token',
      },
    }
    const metadata = await icebergMetadata({ tableUrl, requestInit })
    expect(metadata).toMatchObject({
      'current-schema-id': 1,
      'format-version': 2,
      'last-sequence-number': 3,
      location: 's3a://hyperparam-iceberg/spark/bunnies',
    })
    expect(fetchSpy).toHaveBeenCalledWith(tableUrl + '/metadata/version-hint.text', requestInit)
    expect(fetchSpy).toHaveBeenCalledWith(tableUrl + '/metadata/v5.metadata.json', requestInit)
  })

  it('fetches previous iceberg metadata', async () => {
    const metadataFileName = 'v3.metadata.json'
    const metadata = await icebergMetadata({ tableUrl, metadataFileName })
    expect(metadata).toMatchObject({
      'current-schema-id': 0,
      'format-version': 2,
      'last-sequence-number': 2,
      location: 's3a://hyperparam-iceberg/spark/bunnies',
    })
  })
})
