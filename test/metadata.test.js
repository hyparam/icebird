import { describe, expect, it } from 'vitest'
import { icebergLatestVersion, icebergListVersions, icebergMetadata } from '../src/metadata.js'
import { urlResolver } from '../src/fetch.js'

/**
 * @import {Resolver} from '../src/types.js'
 */

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

  it('fetches latest iceberg metadata with custom resolver', async () => {
    const requestInit = {
      headers: {
        Dummy: 'Bearer my_token',
      },
    }
    const resolver = urlResolver({ requestInit })
    const metadata = await icebergMetadata({ tableUrl, resolver })
    expect(metadata).toMatchObject({
      'current-schema-id': 1,
      'format-version': 2,
      'last-sequence-number': 3,
      location: 's3a://hyperparam-iceberg/spark/bunnies',
    })
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

  it('sorts fallback metadata versions numerically', async () => {
    /** @returns {Promise<string[]>} */
    function lister() {
      return Promise.resolve([
        'v10.metadata.json',
        'v2.metadata.json',
        'v1.metadata.json',
      ])
    }
    /** @type {Resolver} */
    const resolver = {
      reader() {
        throw new Error('version hint missing')
      },
    }

    await expect(icebergLatestVersion({ tableUrl, resolver, lister })).resolves.toBe('v10')
    await expect(icebergListVersions({ tableUrl, resolver, lister })).resolves.toEqual(['v1', 'v2', 'v10'])
  })
})
