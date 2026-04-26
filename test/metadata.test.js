import { describe, expect, it } from 'vitest'
import { gzipSync } from 'node:zlib'
import { icebergLatestVersion, icebergListVersions, icebergMetadata } from '../src/metadata.js'
import { localLister, localResolver } from './helpers.js'

/**
 * @import {Resolver} from '../src/types.js'
 */

describe.concurrent('Iceberg Metadata', () => {
  const tableUrl = 's3://hyperparam-iceberg/spark/bunnies'
  const resolver = localResolver('test/files')
  const lister = localLister('test/files')

  it('fetches the latest sequence number from version-hint.text', async () => {
    const version = await icebergLatestVersion({ tableUrl, resolver, lister })
    expect(version).toBe('v5')
  })

  it('fetches iceberg versions from version-hint.text', async () => {
    const versions = await icebergListVersions({ tableUrl, resolver, lister })
    expect(versions).toEqual(['v1', 'v2', 'v3', 'v4', 'v5'])
  })

  it('fetches latest iceberg metadata with custom resolver', async () => {
    const metadata = await icebergMetadata({ tableUrl, resolver, lister })
    expect(metadata).toMatchObject({
      'current-schema-id': 1,
      'format-version': 2,
      'last-sequence-number': 3,
      location: 's3a://hyperparam-iceberg/spark/bunnies',
    })
  })

  it('fetches previous iceberg metadata', async () => {
    const metadataFileName = 'v3.metadata.json'
    const metadata = await icebergMetadata({ tableUrl, metadataFileName, resolver, lister })
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

  it('sorts compressed fallback metadata versions numerically', async () => {
    /** @returns {Promise<string[]>} */
    function lister() {
      return Promise.resolve([
        'v10.gz.metadata.json',
        '00003-abc.metadata.json.gz',
        'v2.metadata.json.gz',
      ])
    }
    /** @type {Resolver} */
    const resolver = {
      reader() {
        throw new Error('version hint missing')
      },
    }

    await expect(icebergLatestVersion({ tableUrl, resolver, lister })).resolves.toBe('v10')
    await expect(icebergListVersions({ tableUrl, resolver, lister }))
      .resolves.toEqual(['v2', '00003-abc', 'v10'])
  })

  it('reads gzip-compressed metadata discovered by listing', async () => {
    const localTableUrl = 'http://test/gzip-table'
    const metadata = {
      'format-version': 2,
      'current-schema-id': 0,
      location: localTableUrl,
    }
    const metadataBytes = gzipSync(JSON.stringify(metadata))
    const files = new Map([
      [`${localTableUrl}/metadata/version-hint.text`, new TextEncoder().encode('2').buffer],
      [
        `${localTableUrl}/metadata/v2.metadata.json.gz`,
        metadataBytes.buffer.slice(metadataBytes.byteOffset, metadataBytes.byteOffset + metadataBytes.byteLength),
      ],
    ])
    /** @type {Resolver} */
    const resolver = {
      reader(path) {
        const buf = files.get(path)
        if (!buf) throw new Error(`not found: ${path}`)
        return buf
      },
    }
    /** @returns {Promise<string[]>} */
    function lister() {
      return Promise.resolve(['v2.metadata.json.gz'])
    }

    await expect(icebergMetadata({ tableUrl: localTableUrl, resolver, lister })).resolves.toEqual(metadata)
  })
})
