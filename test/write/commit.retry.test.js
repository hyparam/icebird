import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { loadLatestFileCatalogMetadata } from '../../src/metadata.js'
import { icebergAppend, icebergCreateTable } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema} from '../../src/types.js'
 */

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
}

describe('loadLatestFileCatalogMetadata', () => {
  it('probes forward when version-hint is stale (says 2 but v3 exists)', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stale-hint'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, name: 'b' }] })
    expect(files.has(`${tableUrl}/metadata/v3.metadata.json`)).toBe(true)

    // overwrite hint with a stale value
    files.set(`${tableUrl}/metadata/version-hint.text`, new TextEncoder().encode('2'))

    const latest = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(latest.version).toBe(3)
    expect(latest.metadataFileName).toBe('v3.metadata.json')
  })

  it('falls back to listing when version-hint is missing', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/missing-hint'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'a' }] })
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    files.delete(`${tableUrl}/metadata/version-hint.text`)

    const latest = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(latest.version).toBe(2)
    expect(latest.metadataFileName).toBe('v2.metadata.json')
  })

  it('returns v1 right after create', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/just-created'
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })
    await icebergCreateTable({ catalog, tableUrl, schema })

    const latest = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    expect(latest.version).toBe(1)
    expect(latest.metadata['format-version']).toBe(2)
  })

  it('throws if no metadata files exist at all', async () => {
    const tableUrl = 'http://test/empty'
    const { resolver, lister } = memResolver()
    await expect(loadLatestFileCatalogMetadata({ tableUrl, resolver, lister }))
      .rejects.toThrow(/no metadata files/)
  })
})
