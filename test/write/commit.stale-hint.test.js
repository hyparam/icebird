// Under `conditionalCommits`, a corrupted or stale-forward
// `version-hint.text` must not block writes. The hint is a cache; the
// authoritative source is the highest committed `vN.metadata.json` on
// storage. The initial load path must self-heal via
// `loadLatestFileCatalogMetadata` rather than trust the hint.
//
// The legacy (non-conditional) catalog still treats the hint as
// authoritative for backwards compatibility. Verified by the final case.

import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
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
    { id: 2, name: 'msg', required: false, type: 'string' },
  ],
}

describe('icebergAppend with conditionalCommits tolerates a stale-forward hint', () => {
  it('recovers when version-hint.text points past the highest committed version', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stale-fwd-hint'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    await icebergCreateTable({ catalog, tableUrl, schema })
    // Corrupt the hint forward: simulates a partial deploy of a buggy
    // writer, a swallowed hint failure on a previous commit, or a foreign
    // tool that updated the hint but not the metadata file.
    files.set(`${tableUrl}/metadata/version-hint.text`, new TextEncoder().encode('500'))

    const committed = await icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, msg: 'a' }],
    })
    expect(committed.snapshots).toHaveLength(1)
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
  })

  it('two writers, one with a stale-forward hint, both eventually commit', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stale-fwd-hint-race'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    await icebergCreateTable({
      catalog, tableUrl, schema,
      properties: { 'commit.retry.min-wait-ms': '0', 'commit.retry.max-wait-ms': '0' },
    })
    files.set(`${tableUrl}/metadata/version-hint.text`, new TextEncoder().encode('999'))

    const results = await Promise.all([
      icebergAppend({ catalog, tableUrl, records: [{ id: 1n, msg: 'a' }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 2n, msg: 'b' }] }),
    ])
    expect(results).toHaveLength(2)
    expect(files.has(`${tableUrl}/metadata/v2.metadata.json`)).toBe(true)
    expect(files.has(`${tableUrl}/metadata/v3.metadata.json`)).toBe(true)
  })

  it('legacy (non-conditional) catalog still treats the hint as authoritative', async () => {
    // The self-healing load path must be gated on conditionalCommits so
    // existing callers don't silently start tolerating hint corruption.
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/stale-fwd-hint-legacy'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister })

    await icebergCreateTable({ catalog, tableUrl, schema })
    files.set(`${tableUrl}/metadata/version-hint.text`, new TextEncoder().encode('500'))

    await expect(icebergAppend({
      catalog, tableUrl, records: [{ id: 1n, msg: 'a' }],
    })).rejects.toThrow()
  })
})
