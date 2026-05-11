// `icebergAppend` splits staging into a one-shot prepare phase (parquet
// data files + data manifest) and a per-attempt stage phase (manifest list
// + metadata.json). Per the v3 spec's "Manifest Inheritance" section, data
// and manifest files do NOT need to be rewritten on optimistic-commit
// retry, so the retry loop must reuse them. These tests pin that contract:
// no parquet/manifest blow-up under contention, and a forced retry only
// re-PUTs the manifest list.

import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { icebergRead } from '../../src/read.js'
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

describe('icebergAppend prepare-once', () => {
  it('10 concurrent appends produce exactly 10 parquet files and 10 manifests', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/prepare-once-no-blowup'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })
    await icebergCreateTable({
      catalog, tableUrl, schema,
      properties: { 'commit.retry.min-wait-ms': '0', 'commit.retry.max-wait-ms': '0' },
    })

    const N = 10
    await Promise.all(
      Array.from({ length: N }, (_, i) =>
        icebergAppend({ catalog, tableUrl, records: [{ id: BigInt(i), msg: `m${i}` }] })
      )
    )

    const dataFiles = [...files.keys()].filter(k => k.startsWith(`${tableUrl}/data/`))
    const manifests = [...files.keys()].filter(k => /\/metadata\/[0-9a-f-]+-m0\.avro$/.test(k))
    const lists = [...files.keys()].filter(k => /\/metadata\/snap-/.test(k))
    const committed = [...files.keys()].filter(k => /\/metadata\/v\d+\.metadata\.json$/.test(k))

    expect(committed).toHaveLength(N + 1)
    expect(dataFiles).toHaveLength(N)
    expect(manifests).toHaveLength(N)
    // Manifest lists CAN exceed N: each retry writes a fresh one. They are
    // small (one row per prior manifest plus one per new manifest).
    expect(lists.length).toBeGreaterThanOrEqual(N)
    expect(lists.length).toBeLessThan(N * N)
  })

  it('parquet bytes are PUT exactly once across a forced retry', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/prepare-once-forced-retry'
    const { resolver, files } = memResolver()
    const realWriter = resolver.writer
    if (!realWriter) throw new Error('writer required')

    const catalog = fileCatalog({ resolver, conditionalCommits: true })
    await icebergCreateTable({
      catalog, tableUrl, schema,
      properties: { 'commit.retry.min-wait-ms': '0', 'commit.retry.max-wait-ms': '0' },
    })
    const v1 = files.get(`${tableUrl}/metadata/v1.metadata.json`)
    if (!v1) throw new Error('v1 missing')

    /** @type {string[]} */
    const allPuts = []
    let plantedOnce = false
    resolver.writer = (p, options) => {
      // Plant a foreign v2 right before our first conditional PUT lands,
      // forcing exactly one retry.
      if (p.endsWith('/metadata/v2.metadata.json') && options?.ifNoneMatch === '*' && !plantedOnce) {
        plantedOnce = true
        files.set(p, v1)
      }
      const w = realWriter(p, options)
      const orig = w.finish.bind(w)
      w.finish = async () => {
        await orig()
        allPuts.push(p)
      }
      return w
    }

    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, msg: 'a' }] })

    const dataPuts = allPuts.filter(p => p.startsWith(`${tableUrl}/data/`))
    const manifestPuts = allPuts.filter(p => /\/metadata\/[0-9a-f-]+-m0\.avro$/.test(p))
    const listPuts = allPuts.filter(p => /\/metadata\/snap-/.test(p))

    expect(plantedOnce).toBe(true)
    // Pre-fix would have shown 2 data PUTs and 2 manifest PUTs.
    expect(dataPuts).toHaveLength(1)
    expect(manifestPuts).toHaveLength(1)
    // The manifest list IS rewritten per attempt by design.
    expect(listPuts.length).toBeGreaterThanOrEqual(2)
  })

  it('rows are readable after a retry-driven append', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/prepare-once-roundtrip'
    const { resolver, files } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })
    await icebergCreateTable({
      catalog, tableUrl, schema,
      properties: { 'commit.retry.min-wait-ms': '0', 'commit.retry.max-wait-ms': '0' },
    })

    // Race two appends so one of them definitely retries.
    await Promise.all([
      icebergAppend({ catalog, tableUrl, records: [{ id: 1n, msg: 'a' }] }),
      icebergAppend({ catalog, tableUrl, records: [{ id: 2n, msg: 'b' }] }),
    ])

    const rows = await icebergRead({ tableUrl, resolver })
    const ids = rows.map(r => Number(r.id)).sort()
    expect(ids).toEqual([1, 2])
    // Sanity: exactly two parquet files (one per writer, no orphans).
    const dataFiles = [...files.keys()].filter(k => k.startsWith(`${tableUrl}/data/`))
    expect(dataFiles).toHaveLength(2)
  })
})
