// Iceberg snapshot ids are 64-bit longs. Icebird's metadata reader parses
// integers above Number.MAX_SAFE_INTEGER (2^53-1) as BigInt to avoid lossy
// doubles. The write path must round-trip those BigInts: a plain
// `JSON.stringify` throws "Do not know how to serialize a BigInt", and a
// naive replacer that emits a quoted string would corrupt the metadata
// (Iceberg expects bare JSON numbers). These tests pin the contract that a
// commit re-serializing loaded metadata with a snapshot id > 2^53 succeeds
// and preserves the id exactly.

import { describe, expect, it } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { loadLatestFileCatalogMetadata } from '../../src/metadata.js'
import { icebergAppend, icebergCreateTable, icebergExpireSnapshots } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema} from '../../src/types.js'
 */

// > 2^53 (9007199254740992) and < 2^63, so it is a valid 64-bit long that a
// double cannot represent. parseIcebergJson keeps it as a BigInt.
const BIG_ID = 9151314442816847871n

/** @type {Schema} */
const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'msg', required: false, type: 'string' },
  ],
}

/**
 * Rewrite every bare-number occurrence of `from` to `to` in the latest
 * on-disk metadata file, simulating a table whose snapshot id exceeds 2^53.
 * Operates on the raw JSON text so it does not depend on the serializer
 * under test. Word boundaries avoid rewriting a coincidental substring of a
 * larger number (e.g. a timestamp).
 *
 * @param {Map<string, Uint8Array>} files
 * @param {string} tableUrl
 * @param {bigint} from
 * @param {bigint} to
 */
function rewriteSnapshotId(files, tableUrl, from, to) {
  const dir = `${tableUrl}/metadata/`
  let latestKey = ''
  let latestVersion = -1
  for (const key of files.keys()) {
    const m = key.startsWith(dir) && key.match(/\/v(\d+)\.metadata\.json$/)
    if (m && Number(m[1]) > latestVersion) {
      latestVersion = Number(m[1])
      latestKey = key
    }
  }
  if (!latestKey) throw new Error('no metadata file found')
  const text = new TextDecoder().decode(/** @type {Uint8Array} */ (files.get(latestKey)))
  // Only rewrite bare JSON number values (`"key": <id>`), never digits inside
  // a quoted string such as the `manifest-list` path (`.../snap-<id>-...avro`),
  // whose physical file on disk keeps the original id in its name.
  const re = new RegExp(`(?<=: )${from.toString()}(?=[,\\n}\\]])`, 'g')
  const rewritten = text.replace(re, to.toString())
  files.set(latestKey, new TextEncoder().encode(rewritten))
}

describe('commit round-trips snapshot ids above 2^53', () => {
  it('expireSnapshots preserves a surviving snapshot id > 2^53', async () => {
    const tableUrl = 'http://test/bigint-expire'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister })

    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, msg: 'a' }] })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, msg: 'b' }] })

    // Promote the current (newest) snapshot's id above 2^53 on disk.
    const before = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const currentId = before.metadata['current-snapshot-id']
    if (currentId == null) throw new Error('expected a current snapshot')
    const olderId = (before.metadata.snapshots ?? [])
      .map(s => s['snapshot-id'])
      .find(id => BigInt(id) !== BigInt(currentId))
    if (olderId == null) throw new Error('expected two snapshots')
    rewriteSnapshotId(files, tableUrl, BigInt(currentId), BIG_ID)

    // Expiring the older snapshot re-serializes metadata that still holds the
    // BIG_ID snapshot. Before the fix this threw on JSON.stringify(BigInt).
    await icebergExpireSnapshots({ catalog, tableUrl, snapshotIds: [olderId] })

    const after = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const ids = (after.metadata.snapshots ?? []).map(s => s['snapshot-id'])
    expect(ids).toEqual([BIG_ID])
    expect(after.metadata['current-snapshot-id']).toBe(BIG_ID)
    expect(after.metadata.refs?.main?.['snapshot-id']).toBe(BIG_ID)
  })

  it('append records parent-snapshot-id > 2^53 without precision loss', async () => {
    const tableUrl = 'http://test/bigint-append'
    const { resolver, files, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister })

    await icebergCreateTable({ catalog, tableUrl, schema })
    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, msg: 'a' }] })

    // Promote the only snapshot's id above 2^53 on disk. A subsequent append
    // will reference it as parent-snapshot-id (a BigInt), which the commit
    // must serialize as a bare JSON number.
    const before = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const parentId = before.metadata['current-snapshot-id']
    if (parentId == null) throw new Error('expected a current snapshot')
    rewriteSnapshotId(files, tableUrl, BigInt(parentId), BIG_ID)

    await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, msg: 'b' }] })

    const after = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const child = (after.metadata.snapshots ?? [])
      .find(s => BigInt(s['snapshot-id']) !== BIG_ID)
    if (!child) throw new Error('expected the appended snapshot')
    // Exact BigInt equality proves the parent id was not coerced to a double.
    expect(child['parent-snapshot-id']).toBe(BIG_ID)
  })
})
