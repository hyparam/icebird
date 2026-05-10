// Snapshot ids must be unique within `metadata.snapshots`. The 53-bit
// space used by `newSnapshotId()` makes birthday collisions plausible at
// LLM-logging scale (~5e-5 at 1M snapshots, ~5% at 1B), and a duplicate
// id silently corrupts ref resolution and read planning. The writer
// must either re-roll on collision or have `applyUpdates` reject the
// duplicate. These tests pin the contract from both angles.

import { describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { applyUpdates } from '../../src/write/commit.js'
import { icebergAppend, icebergCreateTable } from '../../src/write/write.js'
import { memResolver } from '../helpers.js'

/**
 * @import {Schema, Snapshot, TableMetadata} from '../../src/types.js'
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

describe('applyUpdates rejects duplicate snapshot ids', () => {
  it('throws when add-snapshot reuses an id already present in snapshots', () => {
    /** @type {Snapshot} */
    const existing = {
      'snapshot-id': 12345,
      'sequence-number': 1,
      'timestamp-ms': 1700000000000,
      'manifest-list': 's3://x/snap-1.avro',
      summary: { operation: 'append' },
      'schema-id': 0,
    }
    /** @type {TableMetadata} */
    const metadata = {
      'format-version': 2,
      'table-uuid': '00000000-0000-0000-0000-000000000000',
      location: 's3://x',
      'last-sequence-number': 1,
      'last-updated-ms': 1700000000000,
      'last-column-id': 2,
      'current-schema-id': 0,
      schemas: [schema],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
      snapshots: [existing],
      'current-snapshot-id': 12345,
      refs: { main: { 'snapshot-id': 12345, type: 'branch' } },
    }

    /** @type {Snapshot} */
    const dup = {
      ...existing,
      'sequence-number': 2,
      'timestamp-ms': 1700000001000,
      'manifest-list': 's3://x/snap-2.avro',
    }

    expect(() => applyUpdates(metadata, [{ action: 'add-snapshot', snapshot: dup }]))
      .toThrow(/duplicate snapshot-id|snapshot-id .* already exists/i)
  })
})

describe('icebergAppend never produces duplicate snapshot ids', () => {
  it('three concurrent appends with a constant RNG still produce unique ids', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/snap-id-collision'
    const { resolver } = memResolver()
    const catalog = fileCatalog({ resolver, conditionalCommits: true })

    // Pin globalThis.crypto.getRandomValues so newSnapshotId() always
    // sees the same raw bytes. A correct implementation must either
    // re-roll until the id is unused, or throw on conflict. Either way
    // the surviving snapshots[] list must have no duplicates.
    const real = globalThis.crypto.getRandomValues.bind(globalThis.crypto)
    /**
     * @param {any} buf
     * @returns {any}
     */
    function constantRng(buf) {
      if (buf instanceof BigInt64Array) {
        buf[0] = 0x0001020304050607n
        return buf
      }
      return real(buf)
    }
    const spy = vi.spyOn(globalThis.crypto, 'getRandomValues').mockImplementation(constantRng)
    try {
      await icebergCreateTable({ catalog, tableUrl, schema })
      /** @type {PromiseSettledResult<TableMetadata>[]} */
      const results = await Promise.allSettled([
        icebergAppend({ catalog, tableUrl, records: [{ id: 1n, msg: 'a' }] }),
        icebergAppend({ catalog, tableUrl, records: [{ id: 2n, msg: 'b' }] }),
        icebergAppend({ catalog, tableUrl, records: [{ id: 3n, msg: 'c' }] }),
      ])
      const last = /** @type {PromiseFulfilledResult<TableMetadata>|undefined} */ (
        [...results].reverse().find(r => r.status === 'fulfilled')
      )
      if (!last) throw new Error('expected at least one append to succeed')
      const ids = (last.value.snapshots ?? []).map(s => s['snapshot-id'])
      expect(new Set(ids).size).toBe(ids.length)
    } finally {
      spy.mockRestore()
    }
  })

  it('sequential appends with a constant RNG never produce duplicates', async () => {
    // Catches a fix that only de-dupes within a single in-process commit
    // but lets a fresh process re-collide with an id already on disk.
    // With a constant RNG the second append must either pick a fresh id
    // (re-roll succeeded) or throw (re-roll exhausted), never produce a
    // duplicate snapshot id in the persisted metadata.
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/snap-id-collision-seq'
    const { resolver, lister } = memResolver()
    const catalog = fileCatalog({ resolver, lister, conditionalCommits: true })

    const real = globalThis.crypto.getRandomValues.bind(globalThis.crypto)
    /**
     * @param {any} buf
     * @returns {any}
     */
    function constantRng(buf) {
      if (buf instanceof BigInt64Array) {
        buf[0] = 0x0001020304050607n
        return buf
      }
      return real(buf)
    }
    const spy = vi.spyOn(globalThis.crypto, 'getRandomValues').mockImplementation(constantRng)
    try {
      await icebergCreateTable({ catalog, tableUrl, schema })
      await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, msg: 'a' }] })
      // Second append may succeed or throw; what matters is no duplicate
      // snapshot id ends up persisted.
      try {
        await icebergAppend({ catalog, tableUrl, records: [{ id: 2n, msg: 'b' }] })
      } catch { /* re-roll exhausted is acceptable */ }
    } finally {
      spy.mockRestore()
    }

    const { loadLatestFileCatalogMetadata } = await import('../../src/metadata.js')
    const latest = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const ids = (latest.metadata.snapshots ?? []).map(s => s['snapshot-id'])
    expect(new Set(ids).size).toBe(ids.length)
  })
})
