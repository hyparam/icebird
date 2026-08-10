// The spec requires `parent-snapshot-id` to be "Omitted for any snapshot with
// no parent", and separately documents that Java writes the sentinel
// `"current-snapshot-id": -1` for "no current snapshot" on v1/v2 tables,
// which other implementations should read as null. Passing the sentinel
// through gave the first snapshot appended to such a table a
// `parent-snapshot-id` of -1 — a parent that cannot exist — and sent
// `assert-ref-snapshot-id: -1` with the commit, which the REST spec reads as
// "main must reference snapshot -1" where the intent is null ("the ref must
// not already exist"). Empty tables written by Java, Spark and Athena all
// carry the sentinel; see the v1 metadata under test/files/hyperparam-iceberg.
// These tests pin that it is normalized on both commit paths.

import { afterEach, describe, expect, it, vi } from 'vitest'
import { fileCatalog } from '../../src/catalog/file.js'
import { restCatalogConnect } from '../../src/catalog/rest.js'
import { loadLatestFileCatalogMetadata } from '../../src/metadata.js'
import { icebergAppend } from '../../src/write/write.js'
import { makeFetch } from '../catalog.rest.helpers.js'
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

/**
 * Metadata for an empty table, shaped as the Java writer emits it: the
 * sentinel `-1` alongside an empty `refs` and `snapshots`.
 *
 * @param {string} tableUrl
 * @returns {Record<string, any>}
 */
function emptyTableMetadata(tableUrl) {
  return {
    'format-version': 2,
    'table-uuid': 'a1b2c3d4-0000-0000-0000-000000000001',
    location: tableUrl,
    'last-sequence-number': 0,
    'last-updated-ms': 1700000000000,
    'last-column-id': 2,
    'current-schema-id': 0,
    schemas: [schema],
    'default-spec-id': 0,
    'partition-specs': [{ 'spec-id': 0, fields: [] }],
    'last-partition-id': 999,
    'default-sort-order-id': 0,
    'sort-orders': [{ 'order-id': 0, fields: [] }],
    properties: {},
    'current-snapshot-id': -1,
    refs: {},
    snapshots: [],
    'snapshot-log': [],
    'metadata-log': [],
  }
}

describe('current-snapshot-id -1 sentinel', () => {
  afterEach(() => { vi.unstubAllGlobals() })

  it('file catalog: first append omits parent-snapshot-id', async () => {
    const tableUrl = 'http://test/sentinel-file'
    const { resolver, files, lister } = memResolver()
    files.set(`${tableUrl}/metadata/v1.metadata.json`,
      new TextEncoder().encode(JSON.stringify(emptyTableMetadata(tableUrl))))
    files.set(`${tableUrl}/metadata/version-hint.text`, new TextEncoder().encode('1'))
    const catalog = fileCatalog({ resolver, lister })

    await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, msg: 'a' }] })

    const after = await loadLatestFileCatalogMetadata({ tableUrl, resolver, lister })
    const snapshots = after.metadata.snapshots ?? []
    expect(snapshots).toHaveLength(1)
    // Absent, not -1: the first snapshot of a table has no parent.
    expect('parent-snapshot-id' in snapshots[0]).toBe(false)
  })

  it('rest catalog: first append asserts the main ref as null', async () => {
    const tableUrl = 'http://test/sentinel-rest'
    const { resolver } = memResolver()
    const mock = makeFetch({
      'https://cat/v1/config': {},
      'https://cat/v1/namespaces/db/tables/orders': {
        'metadata-location': `${tableUrl}/metadata/v1.metadata.json`,
        metadata: emptyTableMetadata(tableUrl),
      },
    })
    vi.stubGlobal('fetch', mock.fn)

    const catalog = await restCatalogConnect({ url: 'https://cat' })
    await icebergAppend({
      catalog, namespace: 'db', table: 'orders', resolver,
      records: [{ id: 1n, msg: 'a' }],
    })

    const post = mock.calls.find(c => c.init?.method === 'POST')
    const body = JSON.parse(/** @type {string} */ (post?.init?.body))
    const ref = body.requirements
      .find((/** @type {any} */ r) => r.type === 'assert-ref-snapshot-id')
    // A catalog reads -1 as "main must point at snapshot -1", which never
    // holds; null is the spec's "main must not exist yet".
    expect(ref['snapshot-id']).toBe(null)
    const added = body.updates
      .find((/** @type {any} */ u) => u.action === 'add-snapshot')
    expect('parent-snapshot-id' in added.snapshot).toBe(false)
  })
})
