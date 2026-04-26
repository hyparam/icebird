import { describe, expect, it, vi } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { icebergAppend } from '../../src/write/append.js'
import { icebergCreate } from '../../src/create.js'
import { icebergRead } from '../../src/read.js'

/**
 * @import {Resolver, Schema} from '../../src/types.js'
 */

/**
 * Create an in-memory resolver pair backed by a single map.
 * The writer captures bytes per path; the reader serves them back as AsyncBuffer.
 *
 * @returns {{ resolver: Resolver, files: Map<string, Uint8Array> }}
 */
function memResolver() {
  /** @type {Map<string, Uint8Array>} */
  const files = new Map()
  /** @type {Resolver} */
  const resolver = {
    reader(path) {
      const bytes = files.get(path)
      if (!bytes) throw new Error(`no such file: ${path}`)
      const ab = /** @type {ArrayBuffer} */ (
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
      )
      return {
        byteLength: bytes.byteLength,
        slice: (/** @type {number} */ s, /** @type {number} */ e) => ab.slice(s, e),
      }
    },
    writer(path) {
      const w = new ByteWriter()
      const origFinish = w.finish.bind(w)
      w.finish = () => {
        origFinish()
        files.set(path, w.getBytes())
      }
      return w
    },
  }
  return { resolver, files }
}

describe('icebergAppend', () => {
  /** @type {Schema} */
  const schema = {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: false, type: 'string' },
    ],
  }

  it('appends a snapshot that round-trips through icebergRead', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    // use a non-s3 url so translateS3Url is a no-op
    const tableUrl = 'http://test/table'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })

    const records = [
      { id: 1n, name: 'alice' },
      { id: 2n, name: 'bob' },
    ]
    const v2 = await icebergAppend({ tableUrl, metadata: created, records, resolver })

    expect(v2['current-snapshot-id']).toBeGreaterThan(0)
    expect(v2['last-sequence-number']).toBe(1)
    expect(v2.snapshots).toHaveLength(1)
    expect(v2.snapshots?.[0].summary).toMatchObject({
      operation: 'append',
      'added-records': '2',
      'total-records': '2',
      'total-data-files': '1',
    })

    const read = await icebergRead({ tableUrl, metadata: v2, resolver })
    expect(read).toEqual(records)
  })

  it('supports a second append carrying forward prior manifests', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1700000000000)
    const tableUrl = 'http://test/table2'
    const { resolver } = memResolver()

    const created = await icebergCreate({ tableUrl, resolver, schema })
    const v2 = await icebergAppend({
      tableUrl, metadata: created, resolver,
      records: [{ id: 1n, name: 'alice' }],
    })
    const v3 = await icebergAppend({
      tableUrl, metadata: v2, resolver,
      records: [{ id: 2n, name: 'bob' }, { id: 3n, name: 'carol' }],
    })

    expect(v3.snapshots).toHaveLength(2)
    expect(v3['last-sequence-number']).toBe(2)
    expect(v3.snapshots?.[1].summary['total-records']).toBe('3')
    expect(v3.snapshots?.[1]['parent-snapshot-id']).toBe(v2['current-snapshot-id'])

    const read = await icebergRead({ tableUrl, metadata: v3, resolver })
    expect(read).toEqual([
      { id: 2n, name: 'bob' },
      { id: 3n, name: 'carol' },
      { id: 1n, name: 'alice' },
    ])
  })

  it('throws if resolver.writer is missing', async () => {
    const tableUrl = 'http://test/table3'
    /** @type {Resolver} */
    const resolver = {
      reader() { throw new Error('unused') },
    }
    await expect(icebergAppend({
      tableUrl,
      metadata: /** @type {any} */ ({ 'format-version': 2 }),
      records: [],
      resolver,
    })).rejects.toThrow('resolver.writer is required')
  })
})
