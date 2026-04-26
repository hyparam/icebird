import { beforeEach, describe, expect, it, vi } from 'vitest'
import { icebergCreate } from '../src/create.js'
import * as fetchModule from '../src/fetch.js'
import { ByteWriter } from 'hyparquet-writer'

describe('createIceberg', () => {
  const tableUrl = 's3://test-bucket/table-path'
  const translatedTableUrl = 'https://test-bucket.s3.amazonaws.com/table-path'

  beforeEach(() => {
    vi.spyOn(fetchModule, 'translateS3Url').mockImplementation(url => {
      return url.replace('s3://', 'https://').replace('test-bucket/', 'test-bucket.s3.amazonaws.com/')
    })
    vi.spyOn(crypto, 'randomUUID').mockReturnValue('test-uuid-1234-5678-90ab-cdef12345678')
    vi.spyOn(Date, 'now').mockReturnValue(1609459200000) // 2021-01-01T00:00:00.000Z
  })

  it('creates a new Iceberg table', async () => {
    /** @type {Record<string, ByteWriter>} */
    const writers = {}
    const writer = vi.fn(path => {
      writers[path] = new ByteWriter()
      return writers[path]
    })
    const resolver = { reader: vi.fn(), writer }
    const metadata = await icebergCreate({ tableUrl, resolver })

    // Check the metadata structure
    expect(metadata).toMatchObject({
      'format-version': 2,
      'table-uuid': 'test-uuid-1234-5678-90ab-cdef12345678',
      location: 's3://test-bucket/table-path',
      'last-sequence-number': 0,
      'last-updated-ms': 1609459200000,
      'last-column-id': 0,
      'current-schema-id': 0,
      schemas: [
        { fields: [], 'schema-id': 0, type: 'struct' },
      ],
      'default-spec-id': 0,
      'partition-specs': [
        { fields: [], 'spec-id': 0 },
      ],
      'last-partition-id': 0,
      'sort-orders': [
        { fields: [], 'order-id': 0 },
      ],
      'default-sort-order-id': 0,
    })

    // Check that the writer was called correctly
    expect(writer).toHaveBeenCalledWith(`${translatedTableUrl}/metadata/v1.metadata.json`)
    expect(writer).toHaveBeenCalledWith(`${translatedTableUrl}/version-hint.text`)
    expect(writers[`${translatedTableUrl}/metadata/v1.metadata.json`].offset).toBe(573)
    expect(writers[`${translatedTableUrl}/version-hint.text`].offset).toBe(1)

  })

  it('creates a table with a provided schema', async () => {
    /** @type {Record<string, ByteWriter>} */
    const writers = {}
    const writer = vi.fn(path => {
      writers[path] = new ByteWriter()
      return writers[path]
    })
    const resolver = { reader: vi.fn(), writer }
    /** @type {import('../src/types.js').Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'name', required: false, type: 'string' },
      ],
    }
    const metadata = await icebergCreate({ tableUrl, resolver, schema })

    expect(metadata.schemas).toEqual([schema])
    expect(metadata['last-column-id']).toBe(2)
    expect(metadata['current-schema-id']).toBe(0)
  })

  it('throws an error if tableUrl is not provided', async () => {
    const resolver = { reader: vi.fn(), writer: vi.fn() }
    await expect(icebergCreate({ tableUrl: '', resolver })).rejects.toThrow('tableUrl is required')
  })
})
