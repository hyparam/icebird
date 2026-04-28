import { beforeEach, describe, expect, it, vi } from 'vitest'
import { icebergCreate } from '../src/create.js'
import * as fetchModule from '../src/fetch.js'
import { ByteWriter } from 'hyparquet-writer'

/**
 * @import {Schema} from '../src/types.js'
 */

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
    expect(writer).toHaveBeenCalledWith(`${translatedTableUrl}/metadata/version-hint.text`)
    expect(writers[`${translatedTableUrl}/metadata/v1.metadata.json`].offset).toBe(573)
    expect(writers[`${translatedTableUrl}/metadata/version-hint.text`].offset).toBe(1)

  })

  it('creates a table with a provided schema', async () => {
    /** @type {Record<string, ByteWriter>} */
    const writers = {}
    const writer = vi.fn(path => {
      writers[path] = new ByteWriter()
      return writers[path]
    })
    const resolver = { reader: vi.fn(), writer }
    /** @type {Schema} */
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

  it('persists properties, partitionSpec, and sortOrder', async () => {
    const writer = vi.fn(() => new ByteWriter())
    const resolver = { reader: vi.fn(), writer }
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'ts', required: false, type: 'timestamp' },
      ],
    }
    const metadata = await icebergCreate({
      tableUrl,
      resolver,
      schema,
      partitionSpec: {
        'spec-id': 0,
        fields: [{ 'source-id': 2, 'field-id': 1000, name: 'ts_day', transform: 'day' }],
      },
      sortOrder: {
        'order-id': 1,
        fields: [{ transform: 'identity', 'source-id': 1, direction: 'asc', 'null-order': 'nulls-first' }],
      },
      properties: { 'write.format.default': 'parquet', 'write.parquet.compression-codec': 'zstd' },
    })

    expect(metadata['partition-specs'][0].fields).toHaveLength(1)
    expect(metadata['last-partition-id']).toBe(1000)
    expect(metadata['default-sort-order-id']).toBe(1)
    expect(metadata['sort-orders'][0].fields).toHaveLength(1)
    expect(metadata.properties).toEqual({
      'write.format.default': 'parquet',
      'write.parquet.compression-codec': 'zstd',
    })
  })

  it('creates a v3 table with next-row-id', async () => {
    const writer = vi.fn(() => new ByteWriter())
    const resolver = { reader: vi.fn(), writer }
    const metadata = await icebergCreate({ tableUrl, resolver, formatVersion: 3 })

    expect(metadata['format-version']).toBe(3)
    expect(metadata['next-row-id']).toBe(0)
  })

  it('honors format-version from properties', async () => {
    const writer = vi.fn(() => new ByteWriter())
    const resolver = { reader: vi.fn(), writer }
    const metadata = await icebergCreate({
      tableUrl,
      resolver,
      properties: { 'format-version': '3' },
    })

    expect(metadata['format-version']).toBe(3)
    expect(metadata['next-row-id']).toBe(0)
  })

  it('formatVersion arg overrides properties', async () => {
    const writer = vi.fn(() => new ByteWriter())
    const resolver = { reader: vi.fn(), writer }
    const metadata = await icebergCreate({
      tableUrl,
      resolver,
      formatVersion: 2,
      properties: { 'format-version': '3' },
    })

    expect(metadata['format-version']).toBe(2)
  })

  it('throws on unsupported format-version property', async () => {
    const resolver = { reader: vi.fn(), writer: vi.fn() }
    await expect(icebergCreate({
      tableUrl,
      resolver,
      properties: { 'format-version': '1' },
    })).rejects.toThrow('unsupported format-version: 1')
  })

  it('throws on unsupported format-version', async () => {
    const resolver = { reader: vi.fn(), writer: vi.fn() }
    // @ts-expect-error testing invalid input
    await expect(icebergCreate({ tableUrl, resolver, formatVersion: 1 }))
      .rejects.toThrow('unsupported format-version: 1')
  })

  it.each([
    'unknown',
    'variant',
    'timestamp_ns',
    'timestamptz_ns',
    'geometry',
    'geometry(srid:4326)',
    'geography',
    'geography(srid:4326)',
  ])('rejects v3-only type %s in a v2 table', async type => {
    const resolver = { reader: vi.fn(), writer: vi.fn() }
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [{ id: 1, name: 'col', required: false, type: /** @type {any} */ (type) }],
    }
    await expect(icebergCreate({ tableUrl, resolver, schema }))
      .rejects.toThrow(`type ${type} requires format-version 3`)
  })

  it('rejects v3-only type nested inside a v2 schema', async () => {
    const resolver = { reader: vi.fn(), writer: vi.fn() }
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [{
        id: 1,
        name: 'tags',
        required: false,
        type: { type: 'list', 'element-id': 2, 'element-required': false, element: 'variant' },
      }],
    }
    await expect(icebergCreate({ tableUrl, resolver, schema }))
      .rejects.toThrow('type variant requires format-version 3 (field: tags.element)')
  })

  it('allows v3-only types in a v3 table', async () => {
    const writer = vi.fn(() => new ByteWriter())
    const resolver = { reader: vi.fn(), writer }
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'v', required: false, type: 'variant' },
        { id: 2, name: 'ts', required: false, type: 'timestamp_ns' },
      ],
    }
    const metadata = await icebergCreate({ tableUrl, resolver, schema, formatVersion: 3 })
    expect(metadata['format-version']).toBe(3)
    expect(metadata.schemas[0].fields).toHaveLength(2)
  })
})
