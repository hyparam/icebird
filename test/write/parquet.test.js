import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { writeParquet } from '../../src/write/parquet.js'

/**
 * @import {Schema} from '../../src/types.js'
 */

describe('writeParquet', () => {
  /** @type {Schema} */
  const schema = {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'Name', required: false, type: 'string' },
      { id: 3, name: 'score', required: false, type: 'double' },
    ],
  }
  const records = [
    { id: 1n, Name: 'alice', score: 1.5 },
    { id: 2n, Name: 'bob', score: 2.5 },
    { id: 3n, Name: null, score: null },
  ]

  it('round-trips records and embeds iceberg.schema', async () => {
    const writer = new ByteWriter()
    writeParquet({ writer, schema, records })
    const file = writer.getBuffer()

    const meta = parquetMetadata(file)
    const kv = meta.key_value_metadata?.find(k => k.key === 'iceberg.schema')
    expect(kv).toBeDefined()
    expect(JSON.parse(kv?.value ?? '')).toEqual(schema)

    const rows = await parquetReadObjects({ file, compressors })
    expect(rows).toEqual([
      { id: 1n, Name: 'alice', score: 1.5 },
      { id: 2n, Name: 'bob', score: 2.5 },
      { id: 3n, Name: null, score: null },
    ])
  })

  it('throws on unsupported type', () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const bad = {
      type: 'struct',
      'schema-id': 0,
      fields: [{ id: 1, name: 'x', required: false, type: 'time' }],
    }
    expect(() => writeParquet({ writer, schema: bad, records: [] }))
      .toThrow('unsupported iceberg type: time')
  })

  it('writes a fixed-len-byte-array DECIMAL with computed type_length', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const decSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'price', required: false, type: 'decimal(10, 2)' },
        { id: 2, name: 'big', required: false, type: 'decimal(38,0)' },
      ],
    }
    writeParquet({
      writer,
      schema: decSchema,
      records: [{ price: 9.99, big: 100 }, { price: -5, big: 0 }],
    })
    const file = writer.getBuffer()
    const meta = parquetMetadata(file)
    const price = meta.schema.find(s => s.name === 'price')
    expect(price).toMatchObject({
      type: 'FIXED_LEN_BYTE_ARRAY',
      type_length: 5, // ceil(P*log2(10)/8) for P=10
      converted_type: 'DECIMAL',
      precision: 10,
      scale: 2,
    })
    expect(meta.schema.find(s => s.name === 'big')?.type_length).toBe(16)

    const rows = await parquetReadObjects({ file, compressors })
    expect(rows).toEqual([{ price: 9.99, big: 100 }, { price: -5, big: 0 }])
  })

  it('writes v3 primitive parquet types', async () => {
    const writer = new ByteWriter()
    const ts = new Date('2024-01-02T03:04:05.006Z')
    /** @type {Schema} */
    const v3Schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'ts', required: false, type: 'timestamp_ns' },
        { id: 2, name: 'tz', required: false, type: 'timestamptz_ns' },
        { id: 3, name: 'placeholder', required: false, type: 'unknown' },
      ],
    }

    writeParquet({
      writer,
      schema: v3Schema,
      records: [{ ts, tz: ts, placeholder: 'ignored' }],
    })
    const file = writer.getBuffer()
    const meta = parquetMetadata(file)

    expect(meta.schema.find(s => s.name === 'placeholder')).toBeUndefined()
    expect(meta.schema.find(s => s.name === 'ts')?.logical_type).toEqual({
      type: 'TIMESTAMP',
      isAdjustedToUTC: false,
      unit: 'NANOS',
    })
    expect(meta.schema.find(s => s.name === 'tz')?.logical_type).toEqual({
      type: 'TIMESTAMP',
      isAdjustedToUTC: true,
      unit: 'NANOS',
    })

    const rows = await parquetReadObjects({ file, compressors })
    expect(rows).toEqual([{ ts, tz: ts }])
  })

  it('writes v3 geospatial parquet logical types', () => {
    const writer = new ByteWriter()
    const point = { type: 'Point', coordinates: [30, 10] }
    /** @type {Schema} */
    const v3Schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'geom', required: false, type: 'geometry(srid:4326)' },
        { id: 2, name: 'geog', required: false, type: 'geography(srid:4326,spherical)' },
      ],
    }

    writeParquet({
      writer,
      schema: v3Schema,
      records: [{ geom: point, geog: point }],
    })
    const meta = parquetMetadata(writer.getBuffer())

    expect(meta.schema.find(s => s.name === 'geom')).toMatchObject({
      type: 'BYTE_ARRAY',
      logical_type: { type: 'GEOMETRY' },
    })
    expect(meta.schema.find(s => s.name === 'geog')).toMatchObject({
      type: 'BYTE_ARRAY',
      logical_type: { type: 'GEOGRAPHY' },
    })
  })

  it('uses write-default for missing values', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const defaultSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'tag', required: false, type: 'string', 'write-default': 'unknown' },
      ],
    }
    writeParquet({
      writer,
      schema: defaultSchema,
      records: [
        { id: 1n, tag: 'red' },
        { id: 2n },
        { id: 3n, tag: null },
      ],
    })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual([
      { id: 1n, tag: 'red' },
      { id: 2n, tag: 'unknown' },
      { id: 3n, tag: null },
    ])
  })

  it('rejects required unknown columns', () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const bad = {
      type: 'struct',
      'schema-id': 0,
      fields: [{ id: 1, name: 'placeholder', required: true, type: 'unknown' }],
    }

    expect(() => writeParquet({ writer, schema: bad, records: [] }))
      .toThrow('unsupported required iceberg type: unknown')
  })
})
