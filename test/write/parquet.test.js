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

  it('writes v3 variant parquet logical type', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const variantSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'payload', required: false, type: 'variant', 'initial-default': null, 'write-default': null },
      ],
    }
    const records = [
      { id: 1n, payload: { event: 'click', count: 1, flags: [true, null] } },
      { id: 2n, payload: null },
    ]

    writeParquet({ writer, schema: variantSchema, records })
    const file = writer.getBuffer()
    const meta = parquetMetadata(file)

    expect(meta.schema.find(s => s.name === 'payload')).toMatchObject({
      logical_type: { type: 'VARIANT' },
      num_children: 2,
    })
    const rows = await parquetReadObjects({ file, compressors })
    expect(rows).toEqual(records)
  })

  // A key whose value is `undefined` inside a variant object
  // should be omitted on round-trip not returned with null value
  it('omits undefined-valued keys from variant on round-trip', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const variantSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'attributes', required: false, type: 'variant', 'initial-default': null, 'write-default': null },
      ],
    }
    const records = [
      { id: 1n, attributes: { only_null: null, missing_key: undefined } },
    ]
    writeParquet({ writer, schema: variantSchema, records })
    const file = writer.getBuffer()
    const rows = await parquetReadObjects({ file, compressors })
    expect(rows[0].attributes).toEqual({ only_null: null })
    expect(Object.prototype.hasOwnProperty.call(rows[0].attributes, 'missing_key')).toBe(false)
  })

  it('writes date and time logical types per spec', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const dtSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'd', required: false, type: 'date' },
        { id: 2, name: 't', required: false, type: 'time' },
      ],
    }
    const day = new Date('2024-03-04T00:00:00.000Z')
    const micros = 22n * 3600n * 1_000_000n + 31n * 60n * 1_000_000n + 8n * 1_000_000n + 123_456n // 22:31:08.123456
    writeParquet({
      writer,
      schema: dtSchema,
      records: [{ d: day, t: micros }],
    })
    const file = writer.getBuffer()
    const meta = parquetMetadata(file)

    expect(meta.schema.find(s => s.name === 'd')).toMatchObject({
      type: 'INT32',
      converted_type: 'DATE',
      logical_type: { type: 'DATE' },
    })
    expect(meta.schema.find(s => s.name === 't')).toMatchObject({
      type: 'INT64',
      converted_type: 'TIME_MICROS',
      logical_type: { type: 'TIME', isAdjustedToUTC: false, unit: 'MICROS' },
    })

    const rows = await parquetReadObjects({ file, compressors })
    expect(rows).toEqual([{ d: day, t: micros }])
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
