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

  it('stamps iceberg field_id on top-level parquet columns', () => {
    const writer = new ByteWriter()
    writeParquet({ writer, schema, records })
    const meta = parquetMetadata(writer.getBuffer())
    expect(meta.schema.find(s => s.name === 'id')?.field_id).toBe(1)
    expect(meta.schema.find(s => s.name === 'Name')?.field_id).toBe(2)
    expect(meta.schema.find(s => s.name === 'score')?.field_id).toBe(3)
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

  it('writes a list<long> column with 3-level LIST structure and stamped element-id', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const listSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        {
          id: 2,
          name: 'scores',
          required: false,
          type: { type: 'list', 'element-id': 3, 'element-required': false, element: 'long' },
        },
      ],
    }
    const records = [
      { id: 1n, scores: [10n, 20n] },
      { id: 2n, scores: [] },
      { id: 3n, scores: null },
      { id: 4n, scores: [99n, null, 1n] },
    ]
    writeParquet({ writer, schema: listSchema, records })
    const file = writer.getBuffer()
    const meta = parquetMetadata(file)

    const rows = await parquetReadObjects({ file, compressors })
    expect(rows[0]).toEqual({ id: 1n, scores: [10n, 20n] })
    expect(rows[1]).toEqual({ id: 2n, scores: [] })
    expect(rows[2].scores ?? null).toBeNull()
    expect(rows[3]).toEqual({ id: 4n, scores: [99n, null, 1n] })

    const scores = meta.schema.find(s => s.name === 'scores')
    expect(scores).toMatchObject({
      converted_type: 'LIST',
      logical_type: { type: 'LIST' },
      repetition_type: 'OPTIONAL',
      num_children: 1,
      field_id: 2,
    })
    const repeated = meta.schema.find(s => s.name === 'list')
    expect(repeated).toMatchObject({ repetition_type: 'REPEATED', num_children: 1 })
    const element = meta.schema.find(s => s.name === 'element')
    expect(element).toMatchObject({ type: 'INT64', repetition_type: 'OPTIONAL', field_id: 3 })
  })

  it('writes a required list<string> with required elements', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const listSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        {
          id: 1,
          name: 'tags',
          required: true,
          type: { type: 'list', 'element-id': 2, 'element-required': true, element: 'string' },
        },
      ],
    }
    const records = [
      { tags: ['a', 'b'] },
      { tags: ['c'] },
    ]
    writeParquet({ writer, schema: listSchema, records })
    const file = writer.getBuffer()
    const meta = parquetMetadata(file)

    expect(meta.schema.find(s => s.name === 'tags')?.repetition_type).toBe('REQUIRED')
    expect(meta.schema.find(s => s.name === 'element')).toMatchObject({
      type: 'BYTE_ARRAY',
      converted_type: 'UTF8',
      repetition_type: 'REQUIRED',
      field_id: 2,
    })

    const rows = await parquetReadObjects({ file, compressors })
    expect(rows).toEqual(records)
  })

  it('writes nested list<list<int>>', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const listSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        {
          id: 1,
          name: 'matrix',
          required: false,
          type: {
            type: 'list',
            'element-id': 2,
            'element-required': false,
            element: { type: 'list', 'element-id': 3, 'element-required': false, element: 'int' },
          },
        },
      ],
    }
    const records = [
      { matrix: [[1, 2], [3]] },
      { matrix: [] },
      { matrix: null },
    ]
    writeParquet({ writer, schema: listSchema, records })
    const file = writer.getBuffer()
    const rows = await parquetReadObjects({ file, compressors })
    expect(rows[0]).toEqual({ matrix: [[1, 2], [3]] })
    expect(rows[1]).toEqual({ matrix: [] })
    expect(rows[2].matrix ?? null).toBeNull()
  })

  it('writes a map<string,int> column with 3-level MAP structure', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const mapSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        {
          id: 2,
          name: 'props',
          required: false,
          type: {
            type: 'map',
            'key-id': 3,
            key: 'string',
            'value-id': 4,
            'value-required': false,
            value: 'int',
          },
        },
      ],
    }
    const records = [
      { id: 1n, props: { a: 1, b: 2 } },
      { id: 2n, props: {} },
      { id: 3n, props: null },
      { id: 4n, props: { only: 7 } },
    ]
    writeParquet({ writer, schema: mapSchema, records })
    const file = writer.getBuffer()
    const meta = parquetMetadata(file)

    expect(meta.schema.find(s => s.name === 'props')).toMatchObject({
      converted_type: 'MAP',
      logical_type: { type: 'MAP' },
      repetition_type: 'OPTIONAL',
      num_children: 1,
      field_id: 2,
    })
    expect(meta.schema.find(s => s.name === 'key_value')).toMatchObject({
      repetition_type: 'REPEATED',
      num_children: 2,
    })
    expect(meta.schema.find(s => s.name === 'key')).toMatchObject({
      type: 'BYTE_ARRAY',
      converted_type: 'UTF8',
      repetition_type: 'REQUIRED',
      field_id: 3,
    })
    expect(meta.schema.find(s => s.name === 'value')).toMatchObject({
      type: 'INT32',
      repetition_type: 'OPTIONAL',
      field_id: 4,
    })

    const rows = await parquetReadObjects({ file, compressors })
    expect(rows[0]).toEqual({ id: 1n, props: { a: 1, b: 2 } })
    expect(rows[1]).toEqual({ id: 2n, props: {} })
    expect(rows[2].props ?? null).toBeNull()
    expect(rows[3]).toEqual({ id: 4n, props: { only: 7 } })
  })

  it('accepts ES Map and array-of-pairs inputs for map columns', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const mapSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        {
          id: 1,
          name: 'm',
          required: true,
          type: {
            type: 'map',
            'key-id': 2,
            key: 'string',
            'value-id': 3,
            'value-required': true,
            value: 'long',
          },
        },
      ],
    }
    const records = [
      { m: new Map([['x', 1n], ['y', 2n]]) },
      { m: [['k1', 10n], ['k2', 20n]] },
      { m: [{ key: 'k', value: 99n }] },
    ]
    writeParquet({ writer, schema: mapSchema, records })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual([
      { m: { x: 1n, y: 2n } },
      { m: { k1: 10n, k2: 20n } },
      { m: { k: 99n } },
    ])
  })

  it('writes nested list<map<string,int>>', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const nestedSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        {
          id: 1,
          name: 'sessions',
          required: false,
          type: {
            type: 'list',
            'element-id': 2,
            'element-required': false,
            element: {
              type: 'map',
              'key-id': 3,
              key: 'string',
              'value-id': 4,
              'value-required': false,
              value: 'int',
            },
          },
        },
      ],
    }
    const records = [
      { sessions: [{ a: 1 }, { b: 2, c: 3 }] },
      { sessions: [] },
    ]
    writeParquet({ writer, schema: nestedSchema, records })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual(records)
  })

  it('writes a struct column as a parquet group with stamped child field ids', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const structSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        {
          id: 2,
          name: 'point',
          required: false,
          type: {
            type: 'struct',
            'schema-id': 0,
            fields: [
              { id: 3, name: 'x', required: true, type: 'double' },
              { id: 4, name: 'y', required: true, type: 'double' },
            ],
          },
        },
      ],
    }
    const records = [
      { id: 1n, point: { x: 1.5, y: 2.5 } },
      { id: 2n, point: null },
      { id: 3n, point: { x: -1, y: 0 } },
    ]
    writeParquet({ writer, schema: structSchema, records })
    const file = writer.getBuffer()
    const meta = parquetMetadata(file)

    expect(meta.schema.find(s => s.name === 'point')).toMatchObject({
      repetition_type: 'OPTIONAL',
      num_children: 2,
      field_id: 2,
    })
    expect(meta.schema.find(s => s.name === 'x')).toMatchObject({
      type: 'DOUBLE',
      repetition_type: 'REQUIRED',
      field_id: 3,
    })
    expect(meta.schema.find(s => s.name === 'y')).toMatchObject({
      type: 'DOUBLE',
      repetition_type: 'REQUIRED',
      field_id: 4,
    })

    const rows = await parquetReadObjects({ file, compressors })
    expect(rows[0]).toEqual({ id: 1n, point: { x: 1.5, y: 2.5 } })
    expect(rows[1].point ?? null).toBeNull()
    expect(rows[2]).toEqual({ id: 3n, point: { x: -1, y: 0 } })
  })

  it('writes a struct containing list and map fields', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const nestedSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        {
          id: 1,
          name: 'profile',
          required: true,
          type: {
            type: 'struct',
            'schema-id': 0,
            fields: [
              { id: 2, name: 'name', required: true, type: 'string' },
              {
                id: 3,
                name: 'roles',
                required: false,
                type: { type: 'list', 'element-id': 4, 'element-required': true, element: 'string' },
              },
              {
                id: 5,
                name: 'meta',
                required: false,
                type: {
                  type: 'map',
                  'key-id': 6,
                  key: 'string',
                  'value-id': 7,
                  'value-required': true,
                  value: 'int',
                },
              },
            ],
          },
        },
      ],
    }
    const records = [
      { profile: { name: 'alice', roles: ['admin', 'editor'], meta: { age: 30 } } },
      { profile: { name: 'bob', roles: [], meta: {} } },
    ]
    writeParquet({ writer, schema: nestedSchema, records })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual(records)
  })

  it('writes a struct nested inside a list', async () => {
    const writer = new ByteWriter()
    /** @type {Schema} */
    const nestedSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        {
          id: 1,
          name: 'points',
          required: false,
          type: {
            type: 'list',
            'element-id': 2,
            'element-required': true,
            element: {
              type: 'struct',
              'schema-id': 0,
              fields: [
                { id: 3, name: 'x', required: true, type: 'int' },
                { id: 4, name: 'y', required: true, type: 'int' },
              ],
            },
          },
        },
      ],
    }
    const records = [
      { points: [{ x: 1, y: 2 }, { x: 3, y: 4 }] },
      { points: [] },
    ]
    writeParquet({ writer, schema: nestedSchema, records })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual(records)
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
