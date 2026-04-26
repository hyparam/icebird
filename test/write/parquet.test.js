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
      fields: [{ id: 1, name: 'x', required: false, type: 'decimal(9,2)' }],
    }
    expect(() => writeParquet({ writer, schema: bad, records: [] }))
      .toThrow('unsupported iceberg type: decimal(9,2)')
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
})
