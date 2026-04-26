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
})
