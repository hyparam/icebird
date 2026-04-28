import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { writePositionDeleteFile } from '../../src/write/delete-file.js'

describe('writePositionDeleteFile', () => {
  const deletes = [
    { file_path: 'b.parquet', pos: 5n },
    { file_path: 'a.parquet', pos: 7n },
    { file_path: 'a.parquet', pos: 1n },
    { file_path: 'b.parquet', pos: 0n },
  ]

  it('round-trips deletes sorted by (file_path, pos)', async () => {
    const writer = new ByteWriter()
    writePositionDeleteFile({ writer, deletes })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual([
      { file_path: 'a.parquet', pos: 1n },
      { file_path: 'a.parquet', pos: 7n },
      { file_path: 'b.parquet', pos: 0n },
      { file_path: 'b.parquet', pos: 5n },
    ])
  })

  it('does not mutate the input array', () => {
    const writer = new ByteWriter()
    const input = deletes.slice()
    const before = input.slice()
    writePositionDeleteFile({ writer, deletes: input })
    expect(input).toEqual(before)
  })

  it('embeds iceberg.schema with reserved field ids', () => {
    const writer = new ByteWriter()
    writePositionDeleteFile({ writer, deletes })
    const meta = parquetMetadata(writer.getBuffer())
    const kv = meta.key_value_metadata?.find(k => k.key === 'iceberg.schema')
    expect(kv).toBeDefined()
    const parsed = JSON.parse(kv?.value ?? '')
    expect(parsed.fields).toEqual([
      { id: 2147483546, name: 'file_path', required: true, type: 'string' },
      { id: 2147483545, name: 'pos', required: true, type: 'long' },
    ])
  })

  it('returns DataFile-shaped stats keyed by reserved field id', () => {
    const writer = new ByteWriter()
    const stats = writePositionDeleteFile({ writer, deletes })
    expect(stats.record_count).toBe(4n)
    expect(stats.value_counts).toEqual({ 2147483546: 4n, 2147483545: 4n })
    expect(stats.null_value_counts).toEqual({ 2147483546: 0n, 2147483545: 0n })
    expect(stats.lower_bounds[2147483546]).toEqual(new TextEncoder().encode('a.parquet'))
    expect(stats.upper_bounds[2147483546]).toEqual(new TextEncoder().encode('b.parquet'))
    // pos is INT64 little-endian
    const posLow = new Uint8Array(8)
    const posHigh = new Uint8Array(8)
    posHigh[0] = 7
    expect(stats.lower_bounds[2147483545]).toEqual(posLow)
    expect(stats.upper_bounds[2147483545]).toEqual(posHigh)
  })

  it('accepts numeric pos and coerces to bigint', async () => {
    const writer = new ByteWriter()
    writePositionDeleteFile({
      writer,
      deletes: [{ file_path: 'x.parquet', pos: 3 }],
    })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual([{ file_path: 'x.parquet', pos: 3n }])
  })

  it('rejects empty input', () => {
    const writer = new ByteWriter()
    expect(() => writePositionDeleteFile({ writer, deletes: [] }))
      .toThrow('at least one delete')
  })

  it('rejects negative pos', () => {
    const writer = new ByteWriter()
    expect(() => writePositionDeleteFile({
      writer,
      deletes: [{ file_path: 'x.parquet', pos: -1n }],
    })).toThrow('non-negative')
  })

  it('rejects missing file_path', () => {
    const writer = new ByteWriter()
    expect(() => writePositionDeleteFile({
      writer,
      deletes: [/** @type {any} */ ({ pos: 0n })],
    })).toThrow('file_path')
  })
})
