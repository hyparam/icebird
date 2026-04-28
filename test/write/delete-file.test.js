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
    await writePositionDeleteFile({ writer, deletes })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual([
      { file_path: 'a.parquet', pos: 1n },
      { file_path: 'a.parquet', pos: 7n },
      { file_path: 'b.parquet', pos: 0n },
      { file_path: 'b.parquet', pos: 5n },
    ])
  })

  it('does not mutate the input array', async () => {
    const writer = new ByteWriter()
    const input = deletes.slice()
    const before = input.slice()
    await writePositionDeleteFile({ writer, deletes: input })
    expect(input).toEqual(before)
  })

  it('embeds iceberg.schema with reserved field ids', async () => {
    const writer = new ByteWriter()
    await writePositionDeleteFile({ writer, deletes })
    const meta = parquetMetadata(writer.getBuffer())
    const kv = meta.key_value_metadata?.find(k => k.key === 'iceberg.schema')
    expect(kv).toBeDefined()
    const parsed = JSON.parse(kv?.value ?? '')
    expect(parsed.fields).toEqual([
      { id: 2147483546, name: 'file_path', required: true, type: 'string' },
      { id: 2147483545, name: 'pos', required: true, type: 'long' },
    ])
  })

  it('returns DataFile-shaped stats keyed by reserved field id', async () => {
    const writer = new ByteWriter()
    const stats = await writePositionDeleteFile({ writer, deletes })
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
    await writePositionDeleteFile({
      writer,
      deletes: [{ file_path: 'x.parquet', pos: 3 }],
    })
    const rows = await parquetReadObjects({ file: writer.getBuffer(), compressors })
    expect(rows).toEqual([{ file_path: 'x.parquet', pos: 3n }])
  })

  it('rejects empty input', async () => {
    const writer = new ByteWriter()
    await expect(writePositionDeleteFile({ writer, deletes: [] }))
      .rejects.toThrow('at least one delete')
  })

  it('rejects negative pos', async () => {
    const writer = new ByteWriter()
    await expect(writePositionDeleteFile({
      writer,
      deletes: [{ file_path: 'x.parquet', pos: -1n }],
    })).rejects.toThrow('non-negative')
  })

  it('rejects missing file_path', async () => {
    const writer = new ByteWriter()
    await expect(writePositionDeleteFile({
      writer,
      deletes: [/** @type {any} */ ({ pos: 0n })],
    })).rejects.toThrow('file_path')
  })
})
