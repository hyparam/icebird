import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { writeDataManifest } from '../../src/write/manifest.js'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { avroRead } from '../../src/avro/avro.read.js'

/**
 * @import {DataFile, PartitionSpec, Schema} from '../../src/types.js'
 */

describe('writeDataManifest', () => {
  /** @type {Schema} */
  const schema = {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: false, type: 'string' },
    ],
  }

  /** @type {PartitionSpec} */
  const unpartitioned = { 'spec-id': 0, fields: [] }

  /** @type {DataFile} */
  const dataFile = {
    content: 0,
    file_path: 's3://bucket/table/data/abc.parquet',
    file_format: 'parquet',
    partition: {},
    record_count: 3n,
    file_size_in_bytes: 421n,
    value_counts: { 1: 3n, 2: 3n },
    null_value_counts: { 1: 0n, 2: 1n },
    lower_bounds: { 1: new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0]) },
    upper_bounds: { 1: new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0]) },
    sort_order_id: 0,
  }

  it('writes a manifest that round-trips through the avro reader', async () => {
    const writer = new ByteWriter()
    writeDataManifest({ writer, schema, partitionSpec: unpartitioned, snapshotId: 12345n, dataFiles: [dataFile] })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['format-version']).toBe('2')
    expect(metadata.content).toBe('data')
    expect(metadata['partition-spec']).toBe('[]')
    expect(metadata['partition-spec-id']).toBe('0')
    expect(metadata.schema).toEqual(schema)

    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records).toHaveLength(1)
    expect(records[0]).toMatchObject({
      status: 1,
      snapshot_id: 12345n,
      data_file: {
        content: 0,
        file_path: 's3://bucket/table/data/abc.parquet',
        file_format: 'PARQUET',
        partition: {},
        record_count: 3n,
        file_size_in_bytes: 421n,
        sort_order_id: 0,
      },
    })

    // Stat maps round-trip as Avro array-of-{key,value} records
    const df = records[0].data_file
    expect(df.value_counts).toEqual([
      { key: 1, value: 3n },
      { key: 2, value: 3n },
    ])
    expect(df.null_value_counts).toEqual([
      { key: 1, value: 0n },
      { key: 2, value: 1n },
    ])
    expect(df.lower_bounds).toEqual([
      { key: 1, value: new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0]) },
    ])
    expect(df.upper_bounds).toEqual([
      { key: 1, value: new Uint8Array([5, 0, 0, 0, 0, 0, 0, 0]) },
    ])
    expect(df.nan_value_counts).toBeUndefined()
    expect(df.column_sizes).toBeUndefined()
  })

  it('writes v3 first_row_id as null for new data files', async () => {
    const writer = new ByteWriter()
    writeDataManifest({ writer, schema, partitionSpec: unpartitioned, snapshotId: 12345n, dataFiles: [dataFile], formatVersion: 3 })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['format-version']).toBe('3')

    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].data_file.first_row_id).toBeUndefined()
  })

  it('writes v3 first_row_id when data file metadata already has one', async () => {
    const writer = new ByteWriter()
    writeDataManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 12345n,
      dataFiles: [{ ...dataFile, first_row_id: 1000n }],
      formatVersion: 3,
    })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['format-version']).toBe('3')

    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].data_file.first_row_id).toBe(1000n)
  })
})
