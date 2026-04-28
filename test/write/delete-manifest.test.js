import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { writeDataManifest, writeDeleteManifest } from '../../src/write/manifest.js'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { avroRead } from '../../src/avro/avro.read.js'

/**
 * @import {DataFile, PartitionSpec, Schema} from '../../src/types.js'
 */

describe('writeDeleteManifest', () => {
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
  const positionDeleteFile = {
    content: 1,
    file_path: 's3://bucket/table/data/dels-001.parquet',
    file_format: 'parquet',
    partition: {},
    record_count: 4n,
    file_size_in_bytes: 512n,
    referenced_data_file: 's3://bucket/table/data/abc.parquet',
    value_counts: { 2147483546: 4n, 2147483545: 4n },
    null_value_counts: { 2147483546: 0n, 2147483545: 0n },
    sort_order_id: 0,
  }

  it('round-trips a v2 position-delete manifest', async () => {
    const writer = new ByteWriter()
    writeDeleteManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 999n,
      deleteFiles: [positionDeleteFile],
    })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['format-version']).toBe('2')
    expect(metadata.content).toBe('deletes')
    expect(metadata['partition-spec']).toBe('[]')

    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records).toHaveLength(1)
    expect(records[0]).toMatchObject({
      status: 1,
      snapshot_id: 999n,
      data_file: {
        content: 1,
        file_path: 's3://bucket/table/data/dels-001.parquet',
        file_format: 'PARQUET',
        partition: {},
        record_count: 4n,
        file_size_in_bytes: 512n,
        referenced_data_file: 's3://bucket/table/data/abc.parquet',
      },
    })
    // Position deletes carry no equality_ids — the union reads as undefined.
    expect(records[0].data_file.equality_ids).toBeUndefined()
  })

  it('omits content_offset / content_size_in_bytes from v2 schema', async () => {
    const writer = new ByteWriter()
    writeDeleteManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 1n,
      deleteFiles: [positionDeleteFile],
    })
    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].data_file.content_offset).toBeUndefined()
    expect(records[0].data_file.content_size_in_bytes).toBeUndefined()
  })

  it('writes v3 DV-style fields when provided', async () => {
    const writer = new ByteWriter()
    /** @type {DataFile} */
    const dvFile = {
      ...positionDeleteFile,
      file_format: 'puffin',
      file_path: 's3://bucket/table/data/abc.puffin',
      content_offset: 4n,
      content_size_in_bytes: 64n,
    }
    writeDeleteManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 2n,
      deleteFiles: [dvFile],
      formatVersion: 3,
    })
    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['format-version']).toBe('3')
    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].data_file.content_offset).toBe(4n)
    expect(records[0].data_file.content_size_in_bytes).toBe(64n)
    expect(records[0].data_file.first_row_id).toBeUndefined()
  })

  it('writes equality_ids for equality delete files', async () => {
    const writer = new ByteWriter()
    /** @type {DataFile} */
    const eqFile = {
      ...positionDeleteFile,
      content: 2,
      equality_ids: [1, 2],
      referenced_data_file: undefined,
    }
    writeDeleteManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 3n,
      deleteFiles: [eqFile],
    })
    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].data_file.content).toBe(2)
    expect(records[0].data_file.equality_ids).toEqual([1, 2])
    expect(records[0].data_file.referenced_data_file).toBeUndefined()
  })

  it('rejects data files in a delete manifest', () => {
    const writer = new ByteWriter()
    expect(() => writeDeleteManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 4n,
      deleteFiles: [{ ...positionDeleteFile, content: 0 }],
    })).toThrow('writeDeleteManifest expects delete files')
  })

  it('rejects equality deletes without equality_ids', () => {
    const writer = new ByteWriter()
    expect(() => writeDeleteManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 5n,
      deleteFiles: [{ ...positionDeleteFile, content: 2 }],
    })).toThrow('equality delete file missing equality_ids')
  })

  it('writeDataManifest still rejects delete files', () => {
    const writer = new ByteWriter()
    expect(() => writeDataManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 6n,
      dataFiles: [positionDeleteFile],
    })).toThrow('writeDataManifest expects data files')
  })
})
