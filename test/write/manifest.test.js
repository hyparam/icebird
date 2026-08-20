import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { writeDataManifest, writeExistingDataManifest } from '../../src/write/manifest.js'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { avroRead } from '../../src/avro/avro.read.js'

/**
 * @import {DataFile, ManifestEntry, PartitionSpec, Schema} from '../../src/types.js'
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
    expect(metadata['schema-id']).toBe('0')
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
    expect(metadata['schema-id']).toBe('0')

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

describe('writeExistingDataManifest', () => {
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
    sort_order_id: 0,
  }

  /** @type {ManifestEntry} */
  const entry = {
    status: 1,
    snapshot_id: 111n,
    sequence_number: 7n,
    file_sequence_number: 7n,
    partition_spec_id: 0,
    data_file: dataFile,
  }

  it('writes EXISTING entries that keep their original snapshot and sequence numbers', async () => {
    const writer = new ByteWriter()
    writeExistingDataManifest({ writer, schema, partitionSpec: unpartitioned, entries: [entry] })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata.content).toBe('data')
    expect(metadata['schema-id']).toBe('0')

    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records).toHaveLength(1)
    // status 0 (EXISTING) with explicit numbers: an ADDED entry would leave
    // these null and inherit the rewriting snapshot's sequence number, which
    // would move the file forward in time and change which deletes apply.
    expect(records[0]).toMatchObject({
      status: 0,
      snapshot_id: 111n,
      sequence_number: 7n,
      file_sequence_number: 7n,
      data_file: { file_path: 's3://bucket/table/data/abc.parquet' },
    })
  })

  it('carries v3 first_row_id through', async () => {
    const writer = new ByteWriter()
    writeExistingDataManifest({
      writer,
      schema,
      partitionSpec: unpartitioned,
      entries: [{ ...entry, data_file: { ...dataFile, first_row_id: 1000n } }],
      formatVersion: 3,
    })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['schema-id']).toBe('0')
    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].data_file.first_row_id).toBe(1000n)
  })

  it('round-trips stat maps from a read-decoded manifest', async () => {
    // The Avro reader hands Iceberg maps back as {key, value} record arrays
    // rather than plain objects, and a manifest rewrite feeds exactly those
    // decoded entries back in. Sequence numbers are supplied here the way
    // `icebergManifests` materializes them from the manifest list.
    const first = new ByteWriter()
    writeDataManifest({
      writer: first,
      schema,
      partitionSpec: unpartitioned,
      snapshotId: 111n,
      dataFiles: [{
        ...dataFile,
        value_counts: { 1: 3n, 2: 3n },
        lower_bounds: { 1: new Uint8Array([1, 0, 0, 0, 0, 0, 0, 0]) },
        split_offsets: [4n, 100n],
      }],
    })
    const firstBuffer = first.getBuffer()

    const firstReader = { view: new DataView(firstBuffer), offset: 0 }
    const firstMeta = await avroMetadata(firstReader)
    const decoded = (await avroRead({
      reader: firstReader,
      metadata: firstMeta.metadata,
      syncMarker: firstMeta.syncMarker,
    }))[0]

    const second = new ByteWriter()
    writeExistingDataManifest({
      writer: second,
      schema,
      partitionSpec: unpartitioned,
      entries: [/** @type {ManifestEntry} */ ({
        ...decoded,
        sequence_number: 7n,
        file_sequence_number: 7n,
        partition_spec_id: 0,
      })],
    })
    const secondBuffer = second.getBuffer()

    const secondReader = { view: new DataView(secondBuffer), offset: 0 }
    const secondMeta = await avroMetadata(secondReader)
    const records = await avroRead({
      reader: secondReader,
      metadata: secondMeta.metadata,
      syncMarker: secondMeta.syncMarker,
    })
    expect(records[0].data_file.value_counts).toEqual(decoded.data_file.value_counts)
    expect(records[0].data_file.lower_bounds).toEqual(decoded.data_file.lower_bounds)
    expect(records[0].data_file.split_offsets).toEqual(decoded.data_file.split_offsets)
    expect(records[0].data_file.record_count).toBe(3n)
  })

  it('rejects deleted entries', () => {
    expect(() => writeExistingDataManifest({
      writer: new ByteWriter(),
      schema,
      partitionSpec: unpartitioned,
      entries: [{ ...entry, status: 2 }],
    })).toThrow('writeExistingDataManifest cannot rewrite deleted entries as existing')
  })

  it('rejects delete files', () => {
    expect(() => writeExistingDataManifest({
      writer: new ByteWriter(),
      schema,
      partitionSpec: unpartitioned,
      entries: [{ ...entry, data_file: { ...dataFile, content: 1 } }],
    })).toThrow('writeExistingDataManifest expects data files')
  })

  it('rejects entries without sequence numbers', () => {
    expect(() => writeExistingDataManifest({
      writer: new ByteWriter(),
      schema,
      partitionSpec: unpartitioned,
      entries: [{ ...entry, file_sequence_number: undefined }],
    })).toThrow('existing data manifest entry missing sequence numbers')
  })

  it('rejects entries without a materialized snapshot id', () => {
    expect(() => writeExistingDataManifest({
      writer: new ByteWriter(),
      schema,
      partitionSpec: unpartitioned,
      entries: [{ ...entry, snapshot_id: undefined }],
    })).toThrow('existing data manifest entry missing snapshot id')
  })

  it('rejects entries from another partition spec', () => {
    expect(() => writeExistingDataManifest({
      writer: new ByteWriter(),
      schema,
      partitionSpec: unpartitioned,
      entries: [{ ...entry, partition_spec_id: 1 }],
    })).toThrow('existing data entry partition spec 1 does not match 0')
  })
})
