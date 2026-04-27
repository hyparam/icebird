import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { writeManifestList } from '../../src/write/manifest-list.js'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { avroRead } from '../../src/avro/avro.read.js'

/**
 * @import {Manifest} from '../../src/types.js'
 */

describe('writeManifestList', () => {
  it('round-trips a single manifest entry', async () => {
    /** @type {Manifest} */
    const manifest = {
      manifest_path: 's3://bucket/table/metadata/m0.avro',
      manifest_length: 1234n,
      partition_spec_id: 0,
      content: 0,
      added_snapshot_id: 999n,
      added_files_count: 1,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 3n,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
    }
    const writer = new ByteWriter()
    writeManifestList({
      writer,
      snapshotId: 999n,
      sequenceNumber: 1n,
      manifests: [manifest],
    })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['format-version']).toBe('2')
    expect(metadata['snapshot-id']).toBe('999')
    expect(metadata['sequence-number']).toBe('1')

    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records).toHaveLength(1)
    expect(records[0]).toMatchObject({
      manifest_path: 's3://bucket/table/metadata/m0.avro',
      manifest_length: 1234n,
      partition_spec_id: 0,
      content: 0,
      sequence_number: 1n,
      min_sequence_number: 1n,
      added_snapshot_id: 999n,
      added_files_count: 1,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 3n,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
    })
  })

  it('writes v3 first_row_id metadata', async () => {
    /** @type {Manifest} */
    const manifest = {
      manifest_path: 's3://bucket/table/metadata/m0.avro',
      manifest_length: 1234n,
      partition_spec_id: 0,
      content: 0,
      added_snapshot_id: 999n,
      added_files_count: 1,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 3n,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
      first_row_id: 100n,
    }
    const writer = new ByteWriter()
    writeManifestList({
      writer,
      snapshotId: 999n,
      sequenceNumber: 1n,
      manifests: [manifest],
      formatVersion: 3,
    })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['format-version']).toBe('3')

    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0]).toMatchObject({
      first_row_id: 100n,
    })
  })

  it('round-trips partitions FieldSummary entries', async () => {
    /** @type {Manifest} */
    const manifest = {
      manifest_path: 's3://bucket/table/metadata/m0.avro',
      manifest_length: 1234n,
      partition_spec_id: 0,
      content: 0,
      added_snapshot_id: 999n,
      added_files_count: 1,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 3n,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
      partitions: [
        {
          contains_null: false,
          lower_bound: new Uint8Array([1, 0, 0, 0]),
          upper_bound: new Uint8Array([5, 0, 0, 0]),
        },
        {
          contains_null: true,
          contains_nan: false,
          lower_bound: new Uint8Array([0, 0, 0x80, 0x3f]), // 1.0 LE float
          upper_bound: new Uint8Array([0, 0, 0x40, 0x40]), // 3.0 LE float
        },
      ],
    }
    const writer = new ByteWriter()
    writeManifestList({
      writer,
      snapshotId: 999n,
      sequenceNumber: 1n,
      manifests: [manifest],
    })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].partitions).toEqual([
      {
        contains_null: false,
        contains_nan: undefined,
        lower_bound: new Uint8Array([1, 0, 0, 0]),
        upper_bound: new Uint8Array([5, 0, 0, 0]),
      },
      {
        contains_null: true,
        contains_nan: false,
        lower_bound: new Uint8Array([0, 0, 0x80, 0x3f]),
        upper_bound: new Uint8Array([0, 0, 0x40, 0x40]),
      },
    ])
  })

  it('emits null partitions when manifest has none', async () => {
    /** @type {Manifest} */
    const manifest = {
      manifest_path: 's3://bucket/table/metadata/m0.avro',
      manifest_length: 1234n,
      partition_spec_id: 0,
      content: 0,
      added_snapshot_id: 999n,
      added_files_count: 1,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 3n,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
    }
    const writer = new ByteWriter()
    writeManifestList({
      writer,
      snapshotId: 999n,
      sequenceNumber: 1n,
      manifests: [manifest],
    })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].partitions).toBeUndefined()
  })

  it('writes null first_row_id for v3 delete manifests', async () => {
    /** @type {Manifest} */
    const manifest = {
      manifest_path: 's3://bucket/table/metadata/delete-m0.avro',
      manifest_length: 1234n,
      partition_spec_id: 0,
      content: 1,
      added_snapshot_id: 999n,
      added_files_count: 1,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 3n,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
      first_row_id: 100n,
    }
    const writer = new ByteWriter()
    writeManifestList({
      writer,
      snapshotId: 999n,
      sequenceNumber: 1n,
      manifests: [manifest],
      formatVersion: 3,
    })
    const buffer = writer.getBuffer()

    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    expect(metadata['format-version']).toBe('3')

    const records = await avroRead({ reader, metadata, syncMarker })
    expect(records[0].content).toBe(1)
    expect(records[0].first_row_id).toBeUndefined()
  })
})
