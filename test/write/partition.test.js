import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { avroRead } from '../../src/avro/avro.read.js'
import { groupByPartition, partitionAvroSchema, partitionToAvroRecord } from '../../src/write/partition.js'
import { writeDataManifest } from '../../src/write/manifest.js'

/**
 * @import {DataFile, PartitionSpec, Schema} from '../../src/types.js'
 */

describe('write partition helpers', () => {
  it('uses IEEE floating-point equality for partition tuple keys', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'value', required: false, type: 'double' },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [{ 'source-id': 1, 'field-id': 1000, name: 'value', transform: 'identity' }],
    }

    const groups = groupByPartition([
      { value: 0 },
      { value: -0 },
      { value: 0 },
      { value: NaN },
      { value: Number.NaN },
    ], schema, partitionSpec)

    expect(groups).toHaveLength(3)
    expect(groups[0].records).toHaveLength(2)
    expect(Object.is(groups[1].partition.value, -0)).toBe(true)
    expect(groups[1].records).toHaveLength(1)
    expect(Number.isNaN(groups[2].partition.value)).toBe(true)
    expect(groups[2].records).toHaveLength(2)
  })

  it('builds Avro partition fields for temporal identity transforms', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'event_time', required: false, type: 'time' },
        { id: 2, name: 'created_at', required: false, type: 'timestamp' },
        { id: 3, name: 'observed_at', required: false, type: 'timestamptz' },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [
        { 'source-id': 1, 'field-id': 1000, name: 'event_time', transform: 'identity' },
        { 'source-id': 2, 'field-id': 1001, name: 'created_at', transform: 'identity' },
        { 'source-id': 3, 'field-id': 1002, name: 'observed_at', transform: 'identity' },
      ],
    }

    expect(partitionAvroSchema(schema, partitionSpec).fields).toEqual([
      {
        name: 'event_time',
        'field-id': 1000,
        default: null,
        type: ['null', { type: 'long', logicalType: 'time-micros' }],
      },
      {
        name: 'created_at',
        'field-id': 1001,
        default: null,
        type: ['null', { type: 'long', logicalType: 'timestamp-micros', 'adjust-to-utc': false }],
      },
      {
        name: 'observed_at',
        'field-id': 1002,
        default: null,
        type: ['null', { type: 'long', logicalType: 'timestamp-micros', 'adjust-to-utc': true }],
      },
    ])
  })

  it('coerces long partition values for Avro records', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [{ 'source-id': 1, 'field-id': 1000, name: 'id', transform: 'identity' }],
    }

    expect(partitionToAvroRecord({ id: 7 }, schema, partitionSpec)).toEqual({ id: 7n })
  })

  it('round-trips identity time partition values through a manifest', async () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'event_time', required: false, type: 'time' },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [{ 'source-id': 2, 'field-id': 1000, name: 'event_time', transform: 'identity' }],
    }
    /** @type {DataFile} */
    const dataFile = {
      content: 0,
      file_path: 's3://bucket/table/data/abc.parquet',
      file_format: 'parquet',
      partition: { event_time: 45_000_000n },
      record_count: 1n,
      file_size_in_bytes: 128n,
    }

    const writer = new ByteWriter()
    writeDataManifest({ writer, schema, partitionSpec, snapshotId: 12345n, dataFiles: [dataFile] })
    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    const records = await avroRead({ reader, metadata, syncMarker })

    expect(records[0].data_file.partition.event_time).toBe(45_000_000n)
  })
})
