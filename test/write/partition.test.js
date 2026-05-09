import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { avroRead } from '../../src/avro/avro.read.js'
import { fieldTypeName, groupByPartition, partitionAvroSchema, partitionToAvroRecord } from '../../src/write/partition.js'
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

  it('uses write-defaults when grouping missing partition values', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'country', required: false, type: 'string', 'write-default': 'us' },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [{ 'source-id': 1, 'field-id': 1000, name: 'country', transform: 'identity' }],
    }

    const groups = groupByPartition([
      {},
      { country: 'us' },
      { country: null },
    ], schema, partitionSpec)

    expect(groups).toHaveLength(2)
    expect(groups[0].partition).toEqual({ country: 'us' })
    expect(groups[0].records).toHaveLength(2)
    expect(groups[1].partition).toEqual({ country: null })
  })

  it('canonicalizes long partition keys across number and bigint values', () => {
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

    const groups = groupByPartition([
      { id: 7 },
      { id: 7n },
      { id: 8n },
    ], schema, partitionSpec)

    expect(groups).toHaveLength(2)
    expect(groups[0].records).toHaveLength(2)
    expect(groups[1].partition).toEqual({ id: 8n })
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

  it('builds Avro partition fields for scalar identity transforms', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'flag', required: false, type: 'boolean' },
        { id: 2, name: 'count', required: false, type: 'long' },
        { id: 3, name: 'ratio', required: false, type: 'float' },
        { id: 4, name: 'score', required: false, type: 'double' },
        { id: 5, name: 'payload', required: false, type: 'binary' },
        { id: 6, name: 'event_date', required: false, type: 'date' },
        { id: 7, name: 'created_ns', required: false, type: 'timestamp_ns' },
        { id: 8, name: 'observed_ns', required: false, type: 'timestamptz_ns' },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: schema.fields.map((field, i) => ({
        'source-id': field.id,
        'field-id': 1000 + i,
        name: field.name,
        transform: 'identity',
      })),
    }

    expect(partitionAvroSchema(schema, partitionSpec).fields).toEqual([
      { name: 'flag', 'field-id': 1000, default: null, type: ['null', 'boolean'] },
      { name: 'count', 'field-id': 1001, default: null, type: ['null', 'long'] },
      { name: 'ratio', 'field-id': 1002, default: null, type: ['null', 'float'] },
      { name: 'score', 'field-id': 1003, default: null, type: ['null', 'double'] },
      { name: 'payload', 'field-id': 1004, default: null, type: ['null', 'bytes'] },
      { name: 'event_date', 'field-id': 1005, default: null, type: ['null', { type: 'int', logicalType: 'date' }] },
      {
        name: 'created_ns',
        'field-id': 1006,
        default: null,
        type: ['null', { type: 'long', logicalType: 'timestamp-nanos', 'adjust-to-utc': false }],
      },
      {
        name: 'observed_ns',
        'field-id': 1007,
        default: null,
        type: ['null', { type: 'long', logicalType: 'timestamp-nanos', 'adjust-to-utc': true }],
      },
    ])
  })

  it('builds Avro fixed partition fields for uuid and fixed types', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'uuid_col', required: false, type: 'uuid' },
        { id: 2, name: 'sig', required: false, type: /** @type {const} */ ('fixed[4]') },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [
        { 'source-id': 1, 'field-id': 1000, name: 'uuid_col', transform: 'identity' },
        { 'source-id': 2, 'field-id': 1001, name: 'sig', transform: 'identity' },
      ],
    }

    expect(partitionAvroSchema(schema, partitionSpec).fields).toEqual([
      {
        name: 'uuid_col',
        'field-id': 1000,
        default: null,
        type: ['null', { type: 'fixed', name: 'r102_1000', size: 16, logicalType: 'uuid' }],
      },
      {
        name: 'sig',
        'field-id': 1001,
        default: null,
        type: ['null', { type: 'fixed', name: 'r102_1001', size: 4 }],
      },
    ])
  })

  it('builds int Avro partition fields for void transforms', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'payload', required: false, type: 'variant' },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [{ 'source-id': 1, 'field-id': 1000, name: 'payload_void', transform: 'void' }],
    }

    expect(partitionAvroSchema(schema, partitionSpec).fields).toEqual([
      { name: 'payload_void', 'field-id': 1000, default: null, type: ['null', 'int'] },
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

  it('coerces uuid strings and rejects invalid fixed partition values for Avro records', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'uuid_col', required: false, type: 'uuid' },
        { id: 2, name: 'sig', required: false, type: /** @type {const} */ ('fixed[4]') },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [
        { 'source-id': 1, 'field-id': 1000, name: 'uuid_col', transform: 'identity' },
        { 'source-id': 2, 'field-id': 1001, name: 'sig', transform: 'identity' },
      ],
    }

    const record = partitionToAvroRecord({
      uuid_col: 'f79c3e09-677c-4bbd-a479-3f349cb785e7',
      sig: [1, 2, 3, 4],
    }, schema, partitionSpec)

    expect(record).toEqual({
      uuid_col: new Uint8Array([
        0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd,
        0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7,
      ]),
      sig: new Uint8Array([1, 2, 3, 4]),
    })
    expect(() => partitionToAvroRecord({
      uuid_col: 'f79c3e09-677c-4bbd-a479-3f349cb785e7',
      sig: new Uint8Array([1, 2, 3]),
    }, schema, partitionSpec)).toThrow(/expected fixed\[4\] partition value/)
    expect(partitionToAvroRecord({
      uuid_col: new Uint8Array([
        0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd,
        0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7,
      ]),
      sig: new Uint8Array([1, 2, 3, 4]),
    }, schema, partitionSpec).uuid_col).toEqual(new Uint8Array([
      0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd,
      0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7,
    ]))
    expect(() => partitionToAvroRecord({
      uuid_col: new Uint8Array([1, 2, 3]),
      sig: new Uint8Array([1, 2, 3, 4]),
    }, schema, partitionSpec)).toThrow(/expected uuid partition value/)
    expect(() => partitionToAvroRecord({
      uuid_col: 12,
      sig: new Uint8Array([1, 2, 3, 4]),
    }, schema, partitionSpec)).toThrow(/expected uuid partition value/)
    expect(() => partitionToAvroRecord({
      uuid_col: 'not-a-uuid',
      sig: new Uint8Array([1, 2, 3, 4]),
    }, schema, partitionSpec)).toThrow(/expected uuid partition value/)
  })

  it('throws when converting partition values with a missing source field', () => {
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
      fields: [{ 'source-id': 99, 'field-id': 1000, name: 'missing', transform: 'identity' }],
    }

    expect(() => partitionToAvroRecord({ missing: 1 }, schema, partitionSpec))
      .toThrow(/partition source field id 99 not found/)
  })

  it('validates partition source fields when grouping and building Avro schema', () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
      ],
    }

    expect(() => groupByPartition([], schema, {
      'spec-id': 0,
      fields: [{ 'field-id': 1000, name: 'missing_source', transform: 'identity' }],
    })).toThrow(/partition field missing_source is missing source-id/)
    expect(() => groupByPartition([], schema, {
      'spec-id': 0,
      fields: [{ 'source-id': 99, 'field-id': 1000, name: 'missing', transform: 'identity' }],
    })).toThrow(/partition source field id 99 not found in schema/)
    expect(() => partitionAvroSchema(schema, {
      'spec-id': 0,
      fields: [{ 'source-id': 99, 'field-id': 1000, name: 'missing', transform: 'identity' }],
    })).toThrow(/partition source field id 99 not found/)
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

  it('round-trips identity uuid partition values through a manifest', async () => {
    /** @type {Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' },
        { id: 2, name: 'uuid_col', required: false, type: 'uuid' },
      ],
    }
    /** @type {PartitionSpec} */
    const partitionSpec = {
      'spec-id': 0,
      fields: [{ 'source-id': 2, 'field-id': 1000, name: 'uuid_col', transform: 'identity' }],
    }
    /** @type {DataFile} */
    const dataFile = {
      content: 0,
      file_path: 's3://bucket/table/data/abc.parquet',
      file_format: 'parquet',
      partition: { uuid_col: 'f79c3e09-677c-4bbd-a479-3f349cb785e7' },
      record_count: 1n,
      file_size_in_bytes: 128n,
    }

    const writer = new ByteWriter()
    writeDataManifest({ writer, schema, partitionSpec, snapshotId: 12345n, dataFiles: [dataFile] })
    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    const records = await avroRead({ reader, metadata, syncMarker })

    expect(records[0].data_file.partition.uuid_col).toEqual(new Uint8Array([
      0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd,
      0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7,
    ]))
  })

  it('returns field type names for string and object field types', () => {
    expect(fieldTypeName({ id: 1, name: 'id', required: true, type: 'long' })).toBe('long')
    expect(fieldTypeName({
      id: 2,
      name: 'payload',
      required: false,
      type: { type: 'struct', 'schema-id': 1, fields: [] },
    })).toBe('struct')
  })
})
