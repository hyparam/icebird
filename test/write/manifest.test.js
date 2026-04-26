import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { writeDataManifest } from '../../src/write/manifest.js'
import { avroMetadata } from '../../src/avro/avro.metadata.js'
import { avroRead } from '../../src/avro/avro.read.js'

/**
 * @import {DataFile, Schema} from '../../src/types.js'
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

  it('writes a manifest that round-trips through the avro reader', async () => {
    const writer = new ByteWriter()
    writeDataManifest({ writer, schema, snapshotId: 12345n, dataFile })
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
  })
})
