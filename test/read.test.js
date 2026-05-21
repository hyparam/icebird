import { describe, expect, it } from 'vitest'
import { ByteWriter, parquetWrite } from 'hyparquet-writer'
import { icebergRead, readDataFile } from '../src/read.js'

describe.concurrent('icebergRead', () => {
  it('throws for missing tableUrl', async () => {
    await expect(() => icebergRead({ tableUrl: '' }))
      .rejects.toThrow('tableUrl is required')
  })

  it('throws for fetch errors', async () => {
    // not found
    await expect(() => icebergRead({ tableUrl: 'https://hyperparam.app' }))
      .rejects.toThrow('failed to determine latest iceberg version')

    // invalid dns
    await expect(() => icebergRead({ tableUrl: 'https://nope.hyperparam.app' }))
      .rejects.toThrow('failed to determine latest iceberg version')

    // with metadataFileName
    await expect(() => icebergRead({
      tableUrl: 'https://hyperparam.app',
      metadataFileName: 'invalid.metadata.json',
    })).rejects.toThrow('failed to get iceberg metadata')
  })

  it('throws for invalid row range', async () => {
    await expect(() => icebergRead({ tableUrl: 'https://example.com', rowStart: 5, rowEnd: 3 }))
      .rejects.toThrow('rowStart must be less than rowEnd')

    await expect(() => icebergRead({ tableUrl: 'https://example.com', rowStart: -1 }))
      .rejects.toThrow('rowStart must be positive')
  })

  it('can read row groups concurrently while preserving row order', async () => {
    /** @type {import('../src/types.js').Schema} */
    const schema = {
      type: 'struct',
      'schema-id': 0,
      fields: [{ id: 1, name: 'payload', required: false, type: 'string' }],
    }
    const values = ['a', 'b', 'c'].map(ch => ch.repeat(300_000))
    const writer = new ByteWriter()
    await parquetWrite({
      writer,
      columnData: [{ name: 'payload', data: values }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'payload', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL', field_id: 1 },
      ],
      kvMetadata: [{ key: 'iceberg.schema', value: JSON.stringify(schema) }],
      codec: 'UNCOMPRESSED',
      rowGroupSize: 1,
    })
    const bytes = writer.getBytes()
    expect(bytes.byteLength).toBeGreaterThan(1 << 19)

    let sliceCalls = 0
    let blockedStarts = 0
    let observedParallel = false
    /** @type {(() => void)[]} */
    const releases = []
    /**
     * @param {number} start
     * @param {number} [end]
     * @returns {ArrayBuffer}
     */
    function copySlice(start, end = bytes.byteLength) {
      const slice = bytes.subarray(start, end)
      return slice.buffer.slice(slice.byteOffset, slice.byteOffset + slice.byteLength)
    }
    function releasePending() {
      for (const release of releases.splice(0)) release()
    }
    /** @type {import('hyparquet').AsyncBuffer} */
    const asyncBuffer = {
      byteLength: bytes.byteLength,
      slice(start, end) {
        sliceCalls++
        if (sliceCalls === 1) return copySlice(start, end)
        blockedStarts++
        return new Promise(resolve => {
          releases.push(() => resolve(copySlice(start, end)))
          if (blockedStarts >= 2) {
            observedParallel = true
            releasePending()
          } else {
            queueMicrotask(releasePending)
          }
        })
      },
    }
    /** @type {import('../src/types.js').Resolver} */
    const resolver = {
      reader(path, byteLength) {
        expect(path).toBe('mem://data.parquet')
        expect(byteLength).toBe(bytes.byteLength)
        return asyncBuffer
      },
    }

    /** @type {Record<string, any>[]} */
    const rows = []
    /** @type {import('../src/types.js').ManifestEntry} */
    const dataEntry = {
      status: 1,
      sequence_number: 0n,
      partition_spec_id: 0,
      data_file: {
        content: 0,
        file_path: 'mem://data.parquet',
        file_format: 'parquet',
        partition: {},
        record_count: BigInt(values.length),
        file_size_in_bytes: BigInt(bytes.byteLength),
      },
    }
    /** @type {import('../src/types.js').TableMetadata} */
    const metadata = {
      'format-version': 2,
      'table-uuid': 'test',
      location: 'mem://table',
      'last-sequence-number': 0,
      'last-updated-ms': 0,
      'last-column-id': 1,
      'current-schema-id': 0,
      schemas: [schema],
      'default-spec-id': 0,
      'partition-specs': [{ 'spec-id': 0, fields: [] }],
      'last-partition-id': 0,
      'sort-orders': [{ 'order-id': 0, fields: [] }],
      'default-sort-order-id': 0,
    }
    for await (const batch of readDataFile({
      dataEntry,
      fileRowStart: 0,
      fileRowEnd: values.length,
      schema,
      metadata,
      resolver,
      rowLineage: false,
      positionDeletesMap: new Map(),
      equalityDeleteGroups: [],
      rowGroupConcurrency: 2,
    })) {
      rows.push(...batch)
    }

    expect(observedParallel).toBe(true)
    expect(rows.map(row => row.payload[0])).toEqual(['a', 'b', 'c'])
    expect(rows.map(row => row.payload.length)).toEqual(values.map(value => value.length))
  })
})
