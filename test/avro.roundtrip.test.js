import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { avroMetadata, avroRead, avroWrite } from '../src/index.js'

/**
 * @import {AvroType} from '../src/types.js'
 */

describe('Avro round-trip', () => {
  it('primitive records', () => {
    /** @type {AvroType} */
    const schema = {
      type: 'record',
      name: 'User',
      fields: [
        { name: 'id', type: 'long' },
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' },
        { name: 'alive', type: 'boolean' },
        { name: 'weight', type: 'float' },
        { name: 'height', type: 'double' },
      ],
    }

    const records = [
      { id: 1n, name: 'alice', age: 30, alive: true, weight: 65.5, height: 1.75 },
      { id: 2n, name: 'bob', age: 25, alive: false, weight: 70.0, height: 1.80 },
    ]

    const writer = new ByteWriter()
    avroWrite({ writer, schema, records })

    const reader = { view: writer.view, offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const rows = avroRead({ reader, metadata, syncMarker })

    expect(rows).toEqual(records)
  })

  it('nullable round-trip', () => {
    /** @type {AvroType} */
    const schema = {
      type: 'record',
      name: 'Found',
      fields: [
        { name: 'nullable', type: ['null', 'string'] },
      ],
    }

    const recs = [
      { nullable: 'meaning' },
      { nullable: undefined },
    ]

    const writer = new ByteWriter()
    avroWrite({ writer, schema, records: recs })

    const reader = { view: writer.view, offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const got = avroRead({ reader, metadata, syncMarker })

    expect(got).toEqual(recs)
  })

  it('logical timestamp-millis', () => {
    /** @type {AvroType} */
    const schema = {
      type: 'record',
      name: 'Event',
      fields: [
        {
          name: 'ts',
          type: { type: 'long', logicalType: 'timestamp-millis' },
        },
      ],
    }

    const now = Date.now()
    const original = [{ ts: new Date(now) }]

    const writer = new ByteWriter()
    avroWrite({ writer, schema, records: original })

    const reader = { view: writer.view, offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const round = avroRead({ reader, metadata, syncMarker })

    expect(round[0].ts.getTime()).toBe(original[0].ts.getTime())
  })

  it('logical timestamp-nanos', () => {
    /** @type {AvroType} */
    const schema = {
      type: 'record',
      name: 'EventNanos',
      fields: [
        {
          name: 'ts',
          type: { type: 'long', logicalType: 'timestamp-nanos' },
        },
      ],
    }

    const ts = new Date('2024-01-02T03:04:05.006Z')
    const writer = new ByteWriter()
    avroWrite({ writer, schema, records: [{ ts }] })

    const reader = { view: writer.view, offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const round = avroRead({ reader, metadata, syncMarker })

    expect(round[0].ts.getTime()).toBe(ts.getTime())
  })

  it('logical decimal', () => {
    /** @type {AvroType} */
    const schema = {
      type: 'record',
      name: 'Money',
      fields: [
        {
          name: 'price',
          type: { type: 'bytes', logicalType: 'decimal', precision: 9, scale: 2 },
        },
      ],
    }

    // Multi-byte unscaled values exercise the zigzag length prefix; a single
    // wrong-encoding byte made the decoder run off the end with negative length.
    const recs = [
      { price: 0 },
      { price: 12.34 },
      { price: -1.23 },
      { price: 99999.99 },
    ]

    const writer = new ByteWriter()
    avroWrite({ writer, schema, records: recs })

    const reader = { view: writer.view, offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const got = avroRead({ reader, metadata, syncMarker })

    expect(got).toEqual(recs)
  })

  it('array + map round-trip', () => {
    /** @type {AvroType} */
    const schema = {
      type: 'record',
      name: 'Complex',
      fields: [
        { name: 'tags', type: { type: 'array', items: 'string' } },
        { name: 'metrics', type: ['null', { type: 'record', name: 'Metrics', fields: [
          { name: 'x', type: 'int' },
          { name: 'y', type: ['null', 'int'] },
        ] }] },
      ],
    }

    const recs = [
      { tags: ['a', 'b'], metrics: { x: 1, y: 2 } },
      { tags: [], metrics: { x: -1, y: undefined } },
      { tags: [], metrics: undefined },
    ]

    const writer = new ByteWriter()
    avroWrite({ writer, schema, records: recs })

    const reader = { view: writer.view, offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const got = avroRead({ reader, metadata, syncMarker })

    expect(got).toEqual(recs)
  })
})
