import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { avroWrite } from '../src/avro.write.js'
import { avroData, avroMetadata } from '../src/iceberg.js'

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

    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const rows = avroData({ reader, metadata, syncMarker })

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

    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const got = avroData({ reader, metadata, syncMarker })

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

    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const round = avroData({ reader, metadata, syncMarker })

    expect(round[0].ts.getTime()).toBe(original[0].ts.getTime())
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

    const reader = { view: new DataView(writer.getBuffer()), offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const got = avroData({ reader, metadata, syncMarker })

    expect(got).toEqual(recs)
  })
})
