import { describe, expect, it } from 'vitest'
import { ByteWriter } from 'hyparquet-writer'
import { avroMetadata, avroRead } from '../src/avro/index.js'

/**
 * @import {AvroType} from '../src/types.js'
 */

/**
 * @param {InstanceType<typeof ByteWriter>} w
 * @param {string} s
 */
function writeString(w, s) {
  const b = new TextEncoder().encode(s)
  w.appendZigZag(b.length)
  w.appendBytes(b)
}

/**
 * Hand-build an Avro object-container file for a single record so we can
 * exercise reader-only schema shapes that `avroWrite` does not emit.
 *
 * @param {AvroType} schema
 * @param {Uint8Array} recordBytes
 * @returns {Uint8Array}
 */
function container(schema, recordBytes) {
  const sync = new Uint8Array(16).fill(7)
  const w = new ByteWriter()
  // magic "Obj\x01"
  w.appendBytes(new Uint8Array([0x4f, 0x62, 0x6a, 0x01]))
  // metadata map: avro.schema + avro.codec, then a 0 terminator block
  w.appendZigZag(2)
  writeString(w, 'avro.schema')
  writeString(w, JSON.stringify(schema))
  writeString(w, 'avro.codec')
  writeString(w, 'null')
  w.appendZigZag(0)
  w.appendBytes(sync)
  // one data block: record count, block byte size, data, block sync
  w.appendZigZag(1)
  w.appendZigZag(recordBytes.length)
  w.appendBytes(recordBytes)
  w.appendBytes(sync)
  return w.getBytes().slice()
}

describe('avroRead S3 Tables schema shapes', () => {
  it('reads boxed primitives, maps, and enums', () => {
    /** @type {AvroType} */
    const schema = {
      type: 'record',
      name: 'R',
      fields: [
        { name: 's', type: { type: 'string' } },
        { name: 'n', type: { type: 'long' } },
        { name: 'm', type: { type: 'map', values: 'long' } },
        { name: 'e', type: { type: 'enum', name: 'Color', symbols: ['RED', 'GREEN', 'BLUE'] } },
      ],
    }

    const rec = new ByteWriter()
    writeString(rec, 'hi')
    rec.appendZigZag(5n)
    // map { a: 1, b: 2 }
    rec.appendZigZag(2)
    writeString(rec, 'a')
    rec.appendZigZag(1n)
    writeString(rec, 'b')
    rec.appendZigZag(2n)
    rec.appendZigZag(0)
    // enum index 1 -> GREEN
    rec.appendZigZag(1)

    const bytes = container(schema, rec.getBytes().slice())
    const reader = { view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength), offset: 0 }
    const { metadata, syncMarker } = avroMetadata(reader)
    const rows = avroRead({ reader, metadata, syncMarker })

    expect(rows).toEqual([{ s: 'hi', n: 5n, m: { a: 1n, b: 2n }, e: 'GREEN' }])
  })
})
