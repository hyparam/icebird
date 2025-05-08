import { ByteWriter } from 'hyparquet-writer'

/**
 * @param {Object} options
 * @param {Writer} options.writer
 * @param {AvroRecord} options.schema
 * @param {Record<string, any>[]} options.records
 * @param {number} [options.blockSize]
 */
export function avroWrite({ writer, schema, records, blockSize = 512 }) {
  writer.appendUint32(0x016a624f) // Obj\x01

  const meta = {
    'avro.schema': typeof schema === 'string' ? schema : JSON.stringify(schema),
    'avro.codec': 'null',
  }
  appendZigZag(writer, Object.keys(meta).length)
  for (const [key, value] of Object.entries(meta)) {
    const kb = new TextEncoder().encode(key)
    appendZigZag(writer, kb.length)
    writer.appendBytes(kb)
    const vb = new TextEncoder().encode(value)
    appendZigZag(writer, vb.length)
    writer.appendBytes(vb)
  }
  writer.appendVarInt(0)

  const sync = new Uint8Array(16)
  for (let i = 0; i < 16; i++) sync[i] = Math.random() * 256 | 0
  writer.appendBytes(sync)

  for (let i = 0; i < records.length; i += blockSize) {
    const block = records.slice(i, i + blockSize)
    appendZigZag(writer, block.length) // record count
    const blockWriter = new ByteWriter()
    for (const record of block) {
      for (const { name, type } of schema.fields) {
        writeType(blockWriter, type, record[name])
      }
    }
    appendZigZag(writer, blockWriter.offset) // block size
    writer.appendBuffer(blockWriter.getBuffer())
    writer.appendBytes(sync)
  }

  writer.finish()
}

/**
 * @import {Writer} from 'hyparquet-writer/src/types.js'
 * @import {AvroRecord, AvroType} from '../../src/types.js'
 * @param {Writer} writer
 * @param {AvroType} schema
 * @param {*} value
 */
function writeType(writer, schema, value) {
  if (Array.isArray(schema)) {
    // find matching union branch
    const unionIndex = schema.findIndex(s => {
      if (Array.isArray(s)) throw new Error('nested unions not supported')

      // normalise branch to a tag string we can test against
      const tag = typeof s === 'string' ? s : 'logicalType' in s ? s.logicalType : s.type

      if (value == null) return tag === 'null'
      if (tag === 'boolean') return typeof value === 'boolean'
      if (tag === 'int') return typeof value === 'number' && Number.isInteger(value)
      if (tag === 'long') return typeof value === 'bigint' || typeof value === 'number'
      if (tag === 'float' || tag === 'double') return typeof value === 'number'
      if (tag === 'string') return typeof value === 'string'
      if (tag === 'bytes') return value instanceof Uint8Array
      if (tag === 'record') return typeof value === 'object' && value !== null
      if (tag === 'array') return Array.isArray(value)
      return false
    })

    if (unionIndex === -1) throw new Error('union branch not found')
    appendZigZag(writer, unionIndex)
    writeType(writer, schema[unionIndex], value)
  } else if (typeof schema === 'string') {
    // primitive type
    if (schema === 'null') {
      // no-op
    } else if (schema === 'boolean') {
      writer.appendUint8(value ? 1 : 0)
    } else if (schema === 'int') {
      if (typeof value !== 'number' || !Number.isInteger(value)) {
        throw new Error('expected integer value')
      }
      appendZigZag(writer, value)
    } else if (schema === 'long') {
      if (typeof value !== 'bigint') throw new Error('expected bigint value')
      appendZigZag64(writer, value)
    } else if (schema === 'float') {
      if (typeof value !== 'number') throw new Error('expected number value')
      writer.appendFloat32(value)
    } else if (schema === 'double') {
      if (typeof value !== 'number') throw new Error('expected number value')
      writer.appendFloat64(value)
    } else if (schema === 'bytes') {
      if (!(value instanceof Uint8Array)) throw new Error('expected Uint8Array value')
      appendZigZag(writer, value.length)
      writer.appendBytes(value)
    } else if (schema === 'string') {
      if (typeof value !== 'string') throw new Error('expected string value')
      const b = new TextEncoder().encode(value)
      appendZigZag(writer, b.length)
      writer.appendBytes(b)
    }
  } else if ('logicalType' in schema) {
    if (schema.logicalType === 'date') {
      appendZigZag(writer, value instanceof Date ? Math.floor(value.getTime() / 86400000) : value)
    } else if (schema.logicalType === 'timestamp-millis') {
      appendZigZag64(writer, value instanceof Date ? BigInt(value.getTime()) : BigInt(value))
    } else if (schema.logicalType === 'timestamp-micros') {
      appendZigZag64(
        writer,
        value instanceof Date ? BigInt(value.getTime()) * 1000n : BigInt(value)
      )
    } else if (schema.logicalType === 'decimal') {
      const scale = 'scale' in schema ? schema.scale ?? 0 : 0
      let u
      if (typeof value === 'bigint') {
        u = value
      } else if (typeof value === 'number') {
        u = BigInt(Math.round(value * 10 ** scale))
      } else {
        throw new Error('decimal value must be bigint or number')
      }
      const b = bigIntToBytes(u)
      writer.appendVarInt(b.length)
      writer.appendBytes(b)
    } else {
      throw new Error(`unknown logical type ${schema.logicalType}`)
    }
  } else if (schema.type === 'record') {
    for (const f of schema.fields) {
      writeType(writer, f.type, value[f.name])
    }
  } else if (schema.type === 'array') {
    if (value.length) {
      appendZigZag(writer, value.length)
      for (const it of value) {
        writeType(writer, schema.items, it)
      }
    }
    writer.appendVarInt(0)
  } else {
    throw new Error(`unknown schema type ${JSON.stringify(schema)}`)
  }
}

/**
 * @param {Writer} writer
 * @param {number} v
 */
function appendZigZag(writer, v) {
  writer.appendVarInt(v << 1 ^ v >> 31)
}

/**
 * @param {Writer} writer
 * @param {bigint} v
 */
function appendZigZag64(writer, v) {
  writer.appendVarBigInt(v << 1n ^ v >> 63n)
}

/**
 * Convert a signed BigInt into two’s-complement big-endian bytes.
 * @param {bigint} value
 * @returns {Uint8Array}
 */
function bigIntToBytes(value) {
  const neg = value < 0n
  let abs = neg ? -value : value
  const out = []
  while (abs > 0n) { out.unshift(Number(abs & 0xffn)); abs >>= 8n }
  if (out.length === 0) out.push(0)

  if (neg) {
    for (let i = 0; i < out.length; i++) out[i] ^= 0xff
    for (let i = out.length - 1; i >= 0; i--) {
      out[i] = out[i] + 1 & 0xff
      if (out[i]) break
    }
    if ((out[0] & 0x80) === 0) out.unshift(0xff)
  } else if ((out[0] & 0x80) !== 0) {
    out.unshift(0)
  }

  return Uint8Array.from(out)
}
