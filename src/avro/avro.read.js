import { gunzip } from 'hyparquet-compressors'
import { readZigZag, readZigZagBigInt } from './avro.metadata.js'
import { parseDecimal } from 'hyparquet/src/convert.js'

/**
 * Read avro data blocks.
 * Should be called after avroMetadata.
 *
 * @param {Object} options
 * @param {DataReader} options.reader
 * @param {Record<string, any>} options.metadata
 * @param {Uint8Array} options.syncMarker
 * @returns {Record<string, any>[]}
 */
export function avroRead({ reader, metadata, syncMarker }) {
  const blocks = []
  while (reader.offset < reader.view.byteLength) {
    let recordCount = readZigZag(reader)
    // A record count of 0 signals the end of blocks
    if (recordCount === 0) break
    if (recordCount < 0) {
      // TODO: negative count is followed by block size for array / map
      recordCount = -recordCount
    }
    const blockSize = readZigZag(reader)
    let data = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, blockSize)
    reader.offset += blockSize

    // Read and verify sync marker for the block
    const blockSync = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, 16)
    reader.offset += 16
    for (let i = 0; i < 16; i++) {
      if (blockSync[i] !== syncMarker[i]) {
        throw new Error('sync marker does not match')
      }
    }
    const codec = metadata['avro.codec']

    // De-compress data
    if (codec === 'deflate') {
      data = gunzip(data)
    } else if (codec !== 'null') {
      throw new Error(`unsupported codec: ${codec}`)
    }

    // Decode according to binary or json encoding
    // Loop through metadata['avro.schema'] to parse the block
    const { fields } = metadata['avro.schema']
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const dataReader = { view, offset: 0 }
    for (let i = 0; i < recordCount; i++) {
      /** @type {Record<string, any>} */
      const obj = {}
      for (const field of fields) {
        const value = readType(dataReader, field.type)
        obj[field.name] = value
      }
      blocks.push(obj)
    }
  }
  return blocks
}

/**
 * @import {DataReader} from 'hyparquet/src/types.js'
 * @import {AvroType} from '../../src/types.js'
 * @param {DataReader} reader
 * @param {AvroType} type
 * @returns {any}
 */
function readType(reader, type) {
  if (type === 'null') {
    return undefined
  } else if (Array.isArray(type)) {
    const unionIndex = readZigZag(reader)
    return readType(reader, type[unionIndex])
  } else if (typeof type === 'object' && type.type === 'record') {
    // read recursively
    /** @type {Record<string, any>} */
    const obj = {}
    for (const subField of type.fields) {
      obj[subField.name] = readType(reader, subField.type)
    }
    return obj
  } else if (typeof type === 'object' && type.type === 'array') {
    const arr = []
    while (true) {
      let count = readZigZag(reader)
      if (count === 0) break
      if (count < 0) {
        count = -count
        readZigZag(reader) // block size
      }
      for (let i = 0; i < count; i++) {
        arr.push(readType(reader, type.items))
      }
    }
    return arr
  } else if (typeof type === 'object' && type.logicalType) {
    if (type.logicalType === 'date' && type.type === 'int') {
      const value = readZigZag(reader)
      return new Date(value * 86400000)
    } else if (type.logicalType === 'timestamp-millis' && type.type === 'long') {
      const value = readZigZagBigInt(reader)
      return new Date(Number(value))
    } else if (type.logicalType === 'timestamp-micros' && type.type === 'long') {
      const value = readZigZagBigInt(reader)
      return new Date(Number(value / 1000n))
    } else if (type.logicalType === 'decimal' && 'precision' in type) {
      const bytes = readType(reader, type.type)
      const scale = type.scale || 0
      const factor = 10 ** -scale
      return parseDecimal(bytes) * factor
    } else {
      console.warn(`unknown logical type: ${type.logicalType}`)
      return readType(reader, type.type)
    }
  } else if (type === 'boolean') {
    const value = reader.view.getUint8(reader.offset) === 1
    reader.offset++
    return value
  } else if (type === 'int') {
    return readZigZag(reader)
  } else if (type === 'long') {
    return readZigZagBigInt(reader)
  } else if (type === 'float') {
    const value = reader.view.getFloat32(reader.offset, true)
    reader.offset += 4
    return value
  } else if (type === 'double') {
    const value = reader.view.getFloat64(reader.offset, true)
    reader.offset += 8
    return value
  } else if (type === 'bytes') {
    const length = readZigZag(reader)
    const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
    reader.offset += length
    return bytes
  } else if (type === 'string') {
    const length = readZigZag(reader)
    const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
    const text = new TextDecoder().decode(bytes)
    reader.offset += length
    return text
  } else {
    // enum, fixed, null, map
    throw new Error(`unsupported type: ${type}`)
  }
}
