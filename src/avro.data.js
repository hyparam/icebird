import { inflateSync } from 'fflate'
import { readZigZag, readZigZagBigInt } from './avro.metadata.js'

/**
 * Read avro data blocks.
 * Should be called after avroMetadata.
 *
 * @import {DataReader} from 'hyparquet/src/types.js'
 * @param {Object} options
 * @param {DataReader} options.reader
 * @param {Record<string, any>} options.metadata
 * @param {Uint8Array} options.syncMarker
 * @returns {Record<string, any>[]}
 */
export function avroData({ reader, metadata, syncMarker }) {
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
    let data = new Uint8Array(reader.view.buffer, reader.offset, blockSize)
    reader.offset += blockSize

    // Read and verify sync marker for the block
    const blockSync = new Uint8Array(reader.view.buffer, reader.offset, 16)
    reader.offset += 16
    for (let i = 0; i < 16; i++) {
      if (blockSync[i] !== syncMarker[i]) {
        throw new Error('Sync marker does not match')
      }
    }
    const codec = metadata['avro.codec']

    // De-compress data
    if (codec === 'deflate') {
      data = inflateSync(data)
    } else if (codec !== 'null') {
      throw new Error(`Unsupported codec: ${codec}`)
    }

    // Decode according to binary or json encoding
    // Loop through metadata['avro.schema'] to parse the block
    const { fields } = metadata['avro.schema']
    const dataReader = { view: new DataView(data.buffer), offset: 0 }
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
 * @param {DataReader} reader
 * @param {any} type
 * @returns {any}
 */
function readType(reader, type) {
  if (type === 'null') {
    return null
  } else if (Array.isArray(type)) {
    const unionIndex = readZigZag(reader)
    return readType(reader, type[unionIndex])
  } else if (typeof type === 'object' && type.type === 'record') {
    // Read recursively
    /** @type {Record<string, any>} */
    const obj = {}
    // assert(Array.isArray(type.fields))
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
    const bytes = new Uint8Array(reader.view.buffer, reader.offset, length)
    reader.offset += length
    return bytes
  } else if (type === 'string') {
    const length = readZigZag(reader)
    const text = new TextDecoder().decode(new Uint8Array(reader.view.buffer, reader.offset, length))
    reader.offset += length
    return text
  } else {
    // enum, fixed, null, map
    throw new Error(`Unsupported type: ${type}`)
  }
}
