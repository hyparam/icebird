/**
 * @param {DataReader} reader
 * @returns {number} value
 */
export function readZigZag(reader) {
  let result = 0
  let shift = 0
  while (true) {
    const byte = reader.view.getUint8(reader.offset++)
    result |= (byte & 0x7f) << shift
    if (!(byte & 0x80)) {
      // convert zigzag to int
      return result >>> 1 ^ -(result & 1)
    }
    shift += 7
  }
}

/**
 * @param {DataReader} reader
 * @returns {bigint} value
 */
export function readZigZagBigInt(reader) {
  let result = 0n
  let shift = 0n
  while (true) {
    const byte = reader.view.getUint8(reader.offset++)
    result |= BigInt(byte & 0x7f) << shift
    if (!(byte & 0x80)) {
      // convert zigzag to int
      return result >> 1n ^ -(result & 1n)
    }
    shift += 7n
  }
}

/**
 * Read an Avro string from the DataReader
 *
 * @import {DataReader} from 'hyparquet/src/types.js'
 * @param {DataReader} reader
 * @returns {string}
 */
function readAvroString(reader) {
  const length = readZigZag(reader)
  const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
  reader.offset += length
  return new TextDecoder().decode(bytes)
}

/**
 * Read avro header
 *
 * @param {DataReader} reader
 * @returns {{ metadata: Record<string, any>, syncMarker: Uint8Array }}
 */
export function avroMetadata(reader) {
  // Check avro magic bytes "Obj\x01"
  if (reader.view.getUint32(reader.offset) !== 0x4f626a01) {
    throw new Error('avro invalid magic bytes')
  }
  reader.offset += 4

  // Read metadata map (encoded as Avro map: block count then key/value pairs)
  /** @type {Record<string, string>} */
  const metadata = {}
  let mapCount = readZigZag(reader)
  while (mapCount !== 0) {
    if (mapCount < 0) {
      // Negative count signals a block with a byte count that we skip over
      mapCount = -mapCount
      readZigZag(reader) // block byte count
    }
    for (let i = 0; i < mapCount; i++) {
      const key = readAvroString(reader)
      const value = readAvroString(reader)
      metadata[key] = value
    }
    mapCount = readZigZag(reader)
  }

  // Parse avro-specific metadata
  metadata['avro.schema'] = JSON.parse(metadata['avro.schema'])
  if (metadata['schema']) {
    metadata['schema'] = JSON.parse(metadata['schema'])
  }
  if (metadata['iceberg.schema']) {
    metadata['iceberg.schema'] = JSON.parse(metadata['iceberg.schema'])
  }

  // Read 16-byte sync marker
  const syncMarker = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, 16)
  reader.offset += 16

  return { metadata, syncMarker }
}
