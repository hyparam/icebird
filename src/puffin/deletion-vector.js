import { readRoaringBitmap32 } from './roaring.js'

const DV_MAGIC = 0xd1d33964

/**
 * Decode a Puffin `deletion-vector-v1` blob into deleted row positions.
 *
 * @param {Uint8Array} bytes
 * @returns {Set<bigint>}
 */
export function readDeletionVector(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const payloadLength = view.getUint32(0, false)
  if (payloadLength !== bytes.byteLength - 8) {
    throw new Error(`invalid deletion vector length: ${payloadLength}`)
  }
  if (view.getUint32(4, false) !== DV_MAGIC) {
    throw new Error('invalid deletion vector magic')
  }

  const expectedCrc = view.getUint32(bytes.byteLength - 4, false)
  const payload = bytes.subarray(4, bytes.byteLength - 4)
  const actualCrc = crc32(payload)
  if (actualCrc !== expectedCrc) {
    throw new Error('deletion vector crc mismatch')
  }

  const vector = bytes.subarray(8, bytes.byteLength - 4)
  return readPositionVector(vector)
}

/**
 * @param {Uint8Array} bytes
 * @returns {Set<bigint>}
 */
function readPositionVector(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  let offset = 0
  const bitmapCount = view.getBigUint64(offset, true)
  offset += 8
  if (bitmapCount > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`too many deletion vector bitmaps: ${bitmapCount}`)
  }

  /** @type {Set<bigint>} */
  const positions = new Set()
  for (let i = 0; i < Number(bitmapCount); i++) {
    const key = view.getUint32(offset, true)
    offset += 4
    const start = offset
    const values = readRoaringBitmap32(bytes.subarray(start))
    offset = start + roaringBitmap32Length(bytes.subarray(start))
    const keyBase = BigInt(key) << 32n
    for (const value of values) {
      positions.add(keyBase + BigInt(value))
    }
  }
  return positions
}

/**
 * Return the number of bytes used by one serialized 32-bit Roaring bitmap.
 *
 * @param {Uint8Array} bytes
 * @returns {number}
 */
function roaringBitmap32Length(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  let offset = 0
  const cookie = view.getUint32(offset, true)
  offset += 4
  let size
  let runContainers
  if (cookie === 12346) {
    size = view.getUint32(offset, true)
    offset += 4
  } else if ((cookie & 0xffff) === 12347) {
    size = (cookie >>> 16) + 1
    const runBytes = Math.ceil(size / 8)
    runContainers = bytes.subarray(offset, offset + runBytes)
    offset += runBytes
  } else {
    throw new Error(`invalid roaring cookie: ${cookie}`)
  }

  const cardinalities = new Array(size)
  const types = new Array(size)
  for (let i = 0; i < size; i++) {
    offset += 2
    cardinalities[i] = view.getUint16(offset, true) + 1
    offset += 2
    types[i] = isRunContainer(runContainers, i)
      ? 'run'
      : cardinalities[i] <= 4096 ? 'array' : 'bitmap'
  }
  if (cookie === 12346 || size >= 4) {
    offset += 4 * size
  }

  for (let i = 0; i < size; i++) {
    if (types[i] === 'array') {
      offset += cardinalities[i] * 2
    } else if (types[i] === 'bitmap') {
      offset += 8192
    } else {
      const runCount = view.getUint16(offset, true)
      offset += 2 + runCount * 4
    }
  }
  return offset
}

/**
 * @param {Uint8Array | undefined} runContainers
 * @param {number} index
 * @returns {boolean}
 */
function isRunContainer(runContainers, index) {
  if (!runContainers) return false
  return Boolean(runContainers[Math.floor(index / 8)] & (1 << (index % 8)))
}

let crcTable

/**
 * @param {Uint8Array} bytes
 * @returns {number}
 */
function crc32(bytes) {
  crcTable ??= makeCrcTable()
  let crc = 0xffffffff
  for (const byte of bytes) {
    crc = crcTable[(crc ^ byte) & 0xff] ^ (crc >>> 8)
  }
  return (crc ^ 0xffffffff) >>> 0
}

/**
 * @returns {Uint32Array}
 */
function makeCrcTable() {
  const table = new Uint32Array(256)
  for (let i = 0; i < table.length; i++) {
    let c = i
    for (let j = 0; j < 8; j++) {
      c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
    }
    table[i] = c >>> 0
  }
  return table
}
