/**
 * Decode the Roaring portable bitmap format used inside Iceberg Puffin
 * `deletion-vector-v1` blobs.
 *
 * @param {Uint8Array} bytes
 * @returns {number[]} sorted 32-bit unsigned values
 */
export function readRoaringBitmap32(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  let offset = 0
  const cookie = view.getUint32(offset, true)
  offset += 4

  /** @type {number} */
  let size
  /** @type {Uint8Array | undefined} */
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

  const keys = new Array(size)
  const cardinalities = new Array(size)
  const types = new Array(size)
  for (let i = 0; i < size; i++) {
    keys[i] = view.getUint16(offset, true)
    cardinalities[i] = view.getUint16(offset + 2, true) + 1
    offset += 4
    types[i] = isRunContainer(runContainers, i)
      ? 'run'
      : cardinalities[i] <= 4096 ? 'array' : 'bitmap'
  }

  if (cookie === 12346 || size >= 4) {
    offset += 4 * size
  }

  /** @type {number[]} */
  const values = []
  for (let i = 0; i < size; i++) {
    const keyBase = keys[i] * 65536
    const cardinality = cardinalities[i]
    const type = types[i]
    if (type === 'array') {
      for (let j = 0; j < cardinality; j++) {
        values.push(keyBase + view.getUint16(offset, true))
        offset += 2
      }
    } else if (type === 'bitmap') {
      for (let word = 0; word < 1024; word++) {
        let bits = view.getBigUint64(offset, true)
        offset += 8
        while (bits) {
          const bit = trailingZeroes64(bits)
          values.push(keyBase + word * 64 + bit)
          bits &= bits - 1n
        }
      }
    } else {
      const runCount = view.getUint16(offset, true)
      offset += 2
      for (let run = 0; run < runCount; run++) {
        const start = view.getUint16(offset, true)
        const length = view.getUint16(offset + 2, true) + 1
        offset += 4
        for (let value = start; value < start + length; value++) {
          values.push(keyBase + value)
        }
      }
    }
  }
  return values
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

/**
 * @param {bigint} value
 * @returns {number}
 */
function trailingZeroes64(value) {
  let count = 0
  while ((value & 1n) === 0n) {
    value >>= 1n
    count++
  }
  return count
}
