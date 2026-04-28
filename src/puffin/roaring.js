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
  return Boolean(runContainers[Math.floor(index / 8)] & 1 << index % 8)
}

/**
 * Encode a sorted list of unsigned 32-bit values as a Roaring bitmap in the
 * portable serialization format used by Iceberg `deletion-vector-v1` blobs.
 *
 * Always emits cookie 12346 (no run containers); each high-16-bit bucket is
 * stored as an array container when its cardinality is at most 4096, and as
 * a bitmap container otherwise.
 *
 * @param {number[]} values - Sorted, deduplicated 32-bit unsigned integers.
 * @returns {Uint8Array}
 */
export function writeRoaringBitmap32(values) {
  /** @type {Map<number, number[]>} */
  const containers = new Map()
  for (const value of values) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new Error(`roaring value out of range: ${value}`)
    }
    const key = Math.floor(value / 65536)
    const lo = value % 65536
    let bucket = containers.get(key)
    if (!bucket) {
      bucket = []
      containers.set(key, bucket)
    }
    const last = bucket.length ? bucket[bucket.length - 1] : -1
    if (lo < last) throw new Error(`roaring values must be sorted: ${value}`)
    if (lo > last) bucket.push(lo)
  }

  const keys = [...containers.keys()].sort((a, b) => a - b)
  const bodies = keys.map(key => {
    const items = /** @type {number[]} */ (containers.get(key))
    if (items.length <= 4096) {
      const buf = new Uint8Array(items.length * 2)
      const view = new DataView(buf.buffer)
      for (let i = 0; i < items.length; i++) view.setUint16(i * 2, items[i], true)
      return { key, cardinality: items.length, bytes: buf }
    }
    const buf = new Uint8Array(8192)
    for (const v of items) buf[v >>> 3] |= 1 << (v & 7)
    return { key, cardinality: items.length, bytes: buf }
  })

  const size = bodies.length
  const headerSize = 8 + 4 * size + 4 * size
  const totalSize = headerSize + bodies.reduce((sum, b) => sum + b.bytes.byteLength, 0)
  const out = new Uint8Array(totalSize)
  const view = new DataView(out.buffer)
  let offset = 0
  view.setUint32(offset, 12346, true)
  offset += 4
  view.setUint32(offset, size, true)
  offset += 4
  for (const body of bodies) {
    view.setUint16(offset, body.key, true)
    view.setUint16(offset + 2, body.cardinality - 1, true)
    offset += 4
  }
  let bodyOffset = headerSize
  for (const body of bodies) {
    view.setUint32(offset, bodyOffset, true)
    offset += 4
    bodyOffset += body.bytes.byteLength
  }
  for (const body of bodies) {
    out.set(body.bytes, offset)
    offset += body.bytes.byteLength
  }
  return out
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
