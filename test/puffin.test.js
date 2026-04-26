import { describe, expect, it } from 'vitest'
import { fetchDeleteMaps } from '../src/fetch.js'
import { readDeletionVector } from '../src/puffin/deletion-vector.js'
import { puffinReadDeletionVector, readPuffinMetadata } from '../src/puffin/puffin.js'
import { readRoaringBitmap32 } from '../src/puffin/roaring.js'

describe('puffin deletion vectors', () => {
  it('decodes roaring array, bitmap, and run containers', () => {
    expect(readRoaringBitmap32(roaringArray([1, 5, 65535]))).toEqual([1, 5, 65535])

    const bitmapValues = Array.from({ length: 4097 }, (_, i) => i * 2)
    expect(readRoaringBitmap32(roaringBitmap(bitmapValues))).toEqual(bitmapValues)

    expect(readRoaringBitmap32(roaringRun([[10, 3], [20, 1]]))).toEqual([10, 11, 12, 20])
  })

  it('decodes deletion-vector-v1 blobs', () => {
    const blob = deletionVector([1n, 5n, 4294967301n])

    expect(readDeletionVector(blob)).toEqual(new Set([1n, 5n, 4294967301n]))
  })

  it('reads deletion-vector-v1 blobs from puffin files by offset and length', async () => {
    const referencedDataFile = 's3://bucket/table/data/a.parquet'
    const blob = deletionVector([1n, 3n, 9n])
    const file = puffinFile(blob, referencedDataFile)
    const metadata = readPuffinMetadata(file)
    const [blobMetadata] = metadata.blobs

    const deletes = await puffinReadDeletionVector(asyncBuffer(file), {
      offset: blobMetadata.offset,
      length: blobMetadata.length,
      referencedDataFile,
    })

    expect(deletes).toEqual(new Set([1n, 3n, 9n]))
  })

  it('loads puffin deletion vectors into position delete maps', async () => {
    const referencedDataFile = 's3://bucket/table/data/a.parquet'
    const blob = deletionVector([2n, 4n])
    const file = puffinFile(blob, referencedDataFile)
    const metadata = readPuffinMetadata(file)
    const [blobMetadata] = metadata.blobs
    const { positionDeletesMap } = await fetchDeleteMaps([{
      status: 1,
      sequence_number: 1n,
      file_sequence_number: 1n,
      data_file: {
        content: 1,
        file_path: 's3://bucket/table/metadata/dv.puffin',
        file_format: 'puffin',
        partition: {},
        record_count: 2n,
        file_size_in_bytes: BigInt(file.byteLength),
        referenced_data_file: referencedDataFile,
        content_offset: BigInt(blobMetadata.offset),
        content_size_in_bytes: BigInt(blobMetadata.length),
      },
    }], {
      reader() {
        return asyncBuffer(file)
      },
    })

    expect(positionDeletesMap.get(referencedDataFile)).toEqual([{
      deleteEntry: expect.objectContaining({ sequence_number: 1n }),
      positions: new Set([2n, 4n]),
    }])
  })
})

/**
 * @param {Uint8Array} bytes
 * @returns {import('hyparquet').AsyncBuffer}
 */
function asyncBuffer(bytes) {
  return {
    byteLength: bytes.byteLength,
    slice(start, end) {
      const { buffer } = /** @type {{ buffer: ArrayBuffer }} */ (bytes)
      return buffer.slice(bytes.byteOffset + start, end === undefined ? bytes.byteLength : bytes.byteOffset + end)
    },
  }
}

/**
 * @param {Uint8Array} blob
 * @param {string} referencedDataFile
 * @returns {Uint8Array}
 */
function puffinFile(blob, referencedDataFile) {
  const blobOffset = 4
  const footer = {
    blobs: [{
      type: 'deletion-vector-v1',
      fields: [],
      'snapshot-id': -1,
      'sequence-number': -1,
      offset: blobOffset,
      length: blob.byteLength,
      properties: {
        'referenced-data-file': referencedDataFile,
        cardinality: '3',
      },
    }],
  }
  const footerPayload = new TextEncoder().encode(JSON.stringify(footer))
  const out = new Uint8Array(4 + blob.byteLength + 4 + footerPayload.byteLength + 4 + 4 + 4)
  const view = new DataView(out.buffer)
  let offset = 0
  view.setUint32(offset, 0x50464131, false)
  offset += 4
  out.set(blob, offset)
  offset += blob.byteLength
  view.setUint32(offset, 0x50464131, false)
  offset += 4
  out.set(footerPayload, offset)
  offset += footerPayload.byteLength
  view.setInt32(offset, footerPayload.byteLength, true)
  offset += 4
  view.setUint32(offset, 0, true)
  offset += 4
  view.setUint32(offset, 0x50464131, false)
  return out
}

/**
 * @param {bigint[]} positions
 * @returns {Uint8Array}
 */
function deletionVector(positions) {
  const byKey = new Map()
  for (const pos of positions) {
    const key = Number(pos >> 32n)
    const value = Number(pos & 0xffffffffn)
    const values = byKey.get(key) ?? []
    values.push(value)
    byKey.set(key, values)
  }

  const bitmaps = [...byKey.entries()]
    .sort(([a], [b]) => a - b)
    .map(([key, values]) => ({ key, bitmap: roaringArray(values) }))
  const vectorLength = 8 + bitmaps.reduce((sum, { bitmap }) => sum + 4 + bitmap.byteLength, 0)
  const vector = new Uint8Array(vectorLength)
  const vectorView = new DataView(vector.buffer)
  vectorView.setBigUint64(0, BigInt(bitmaps.length), true)
  let vectorOffset = 8
  for (const { key, bitmap } of bitmaps) {
    vectorView.setUint32(vectorOffset, key, true)
    vectorOffset += 4
    vector.set(bitmap, vectorOffset)
    vectorOffset += bitmap.byteLength
  }

  const payloadLength = 4 + vector.byteLength
  const out = new Uint8Array(4 + payloadLength + 4)
  const view = new DataView(out.buffer)
  view.setUint32(0, payloadLength, false)
  view.setUint32(4, 0xd1d33964, false)
  out.set(vector, 8)
  view.setUint32(out.byteLength - 4, crc32(out.subarray(4, out.byteLength - 4)), false)
  return out
}

/**
 * @param {number[]} values
 * @returns {Uint8Array}
 */
function roaringArray(values) {
  values = [...values].sort((a, b) => a - b)
  const out = new Uint8Array(16 + values.length * 2)
  const view = new DataView(out.buffer)
  view.setUint32(0, 12346, true)
  view.setUint32(4, 1, true)
  view.setUint16(8, 0, true)
  view.setUint16(10, values.length - 1, true)
  view.setUint32(12, 16, true)
  let offset = 16
  for (const value of values) {
    view.setUint16(offset, value, true)
    offset += 2
  }
  return out
}

/**
 * @param {number[]} values
 * @returns {Uint8Array}
 */
function roaringBitmap(values) {
  const out = new Uint8Array(16 + 8192)
  const view = new DataView(out.buffer)
  view.setUint32(0, 12346, true)
  view.setUint32(4, 1, true)
  view.setUint16(8, 0, true)
  view.setUint16(10, values.length - 1, true)
  view.setUint32(12, 16, true)
  for (const value of values) {
    const wordOffset = 16 + Math.floor(value / 64) * 8
    const bit = BigInt(value % 64)
    view.setBigUint64(wordOffset, view.getBigUint64(wordOffset, true) | 1n << bit, true)
  }
  return out
}

/**
 * @param {[number, number][]} runs
 * @returns {Uint8Array}
 */
function roaringRun(runs) {
  const cardinality = runs.reduce((sum, [, length]) => sum + length, 0)
  const out = new Uint8Array(4 + 1 + 4 + 2 + runs.length * 4)
  const view = new DataView(out.buffer)
  view.setUint32(0, 12347, true)
  out[4] = 1
  view.setUint16(5, 0, true)
  view.setUint16(7, cardinality - 1, true)
  view.setUint16(9, runs.length, true)
  let offset = 11
  for (const [start, length] of runs) {
    view.setUint16(offset, start, true)
    view.setUint16(offset + 2, length - 1, true)
    offset += 4
  }
  return out
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
    crc = crcTable[(crc ^ byte) & 0xff] ^ crc >>> 8
  }
  crc = ~crc
  return crc >>> 0
}

/**
 * @returns {Uint32Array}
 */
function makeCrcTable() {
  const table = new Uint32Array(256)
  for (let i = 0; i < table.length; i++) {
    let c = i
    for (let j = 0; j < 8; j++) {
      c = c & 1 ? 0xedb88320 ^ c >>> 1 : c >>> 1
    }
    table[i] = c >>> 0
  }
  return table
}
