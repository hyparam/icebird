import { describe, expect, it } from 'vitest'
import { fetchDeleteMaps } from '../src/fetch.js'
import { readDeletionVector, writeDeletionVector } from '../src/puffin/deletion-vector.js'
import { puffinReadDeletionVector, readPuffinMetadata, writePuffinFile } from '../src/puffin/puffin.js'
import { readRoaringBitmap32, writeRoaringBitmap32 } from '../src/puffin/roaring.js'

/**
 * @import {AsyncBuffer} from 'hyparquet'
 */

describe('puffin deletion vectors', () => {
  it('decodes roaring run containers', () => {
    // run container with runs [10..12] and [20..20]
    const bytes = new Uint8Array([
      0x3b, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x03, 0x00, 0x02, 0x00,
      0x0a, 0x00, 0x02, 0x00, 0x14, 0x00, 0x00, 0x00,
    ])
    expect(readRoaringBitmap32(bytes)).toEqual([10, 11, 12, 20])
  })

  it('decodes deletion-vector-v1 blobs', () => {
    const blob = writeDeletionVector([1n, 5n, 4294967301n])

    expect(readDeletionVector(blob)).toEqual(new Set([1n, 5n, 4294967301n]))
  })

  it('reads deletion-vector-v1 blobs from puffin files by offset and length', async () => {
    const referencedDataFile = 's3://bucket/table/data/a.parquet'
    const blob = writeDeletionVector([1n, 3n, 9n])
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
    const blob = writeDeletionVector([2n, 4n])
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

describe('puffin writers', () => {
  it('round-trips array-container roaring bitmaps', () => {
    const values = [0, 1, 5, 1000, 65535]
    expect(readRoaringBitmap32(writeRoaringBitmap32(values))).toEqual(values)
  })

  it('round-trips bitmap-container roaring bitmaps', () => {
    const values = Array.from({ length: 4097 }, (_, i) => i * 2)
    expect(readRoaringBitmap32(writeRoaringBitmap32(values))).toEqual(values)
  })

  it('round-trips multi-container roaring bitmaps spanning high-16-bit buckets', () => {
    const values = [1, 65535, 65536, 70000, 4294967295]
    expect(readRoaringBitmap32(writeRoaringBitmap32(values))).toEqual(values)
  })

  it('round-trips an empty roaring bitmap', () => {
    expect(readRoaringBitmap32(writeRoaringBitmap32([]))).toEqual([])
  })

  it('rejects unsorted or out-of-range roaring values', () => {
    expect(() => writeRoaringBitmap32([5, 3])).toThrow(/sorted/)
    expect(() => writeRoaringBitmap32([-1])).toThrow(/out of range/)
    expect(() => writeRoaringBitmap32([0x100000000])).toThrow(/out of range/)
  })

  it('round-trips deletion vectors that span 64-bit buckets', () => {
    const positions = [0n, 1n, 65535n, 4294967296n, 4294967301n, 8589934593n]
    const blob = writeDeletionVector(positions)
    expect(readDeletionVector(blob)).toEqual(new Set(positions))
  })

  it('deduplicates and sorts unsorted deletion-vector input', () => {
    const blob = writeDeletionVector([5n, 1n, 5n, 3n])
    expect(readDeletionVector(blob)).toEqual(new Set([1n, 3n, 5n]))
  })

  it('rejects non-bigint or negative deletion-vector positions', () => {
    expect(() => writeDeletionVector([/** @type {any} */ (5)])).toThrow(/bigint/)
    expect(() => writeDeletionVector([-1n])).toThrow(/non-negative/)
  })

  it('round-trips a puffin file with a single deletion-vector blob', async () => {
    const referencedDataFile = 's3://bucket/table/data/a.parquet'
    const blob = writeDeletionVector([2n, 7n, 4294967300n])
    const file = writePuffinFile({
      blobs: [{
        type: 'deletion-vector-v1',
        fields: [],
        snapshotId: 42,
        sequenceNumber: 1,
        data: blob,
        properties: { 'referenced-data-file': referencedDataFile, cardinality: '3' },
      }],
    })
    const metadata = readPuffinMetadata(file)
    expect(metadata.blobs).toHaveLength(1)
    const [meta] = metadata.blobs
    expect(meta.type).toBe('deletion-vector-v1')
    expect(meta['snapshot-id']).toBe(42)
    expect(meta['sequence-number']).toBe(1)
    expect(meta.offset).toBe(4)
    expect(meta.length).toBe(blob.byteLength)
    expect(meta.properties?.['referenced-data-file']).toBe(referencedDataFile)

    const deletes = await puffinReadDeletionVector(asyncBuffer(file), {
      offset: meta.offset,
      length: meta.length,
      referencedDataFile,
    })
    expect(deletes).toEqual(new Set([2n, 7n, 4294967300n]))
  })

  it('round-trips a puffin file with multiple blobs at distinct offsets', async () => {
    const blobA = writeDeletionVector([1n])
    const blobB = writeDeletionVector([2n, 3n])
    const file = writePuffinFile({
      blobs: [
        { type: 'deletion-vector-v1', data: blobA, properties: { 'referenced-data-file': 'a.parquet' } },
        { type: 'deletion-vector-v1', data: blobB, properties: { 'referenced-data-file': 'b.parquet' } },
      ],
      properties: { 'created-by': 'icebird' },
    })
    const metadata = readPuffinMetadata(file)
    expect(metadata.blobs).toHaveLength(2)
    expect(metadata.properties?.['created-by']).toBe('icebird')
    expect(metadata.blobs[0].offset).toBe(4)
    expect(metadata.blobs[1].offset).toBe(4 + blobA.byteLength)
    expect(metadata.blobs[1].length).toBe(blobB.byteLength)

    const second = await puffinReadDeletionVector(asyncBuffer(file), {
      offset: metadata.blobs[1].offset,
      length: metadata.blobs[1].length,
      referencedDataFile: 'b.parquet',
    })
    expect(second).toEqual(new Set([2n, 3n]))
  })

  it('rejects empty blob list and non-Uint8Array data', () => {
    expect(() => writePuffinFile({ blobs: [] })).toThrow(/at least one blob/)
    expect(() => writePuffinFile({
      blobs: [{ type: 'deletion-vector-v1', data: /** @type {any} */ ('not-bytes') }],
    })).toThrow(/Uint8Array/)
  })
})

/**
 * @param {Uint8Array} bytes
 * @returns {AsyncBuffer}
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
  return writePuffinFile({
    blobs: [{
      type: 'deletion-vector-v1',
      data: blob,
      properties: { 'referenced-data-file': referencedDataFile, cardinality: '3' },
    }],
  })
}
