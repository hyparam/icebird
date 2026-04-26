import { readDeletionVector } from './deletion-vector.js'

const PUFFIN_MAGIC = 0x50464131

/**
 * Read a Puffin `deletion-vector-v1` blob selected by manifest offset/length.
 *
 * @import {AsyncBuffer} from 'hyparquet'
 * @import {PuffinFileMetadata} from '../../src/puffin/types.js'
 * @param {AsyncBuffer} file
 * @param {object} options
 * @param {number|bigint} options.offset
 * @param {number|bigint} options.length
 * @param {string} [options.referencedDataFile]
 * @returns {Promise<Set<bigint>>}
 */
export async function puffinReadDeletionVector(file, { offset, length, referencedDataFile }) {
  const buffer = await file.slice(0, file.byteLength)
  const bytes = new Uint8Array(buffer)
  const metadata = readPuffinMetadata(bytes)
  const blob = metadata.blobs.find(blob => {
    return blob.type === 'deletion-vector-v1' &&
      BigInt(blob.offset) === BigInt(offset) &&
      BigInt(blob.length) === BigInt(length)
  })
  if (!blob) throw new Error('puffin deletion-vector-v1 blob not found')
  if (blob['compression-codec']) {
    throw new Error(`unsupported puffin blob compression: ${blob['compression-codec']}`)
  }
  if (referencedDataFile && blob.properties?.['referenced-data-file'] !== referencedDataFile) {
    throw new Error('puffin deletion vector referenced-data-file mismatch')
  }
  const start = toSafeNumber(blob.offset)
  const end = start + toSafeNumber(blob.length)
  return readDeletionVector(bytes.subarray(start, end))
}

/**
 * @param {Uint8Array} bytes
 * @returns {PuffinFileMetadata}
 */
export function readPuffinMetadata(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  if (view.getUint32(0, false) !== PUFFIN_MAGIC) throw new Error('invalid puffin file magic')
  if (view.getUint32(bytes.byteLength - 4, false) !== PUFFIN_MAGIC) {
    throw new Error('invalid puffin footer magic')
  }

  const payloadSize = view.getInt32(bytes.byteLength - 12, true)
  if (payloadSize < 0) throw new Error(`invalid puffin footer payload size: ${payloadSize}`)
  const flags = view.getUint32(bytes.byteLength - 8, true)
  const compressed = Boolean(flags & 1)
  if (flags & ~1) throw new Error(`unsupported puffin footer flags: ${flags}`)
  if (compressed) throw new Error('compressed puffin footers are not supported')

  const payloadStart = bytes.byteLength - 12 - payloadSize
  const footerMagicStart = payloadStart - 4
  if (footerMagicStart < 4 || view.getUint32(footerMagicStart, false) !== PUFFIN_MAGIC) {
    throw new Error('invalid puffin footer start magic')
  }
  const payload = bytes.subarray(payloadStart, payloadStart + payloadSize)
  return JSON.parse(new TextDecoder().decode(payload))
}

/**
 * @param {number|bigint} value
 * @returns {number}
 */
function toSafeNumber(value) {
  const out = Number(value)
  if (!Number.isSafeInteger(out)) throw new Error(`puffin offset exceeds safe integer range: ${value}`)
  return out
}
