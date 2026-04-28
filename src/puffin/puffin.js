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

/**
 * @typedef {object} PuffinBlobInput
 * @property {string} type
 * @property {number[]} [fields]
 * @property {number|bigint} [snapshotId]
 * @property {number|bigint} [sequenceNumber]
 * @property {Uint8Array} data
 * @property {Record<string, string>} [properties]
 */

/**
 * Assemble a Puffin file from one or more uncompressed blobs. The footer
 * payload is JSON (uncompressed, flags=0), matching what `readPuffinMetadata`
 * parses. Blob `offset`/`length` values are computed from the assembled
 * layout, so callers do not need to pre-compute them.
 *
 * @param {object} options
 * @param {PuffinBlobInput[]} options.blobs
 * @param {Record<string, string>} [options.properties]
 * @returns {Uint8Array}
 */
export function writePuffinFile({ blobs, properties }) {
  if (!Array.isArray(blobs) || !blobs.length) {
    throw new Error('writePuffinFile requires at least one blob')
  }
  let cursor = 4
  const blobMeta = blobs.map(blob => {
    if (!blob.type) throw new Error('puffin blob type is required')
    if (!(blob.data instanceof Uint8Array)) {
      throw new Error('puffin blob data must be a Uint8Array')
    }
    /** @type {Record<string, unknown>} */
    const meta = {
      type: blob.type,
      fields: blob.fields ?? [],
      'snapshot-id': toSafeNumber(blob.snapshotId ?? -1),
      'sequence-number': toSafeNumber(blob.sequenceNumber ?? -1),
      offset: cursor,
      length: blob.data.byteLength,
    }
    if (blob.properties) meta.properties = blob.properties
    cursor += blob.data.byteLength
    return meta
  })
  /** @type {Record<string, unknown>} */
  const footer = { blobs: blobMeta }
  if (properties) footer.properties = properties
  const footerPayload = new TextEncoder().encode(JSON.stringify(footer))

  const blobsBytes = blobs.reduce((sum, b) => sum + b.data.byteLength, 0)
  const totalSize = 4 + blobsBytes + 4 + footerPayload.byteLength + 4 + 4 + 4
  const out = new Uint8Array(totalSize)
  const view = new DataView(out.buffer)
  let offset = 0
  view.setUint32(offset, PUFFIN_MAGIC, false)
  offset += 4
  for (const blob of blobs) {
    out.set(blob.data, offset)
    offset += blob.data.byteLength
  }
  view.setUint32(offset, PUFFIN_MAGIC, false)
  offset += 4
  out.set(footerPayload, offset)
  offset += footerPayload.byteLength
  view.setInt32(offset, footerPayload.byteLength, true)
  offset += 4
  view.setUint32(offset, 0, true)
  offset += 4
  view.setUint32(offset, PUFFIN_MAGIC, false)
  return out
}
