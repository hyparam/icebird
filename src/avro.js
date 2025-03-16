import avro from 'avsc'
import { Readable } from 'stream'
import { translateS3Url } from './iceberg.fetch.js'

/**
 * Parse long to bigint.
 *
 * @param {avro.Schema} schema
 * @returns {avro.Type}
 */
export function parseHook(schema) {
  return avro.Type.forSchema(schema, {
    registry: {
      long: avro.types.LongType.__with({
        fromBuffer: (/** @type {Buffer} */ buf) => buf.readBigInt64LE(),
        toBuffer: (/** @type {bigint} */ n) => {
          const buf = Buffer.alloc(8)
          buf.writeBigInt64LE(n)
          return buf
        },
        fromJSON: BigInt,
        toJSON: Number,
        isValid: (/** @type {any} */ n) => typeof n === 'bigint',
        compare: (/** @type {Buffer} */ n1, /** @type {Buffer} */ n2) => { return n1 === n2 ? 0 : n1 < n2 ? -1 : 1 },
      }),
    },
  })
}

/**
 * Decodes Avro records from a url.
 *
 * @param {string} manifestUrl - The URL of the manifest file
 * @returns {Promise<any[]>} The decoded Avro records
 */
export async function decodeAvroRecords(manifestUrl) {
  const safeUrl = translateS3Url(manifestUrl)
  const buffer = await fetch(safeUrl).then(res => res.arrayBuffer())
  const blob = new Blob([buffer])
  const webStream = blob.stream()
  // @ts-ignore
  const nodeStream = Readable.fromWeb(webStream)
  const decoder = new avro.streams.BlockDecoder({ parseHook })
  /** @type {any[]} */
  const records = []
  return new Promise((resolve, reject) => {
    decoder.on('data', record => records.push(record))
    decoder.on('end', () => resolve(records))
    decoder.on('error', reject)
    nodeStream.pipe(decoder)
  })
}
