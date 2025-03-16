import avro from 'avsc'
import { Readable } from 'stream'
import { translateS3Url } from './iceberg.fetch.js'

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
  const decoder = new avro.streams.BlockDecoder()
  /** @type {any[]} */
  const records = []
  return new Promise((resolve, reject) => {
    decoder.on('data', record => records.push(record))
    decoder.on('end', () => resolve(records))
    decoder.on('error', reject)
    nodeStream.pipe(decoder)
  })
}
