import { decodeAvroRecords } from './avro.js'

/**
 * Translates an S3A URL to an HTTPS URL for direct access to the object.
 *
 * @param {string} url
 * @returns {string}
 */
export function translateS3Url(url) {
  if (url.startsWith('s3a://')) {
    const rest = url.slice('s3a://'.length)
    const slashIndex = rest.indexOf('/')
    if (slashIndex === -1) {
      throw new Error('Invalid S3 URL, missing "/" after bucket')
    }
    const bucket = rest.slice(0, slashIndex)
    const key = rest.slice(slashIndex)
    return `https://${bucket}.s3.amazonaws.com${key}`
  }
  return url
}

/**
 * Fetches the Iceberg table metadata JSON from S3
 *
 * @import {IcebergMetadata} from './types.js'
 * @param {string} tableBaseUrl - Base S3 URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {string} metadataFileName - Name of the metadata JSON file
 * @returns {Promise<IcebergMetadata>} The table metadata as a JSON object
 */
export function fetchIcebergMetadata(tableBaseUrl, metadataFileName) {
  const url = `${tableBaseUrl.replace(/\/$/, '')}/metadata/${metadataFileName}`
  const safeUrl = translateS3Url(url)
  return fetch(safeUrl).then(res => res.json())
}

/**
 * Fetches data files information from multiple manifest file URLs.
 *
 * @import {DataFile} from './types.d.ts'
 * @param {string[]} manifestUrls - The URLs of the manifest files
 * @returns {Promise<DataFile[]>} Array of data file information
 */
export async function fetchDataFilesFromManifests(manifestUrls) {
  /** @type {DataFile[]} */
  const files = []
  for (const url of manifestUrls) {
    const records = await decodeAvroRecords(url)
    for (const rec of records) {
      files.push(rec.data_file)
    }
  }
  return files
}
