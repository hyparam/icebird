import { avroData } from './avro.data.js'
import { avroMetadata } from './avro.metadata.js'

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
 * Fetches the Iceberg table snapshot version from S3 using the version hint file.
 *
 * @param {string} tableBaseUrl - Base S3 URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @returns {Promise<number>} The snapshot version
 */
export function fetchSnapshotVersion(tableBaseUrl) {
  const url = `${tableBaseUrl}/metadata/version-hint.text`
  const safeUrl = translateS3Url(url)
  // TODO: If version-hint is not found, try listing or binary search.
  return fetch(safeUrl).then(res => res.text()).then(text => parseInt(text))
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
  const url = `${tableBaseUrl}/metadata/${metadataFileName}`
  const safeUrl = translateS3Url(url)
  return fetch(safeUrl).then(res => res.json())
}

/**
 * Fetches data files information from multiple manifest file URLs.
 *
 * @import {DataFile} from './types.js'
 * @param {string[]} manifestUrls - The URLs of the manifest files
 * @returns {Promise<DataFile[]>} Array of data file information
 */
export async function fetchDataFilesFromManifests(manifestUrls) {
  /** @type {DataFile[]} */
  const files = []
  for (const url of manifestUrls) {
    const records = await fetchAvroRecords(url)
    for (const rec of records) {
      files.push(rec.data_file)
    }
  }
  return files
}

/**
 * Decodes Avro records from a url.
 *
 * @param {string} manifestUrl - The URL of the manifest file
 * @returns {Promise<Record<string, any>[]>} The decoded Avro records
 */
export async function fetchAvroRecords(manifestUrl) {
  const safeUrl = translateS3Url(manifestUrl)
  const buffer = await fetch(safeUrl).then(res => res.arrayBuffer())
  const reader = { view: new DataView(buffer), offset: 0 }
  const { metadata, syncMarker } = await avroMetadata(reader)
  return await avroData({ reader, metadata, syncMarker })
}
