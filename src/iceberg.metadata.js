import { translateS3Url } from './iceberg.fetch.js'

/**
 * Fetches the Iceberg table snapshot version using the version hint file.
 *
 * @param {string} tableUrl - Base URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @returns {Promise<number>} The snapshot version
 */
export function icebergLatestVersion(tableUrl) {
  const url = `${tableUrl}/metadata/version-hint.text`
  const safeUrl = translateS3Url(url)
  // TODO: If version-hint is not found, try listing or binary search.
  return fetch(safeUrl).then(res => res.text()).then(text => parseInt(text))
}

/**
 * Fetches the Iceberg table metadata.
 * If metadataFileName is not privided, uses icebergLatestVersion to get the version hint.
 *
 * @import {IcebergMetadata} from '../src/types.js'
 * @param {string} tableUrl - Base URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {string} [metadataFileName] - Name of the metadata JSON file
 * @returns {Promise<IcebergMetadata>} The table metadata as a JSON object
 */
export async function icebergMetadata(tableUrl, metadataFileName) {
  if (!metadataFileName) {
    const version = await icebergLatestVersion(tableUrl)
    metadataFileName = `v${version}.metadata.json`
  }
  const url = `${tableUrl}/metadata/${metadataFileName}`
  const safeUrl = translateS3Url(url)
  return fetch(safeUrl).then(res => res.json())
}
