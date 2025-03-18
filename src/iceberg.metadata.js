import { translateS3Url } from './iceberg.fetch.js'

/**
 * Fetches the Iceberg table snapshot version from S3 using the version hint file.
 *
 * @param {string} tableUrl - Base S3 URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @returns {Promise<number>} The snapshot version
 */
export function fetchLatestSequenceNumber(tableUrl) {
  const url = `${tableUrl}/metadata/version-hint.text`
  const safeUrl = translateS3Url(url)
  // TODO: If version-hint is not found, try listing or binary search.
  return fetch(safeUrl).then(res => res.text()).then(text => parseInt(text))
}

/**
 * Fetches the Iceberg table metadata JSON from S3.
 * If metadataFileName is not privided, uses fetchSnapshotVersion to get the version hint.
 *
 * @import {IcebergMetadata} from './types.js'
 * @param {string} tableUrl - Base S3 URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {string} [metadataFileName] - Name of the metadata JSON file
 * @returns {Promise<IcebergMetadata>} The table metadata as a JSON object
 */
export async function fetchIcebergMetadata(tableUrl, metadataFileName) {
  if (!metadataFileName) {
    const version = await fetchLatestSequenceNumber(tableUrl)
    metadataFileName = `v${version}.metadata.json`
  }
  const url = `${tableUrl}/metadata/${metadataFileName}`
  const safeUrl = translateS3Url(url)
  return fetch(safeUrl).then(res => res.json())
}
