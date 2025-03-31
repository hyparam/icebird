import { translateS3Url } from './iceberg.fetch.js'

/**
 * Fetches the Iceberg table snapshot version using the version hint file.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {RequestInit} [options.requestInit] - Optional fetch request initialization
 * @returns {Promise<number>} The snapshot version
 */
export function icebergLatestVersion({ tableUrl, requestInit }) {
  const url = `${tableUrl}/metadata/version-hint.text`
  const safeUrl = translateS3Url(url)
  // TODO: If version-hint is not found, try listing or binary search.
  return fetch(safeUrl, requestInit)
    .then(async res => {
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      const text = await res.text()
      const version = parseInt(text)
      if (isNaN(version)) throw new Error(`invalid version: ${text}`)
      return version
    })
    .catch(err => {
      throw new Error(`failed to get version hint: ${err.message}`)
    })
}

/**
 * Fetches the Iceberg table metadata.
 * If metadataFileName is not privided, uses icebergLatestVersion to get the version hint.
 *
 * @import {IcebergMetadata} from '../src/types.js'
 * @param {object} options
 * @param {string} options.tableUrl - Base URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {string} [options.metadataFileName] - Name of the metadata JSON file
 * @param {RequestInit} [options.requestInit] - Optional fetch request initialization
 * @returns {Promise<IcebergMetadata>} The table metadata as a JSON object
 */
export async function icebergMetadata({ tableUrl, metadataFileName, requestInit }) {
  if (!metadataFileName) {
    const version = await icebergLatestVersion({ tableUrl, requestInit })
    metadataFileName = `v${version}.metadata.json`
  }
  const url = `${tableUrl}/metadata/${metadataFileName}`
  const safeUrl = translateS3Url(url)
  return fetch(safeUrl, requestInit)
    .then(res => {
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      return res.json()
    })
    .catch(err => {
      throw new Error(`failed to get iceberg metadata: ${err.message}`)
    })
}
