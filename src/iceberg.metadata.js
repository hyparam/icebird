import { translateS3Url } from './iceberg.fetch.js'

/**
 * Fetches the iceberg metadata version using the version hint file.
 * If the version hint file is not found, tries to list S3 to find the latest metadata file.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {RequestInit} [options.requestInit] - Optional fetch request initialization
 * @returns {Promise<string>} The snapshot version
 */
export function icebergLatestVersion({ tableUrl, requestInit }) {
  const url = `${tableUrl}/metadata/version-hint.text`
  const safeUrl = translateS3Url(url)
  // fetch version-hint.text
  return fetch(safeUrl, requestInit)
    .then(async res => {
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      const text = await res.text()
      const version = parseInt(text)
      if (isNaN(version)) throw new Error(`invalid version: ${text}`)
      return `v${version}`
    })
    .catch(err => {
      // version hint not found, try listing S3 bucket
      const s3parts = s3ParseUrl(tableUrl)
      if (s3parts) {
        const { bucket, prefix } = s3parts
        return s3ListVersions(bucket, prefix + '/metadata/')
          .then(files => {
            if (files.length === 0) throw new Error('no metadata files found')
            return files[files.length - 1]
          })
          .catch(err => {
            throw new Error(`failed to list S3 objects: ${err.message}`)
          })
      } else {
        throw err
      }
    })
    .catch(err => {
      throw new Error(`failed to determine latest iceberg version: ${err.message}`)
    })
}

/**
 * Returns a list of iceberg metadata versions.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {RequestInit} [options.requestInit] - Optional fetch request initialization
 * @returns {Promise<string[]>} A list of iceberg table versions
 */
export function icebergListVersions({ tableUrl, requestInit }) {
  const url = `${tableUrl}/metadata/version-hint.text`
  const safeUrl = translateS3Url(url)
  // fetch version-hint.text
  return fetch(safeUrl, requestInit)
    .then(async res => {
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      const text = await res.text()
      const version = parseInt(text)
      if (isNaN(version)) throw new Error(`invalid version: ${text}`)
      return Array.from({ length: version }, (_, i) => `v${i + 1}`)
    })
    .catch(err => {
      // version hint not found, try listing S3 bucket
      const s3parts = s3ParseUrl(tableUrl)
      if (s3parts) {
        const { bucket, prefix } = s3parts
        return s3ListVersions(bucket, prefix + '/metadata/')
      } else {
        throw err
      }
    })
    .catch(err => {
      throw new Error(`failed to determine latest iceberg version: ${err.message}`)
    })
}

/**
 * Fetches the iceberg table metadata.
 * If metadataFileName is not provided, uses icebergLatestVersion to get the version hint.
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
    metadataFileName = `${version}.metadata.json`
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

/**
 * Lists objects in an S3 bucket with a specific prefix.
 *
 * @param {string} bucket
 * @param {string} prefix
 * @returns {Promise<{ key: string, lastModified: string }[]>}
 */
function s3list(bucket, prefix) {
  const url = `https://${bucket}.s3.amazonaws.com/?list-type=2&prefix=${prefix}&delimiter=/`
  return fetch(url)
    .then(res => {
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      return res.text()
    })
    .then(text => {
      // Janky parse XML response with regex:
      const regex = /<Contents>(.*?)<\/Contents>/gs
      const matches = text.match(regex) || []
      const results = []
      for (const match of matches) {
        const keyMatch = match.match(/<Key>(.*?)<\/Key>/)
        const lastModifiedMatch = match.match(/<LastModified>(.*?)<\/LastModified>/)
        if (!keyMatch || !lastModifiedMatch) throw new Error('failed to parse S3 list response')
        const key = keyMatch[1]
        const lastModified = lastModifiedMatch[1]
        results.push({ key, lastModified })
      }
      return results
    })
    .catch(err => {
      throw new Error(`failed to list S3 objects: ${err.message}`)
    })
}

/**
 * @param {string} bucket
 * @param {string} prefix
 * @returns {Promise<string[]>}
 */
function s3ListVersions(bucket, prefix) {
  return s3list(bucket, prefix)
    .then(files => {
      // sort most recent files first
      const sorted = files
        .filter(f => f.key.endsWith('.metadata.json'))
        .sort((a, b) => a.lastModified.localeCompare(b.lastModified))
      return sorted.map(file => file.key.split('/').slice(-1)[0]
        .replace(/\.metadata\.json$/, ''))
    })
    .catch(err => {
      throw new Error(`failed to list S3 objects: ${err.message}`)
    })
}

/**
 * Checks if a URL is an S3 URL.
 *
 * @param {string} url
 * @returns {{ bucket: string, prefix: string } | undefined}
 */
function s3ParseUrl(url) {
  if (url.startsWith('s3://') || url.startsWith('s3a://')) {
    const parts = url.split('/')
    return { bucket: parts[2], prefix: parts.slice(3).join('/') }
  } else if (url.startsWith('https://s3.amazonaws.com/')) {
    const parts = url.split('/')
    return { bucket: parts[3], prefix: parts.slice(4).join('/') }
  } else if (url.match(/https:\/\/\w+\.s3\.amazonaws\.com\//)) {
    const parts = url.split('/')
    return { bucket: parts[2], prefix: parts.slice(3).join('/') }
  }
}
