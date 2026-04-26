import { asyncBufferFromUrl, cachedAsyncBuffer, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { avroRead } from './avro/avro.read.js'
import { avroMetadata } from './avro/avro.metadata.js'

/**
 * @import {ManifestEntry, Lister, Resolver} from '../src/types.js'
 */

/**
 * Translates an S3A URL to an HTTPS URL for direct access to the object.
 *
 * @param {string} url
 * @returns {string}
 */
export function translateS3Url(url) {
  if (url.startsWith('s3a://') || url.startsWith('s3://')) {
    const rest = url.slice(url.indexOf('://') + 3)
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
 * Creates a resolver that fetches files via HTTP, translating S3 URLs.
 *
 * @param {object} [options]
 * @param {RequestInit} [options.requestInit] - Optional fetch request initialization
 * @returns {Resolver}
 */
export function urlResolver({ requestInit } = {}) {
  return {
    reader(url, byteLength) {
      return asyncBufferFromUrl({ url: translateS3Url(url), byteLength, requestInit })
    },
  }
}

/**
 * Creates a lister that lists files in an S3 directory via the S3 XML API.
 * Accepts s3://, s3a://, and https://*.s3.amazonaws.com/ URLs.
 *
 * @param {object} [options]
 * @param {RequestInit} [options.requestInit] - Optional fetch request initialization
 * @returns {Lister}
 */
export function s3Lister({ requestInit } = {}) {
  return async function list(url) {
    const s3parts = s3ParseUrl(url)
    if (!s3parts) throw new Error(`not an S3 URL: ${url}`)
    const { bucket, prefix } = s3parts
    const listUrl = `https://${bucket}.s3.amazonaws.com/?list-type=2&prefix=${prefix.replace(/\/$/, '')}/&delimiter=/`
    const res = await fetch(listUrl, requestInit)
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
    const text = await res.text()
    const regex = /<Contents>(.*?)<\/Contents>/gs
    const matches = text.match(regex) || []
    return matches.map(match => {
      const keyMatch = match.match(/<Key>(.*?)<\/Key>/)
      if (!keyMatch) throw new Error('failed to parse S3 list response')
      // Return just the filename (last path segment)
      return keyMatch[1].split('/').pop() ?? ''
    }).filter(Boolean)
  }
}

/**
 * Checks if a URL is an S3 URL and extracts bucket and prefix.
 *
 * @param {string} url
 * @returns {{ bucket: string, prefix: string } | undefined}
 */
export function s3ParseUrl(url) {
  if (url.startsWith('s3://') || url.startsWith('s3a://')) {
    const parts = url.split('/')
    return { bucket: parts[2], prefix: parts.slice(3).join('/') }
  } else if (url.startsWith('https://s3.amazonaws.com/')) {
    const parts = url.split('/')
    return { bucket: parts[3], prefix: parts.slice(4).join('/') }
  } else if (url.match(/^https:\/\/\w+\.s3\.amazonaws\.com\//)) {
    const parts = url.split('/')
    return { bucket: parts[2].split('.')[0], prefix: parts.slice(3).join('/') }
  }
}

/**
 * Reads delete files from delete manifests.
 * Position deletes are grouped by target data file.
 * Equality deletes are grouped by sequence number.
 *
 * @param {ManifestEntry[]} deleteEntries
 * @param {Resolver} resolver
 * @returns {Promise<{positionDeletesMap: Map<string, Set<bigint>>, equalityDeletesMap: Map<bigint, Record<string, any>[]>}>}
 */
export async function fetchDeleteMaps(deleteEntries, resolver) {
  // Build maps of delete entries keyed by target data file path
  /** @type {Map<string, Set<bigint>>} */
  const positionDeletesMap = new Map()
  /** @type {Map<bigint, Record<string, any>[]>} */
  const equalityDeletesMap = new Map()

  // Fetch delete files in parallel
  await Promise.all(deleteEntries.map(async deleteEntry => {
    const { content, file_path, file_size_in_bytes } = deleteEntry.data_file
    const asyncBuffer = await resolver.reader(file_path, Number(file_size_in_bytes))
    const file = cachedAsyncBuffer(asyncBuffer)
    const deleteRows = await parquetReadObjects({ file, compressors })
    for (const deleteRow of deleteRows) {
      if (content === 1) { // position delete
        const { file_path, pos } = deleteRow
        if (!file_path) throw new Error('position delete missing target file path')
        if (pos === undefined) throw new Error('position delete missing pos')
        // note: pos is relative to the data file's row order
        let set = positionDeletesMap.get(file_path)
        if (!set) {
          set = new Set()
          positionDeletesMap.set(file_path, set)
        }
        set.add(pos)
      } else if (content === 2) { // equality delete
        // save the entire delete row (restrict this to equalityIds?)
        const { sequence_number } = deleteEntry
        if (sequence_number === undefined) {
          throw new Error('equality delete missing sequence number')
        }
        let list = equalityDeletesMap.get(sequence_number)
        if (!list) {
          list = []
          equalityDeletesMap.set(sequence_number, list)
        }
        list.push(deleteRow)
      }
    }
  }))

  return { positionDeletesMap, equalityDeletesMap }
}

/**
 * Read the full contents of a resolved file as a string.
 *
 * @param {Resolver} resolver
 * @param {string} path
 * @returns {Promise<string>}
 */
export async function resolveText(resolver, path) {
  const ab = await resolver.reader(path)
  const buf = await ab.slice(0, ab.byteLength)
  return new TextDecoder().decode(buf)
}

/**
 * Decodes Avro records from a url.
 *
 * @param {string} url - The URL or path of the manifest file
 * @param {Resolver} resolver - Resolves a path to an AsyncBuffer
 * @returns {Promise<Record<string, any>[]>} The decoded Avro records
 */
export async function fetchAvroRecords(url, resolver) {
  const ab = await resolver.reader(url)
  const buffer = await ab.slice(0, ab.byteLength)
  const reader = { view: new DataView(buffer), offset: 0 }
  const { metadata, syncMarker } = await avroMetadata(reader)
  return await avroRead({ reader, metadata, syncMarker })
}
