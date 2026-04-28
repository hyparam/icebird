import { asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { ByteWriter } from 'hyparquet-writer'
import { avroRead } from './avro/avro.read.js'
import { avroMetadata } from './avro/avro.metadata.js'
import { puffinReadDeletionVector } from './puffin/puffin.js'
import { sanitize } from './utils.js'

/**
 * @import {FileMetaData} from 'hyparquet'
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
    writer(url) {
      const w = new ByteWriter()
      w.finish = async function() {
        const target = translateS3Url(url)
        // Snapshot the bytes before the request — fetch may read the body
        // lazily and the underlying ByteWriter buffer must not mutate
        // mid-upload (e.g. if the writer is reused after finish).
        const body = w.getBytes().slice()
        const res = await fetch(target, { ...requestInit, method: 'PUT', body })
        if (!res.ok) {
          throw new Error(`PUT ${url}: ${res.status} ${res.statusText}`)
        }
      }
      return w
    },
    async deleter(url) {
      const res = await fetch(translateS3Url(url), { ...requestInit, method: 'DELETE' })
      // S3 DELETE returns 204 on success and 204 even when the key is absent;
      // tolerate 404 for non-S3 backends that surface "not found" instead.
      if (!res.ok && res.status !== 404) {
        throw new Error(`DELETE ${url}: ${res.status} ${res.statusText}`)
      }
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
 * Position deletes are grouped by target data file. Delete entry metadata is
 * preserved so scan planning can apply partition and sequence scope later.
 *
 * @param {ManifestEntry[]} deleteEntries
 * @param {Resolver} resolver
 * @returns {Promise<{positionDeletesMap: Map<string, {deleteEntry: ManifestEntry, positions: Set<bigint>}[]>, equalityDeleteGroups: {deleteEntry: ManifestEntry, rows: Record<number, any>[]}[]}>}
 */
export async function fetchDeleteMaps(deleteEntries, resolver) {
  // Build maps of delete entries keyed by target data file path
  /** @type {Map<string, {deleteEntry: ManifestEntry, positions: Set<bigint>}[]>} */
  const positionDeletesMap = new Map()
  /** @type {{deleteEntry: ManifestEntry, rows: Record<number, any>[]}[]} */
  const equalityDeleteGroups = []

  // Fetch delete files in parallel
  await Promise.all(deleteEntries.map(async deleteEntry => {
    const { content, file_path, file_size_in_bytes } = deleteEntry.data_file
    const asyncBuffer = await resolver.reader(file_path, Number(file_size_in_bytes))
    const file = cachedAsyncBuffer(asyncBuffer)

    if (content === 1) { // position delete
      if (isDeletionVector(deleteEntry)) {
        const { referenced_data_file, content_offset, content_size_in_bytes } = deleteEntry.data_file
        if (!referenced_data_file) throw new Error('deletion vector missing referenced_data_file')
        if (content_offset == null) throw new Error('deletion vector missing content_offset')
        if (content_size_in_bytes == null) throw new Error('deletion vector missing content_size_in_bytes')
        const positions = await puffinReadDeletionVector(file, {
          offset: content_offset,
          length: content_size_in_bytes,
          referencedDataFile: referenced_data_file,
        })
        const set = new Set()
        for (const pos of positions) {
          set.add(pos)
        }
        addPositionDeleteGroup(positionDeletesMap, referenced_data_file, deleteEntry, set)
      } else {
        const deleteRows = await parquetReadObjects({ file, compressors })
        /** @type {Map<string, Set<bigint>>} */
        const positionsByPath = new Map()
        for (const deleteRow of deleteRows) {
          const { file_path, pos } = deleteRow
          if (!file_path) throw new Error('position delete missing target file path')
          if (pos === undefined) throw new Error('position delete missing pos')
          // note: pos is relative to the data file's row order
          let set = positionsByPath.get(file_path)
          if (!set) {
            set = new Set()
            positionsByPath.set(file_path, set)
          }
          set.add(pos)
        }
        for (const [targetPath, positions] of positionsByPath) {
          addPositionDeleteGroup(positionDeletesMap, targetPath, deleteEntry, positions)
        }
      }
    } else if (content === 2) { // equality delete
      const { sequence_number } = deleteEntry
      const equalityIds = deleteEntry.data_file.equality_ids
      if (sequence_number === undefined) {
        throw new Error('equality delete missing sequence number')
      }
      if (!equalityIds?.length) {
        throw new Error('equality delete missing equality_ids')
      }

      const metadata = await parquetMetadataAsync(file)
      const columnNamesById = equalityColumnNamesById(metadata, equalityIds)
      const columns = equalityIds.map(id => columnNamesById[id])
      const deleteRows = await parquetReadObjects({ file, metadata, columns, compressors })
      const rows = []
      for (const deleteRow of deleteRows) {
        rows.push(equalityPredicate(deleteRow, equalityIds, columnNamesById))
      }
      equalityDeleteGroups.push({ deleteEntry, rows })
    }
  }))

  return { positionDeletesMap, equalityDeleteGroups }
}

/**
 * @param {Map<string, {deleteEntry: ManifestEntry, positions: Set<bigint>}[]>} positionDeletesMap
 * @param {string} targetPath
 * @param {ManifestEntry} deleteEntry
 * @param {Set<bigint>} positions
 */
function addPositionDeleteGroup(positionDeletesMap, targetPath, deleteEntry, positions) {
  let groups = positionDeletesMap.get(targetPath)
  if (!groups) {
    groups = []
    positionDeletesMap.set(targetPath, groups)
  }
  groups.push({ deleteEntry, positions })
}

/**
 * @param {ManifestEntry} deleteEntry
 * @returns {boolean}
 */
function isDeletionVector(deleteEntry) {
  const dataFile = deleteEntry.data_file
  return dataFile.file_format.toLowerCase() === 'puffin' ||
    dataFile.content_offset != null ||
    dataFile.content_size_in_bytes != null
}

/**
 * Map equality field ids to physical parquet column names in a delete file.
 *
 * @param {FileMetaData} parquetMetadata
 * @param {number[]} equalityIds
 * @returns {Record<number, string>}
 */
function equalityColumnNamesById(parquetMetadata, equalityIds) {
  const kv = parquetMetadata.key_value_metadata?.find(k => k.key === 'iceberg.schema')
  if (!kv?.value) throw new Error('equality delete missing iceberg.schema parquet metadata')
  const schema = JSON.parse(kv.value)
  /** @type {Record<number, string>} */
  const out = {}
  for (const id of equalityIds) {
    const field = schema.fields.find((/** @type {{ id: number }} */ f) => f.id === id)
    if (!field) throw new Error(`equality delete missing field id ${id} in parquet schema`)
    out[id] = sanitize(field.name)
  }
  return out
}

/**
 * Keep only the columns identified by equality_ids, keyed by Iceberg field id.
 *
 * @param {Record<string, any>} deleteRow
 * @param {number[]} equalityIds
 * @param {Record<number, string>} columnNamesById
 * @returns {Record<number, any>}
 */
function equalityPredicate(deleteRow, equalityIds, columnNamesById) {
  /** @type {Record<number, any>} */
  const out = {}
  for (const id of equalityIds) {
    out[id] = deleteRow[columnNamesById[id]]
  }
  return out
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
  let buf = await ab.slice(0, ab.byteLength)
  if (isGzip(buf)) {
    buf = await decompressGzip(buf)
  }
  return new TextDecoder().decode(buf)
}

/**
 * @param {ArrayBuffer} buf
 * @returns {boolean}
 */
function isGzip(buf) {
  if (buf.byteLength < 2) return false
  const view = new Uint8Array(buf, 0, 2)
  return view[0] === 0x1f && view[1] === 0x8b
}

/**
 * @param {ArrayBuffer} buf
 * @returns {Promise<ArrayBuffer>}
 */
async function decompressGzip(buf) {
  if (!globalThis.DecompressionStream) {
    throw new Error('gzip decompression is not supported in this environment')
  }
  const stream = new Blob([buf]).stream().pipeThrough(new DecompressionStream('gzip'))
  return await new Response(stream).arrayBuffer()
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
