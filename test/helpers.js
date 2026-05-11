import fs from 'fs'
import path from 'path'
import { asyncBufferFromFile } from 'hyparquet'
import { ByteWriter } from 'hyparquet-writer'
import { s3ParseUrl } from '../src/fetch.js'

/**
 * @import {Lister, Resolver} from '../src/types.js'
 */

/**
 * Read file and parse as JSON
 *
 * @param {string} filePath
 * @returns {any}
 */
export function fileToJson(filePath) {
  const buffer = fs.readFileSync(filePath)
  return JSON.parse(buffer.toString())
}

/**
 * Maps an s3:// / s3a:// / https://...s3.amazonaws.com/ URL to a local path
 * under baseDir, mirroring the bucket layout (`<baseDir>/<bucket>/<prefix>`).
 *
 * @param {string} baseDir
 * @param {string} url
 * @returns {string}
 */
function localPath(baseDir, url) {
  const parts = s3ParseUrl(url)
  if (!parts) throw new Error(`not an S3 URL: ${url}`)
  return path.join(baseDir, parts.bucket, parts.prefix)
}

/**
 * Resolver that reads files from a local fixtures directory mirroring the
 * S3 bucket layout. Useful for tests that would otherwise fetch from S3.
 *
 * @param {string} baseDir
 * @returns {Resolver}
 */
export function localResolver(baseDir) {
  return {
    reader(url) {
      return asyncBufferFromFile(localPath(baseDir, url))
    },
  }
}

/**
 * Lister that reads directory entries from a local fixtures directory
 * mirroring the S3 bucket layout.
 *
 * @param {string} baseDir
 * @returns {Lister}
 */
export function localLister(baseDir) {
  return function list(url) {
    return Promise.resolve(fs.readdirSync(localPath(baseDir, url)))
  }
}

/**
 * In-memory Resolver backed by a Map. Useful for round-trip tests that want
 * to write and then read back without touching the filesystem or S3.
 *
 * @returns {{ resolver: Resolver, files: Map<string, Uint8Array>, lister: Lister }}
 */
export function memResolver() {
  /** @type {Map<string, Uint8Array>} */
  const files = new Map()
  /** @type {Resolver} */
  const resolver = {
    reader(p) {
      const bytes = files.get(p)
      if (!bytes) throw new Error(`no such file: ${p}`)
      const ab = /** @type {ArrayBuffer} */ (
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
      )
      return {
        byteLength: bytes.byteLength,
        slice: (/** @type {number} */ s, /** @type {number} */ e) => ab.slice(s, e),
      }
    },
    writer(p, options) {
      const w = new ByteWriter()
      const origFinish = w.finish.bind(w)
      w.finish = async () => {
        await origFinish()
        if (options?.ifNoneMatch === '*' && files.has(p)) {
          /** @type {Error & { status?: number }} */
          const err = new Error(`PUT ${p}: 412 Precondition Failed`)
          err.status = 412
          throw err
        }
        files.set(p, w.getBytes())
      }
      return w
    },
    deleter(p) {
      files.delete(p)
      return Promise.resolve()
    },
  }
  /** @type {Lister} */
  function lister(dir) {
    const prefix = dir.endsWith('/') ? dir : `${dir}/`
    /** @type {string[]} */
    const out = []
    for (const k of files.keys()) {
      if (k.startsWith(prefix)) {
        const tail = k.slice(prefix.length)
        if (!tail.includes('/')) out.push(tail)
      }
    }
    return Promise.resolve(out)
  }
  return { resolver, files, lister }
}
