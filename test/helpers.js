import fs from 'fs'
import path from 'path'
import { asyncBufferFromFile } from 'hyparquet'
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
  return async function list(url) {
    return fs.readdirSync(localPath(baseDir, url))
  }
}
