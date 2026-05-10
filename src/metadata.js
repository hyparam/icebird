import { resolveText, s3Lister, urlResolver } from './fetch.js'

/**
 * @import {Lister, Resolver, TableMetadata} from '../src/types.js'
 */

/**
 * Extract the numeric version from an Iceberg metadata filename.
 *
 * @param {string} file
 * @returns {number|undefined}
 */
function metadataFileVersionNumber(file) {
  const match = file.match(/^(?:v(\d+)|(\d+)-.+)(?:\.metadata\.json|\.gz\.metadata\.json|\.metadata\.json\.gz)$/)
  if (!match) return undefined
  return Number(match[1] ?? match[2])
}

/**
 * Strip an Iceberg metadata suffix, preserving the version naming style.
 *
 * @param {string} file
 * @returns {string|undefined}
 */
function metadataFileVersionName(file) {
  if (metadataFileVersionNumber(file) === undefined) return undefined
  return file.replace(/(?:\.metadata\.json\.gz|\.gz\.metadata\.json|\.metadata\.json)$/, '')
}

/**
 * Return normalized version names from metadata filenames.
 *
 * @param {string[]} files
 * @returns {string[]}
 */
function metadataVersions(files) {
  /** @type {Map<number, string>} */
  const versions = new Map()
  for (const file of files) {
    const version = metadataFileVersionNumber(file)
    const name = metadataFileVersionName(file)
    if (version === undefined || name === undefined) continue
    const current = versions.get(version)
    const paddedVersion = String(version).padStart(5, '0')
    if (
      current === undefined ||
      metadataFilePreference(file, paddedVersion) < metadataFilePreference(`${current}.metadata.json`, paddedVersion)
    ) {
      versions.set(version, name)
    }
  }
  return [...versions.entries()]
    .sort(([a], [b]) => a - b)
    .map(([, name]) => name)
}

/**
 * Fetches the iceberg metadata version using the version hint file.
 * If the version hint file is not found, tries to list metadata dir to find the latest metadata file.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {Resolver} [options.resolver] - Resolves a path to an AsyncBuffer
 * @param {Lister} [options.lister] - Lists files in a directory
 * @returns {Promise<string>} The snapshot version
 */
export function icebergLatestVersion({ tableUrl, resolver, lister }) {
  resolver ??= urlResolver()
  lister ??= s3Lister()
  const url = `${tableUrl}/metadata/version-hint.text`
  return resolveText(resolver, url)
    .then(text => {
      const version = parseInt(text)
      if (isNaN(version)) throw new Error(`invalid version: ${text}`)
      return `v${version}`
    })
    .catch(() => {
      // version hint not found, try listing metadata dir
      const metadataDir = `${tableUrl}/metadata`
      return lister(metadataDir)
        .then(files => {
          const versions = metadataVersions(files)
          if (versions.length === 0) throw new Error('no metadata files found')
          return versions[versions.length - 1]
        })
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
 * @param {Resolver} [options.resolver] - Resolves a path to an AsyncBuffer
 * @param {Lister} [options.lister] - Lists files in a directory
 * @returns {Promise<string[]>} A list of iceberg table versions
 */
export function icebergListVersions({ tableUrl, resolver, lister }) {
  resolver ??= urlResolver()
  lister ??= s3Lister()
  const url = `${tableUrl}/metadata/version-hint.text`
  return resolveText(resolver, url)
    .then(text => {
      const version = parseInt(text)
      if (isNaN(version)) throw new Error(`invalid version: ${text}`)
      return Array.from({ length: version }, (_, i) => `v${i + 1}`)
    })
    .catch(() => {
      // version hint not found, try listing metadata dir
      const metadataDir = `${tableUrl}/metadata`
      return lister(metadataDir).then(metadataVersions)
    })
    .catch(err => {
      throw new Error(`failed to determine latest iceberg version: ${err.message}`)
    })
}

/**
 * Fetches the iceberg table metadata.
 * If metadataFileName is not provided, uses icebergLatestVersion to get the version hint.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL of the table (e.g. "s3://my-bucket/path/to/table")
 * @param {string} [options.metadataFileName] - Name of the metadata JSON file
 * @param {Resolver} [options.resolver] - Resolves a path to an AsyncBuffer
 * @param {Lister} [options.lister] - Lists files in a directory
 * @returns {Promise<TableMetadata>} The table metadata as a JSON object
 */
export async function icebergMetadata({ tableUrl, metadataFileName, resolver, lister }) {
  return (await resolveMetadata({ tableUrl, metadataFileName, resolver, lister })).metadata
}

/**
 * Like `icebergMetadata`, but also returns the actual on-disk filename the
 * metadata was resolved from. The write/commit path needs this so it can
 * record the *real* previous metadata filename in `metadata-log` instead of
 * synthesizing an Icebird-style `vN.metadata.json` path that doesn't exist
 * when the prior writer (java/rust/python) used `NNNNN-<uuid>.metadata.json`.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {string} [options.metadataFileName]
 * @param {Resolver} [options.resolver]
 * @param {Lister} [options.lister]
 * @returns {Promise<{ metadata: TableMetadata, metadataFileName: string }>}
 */
export async function resolveMetadata({ tableUrl, metadataFileName, resolver, lister }) {
  resolver ??= urlResolver()
  lister ??= s3Lister()
  if (!metadataFileName) {
    const version = await icebergLatestVersion({ tableUrl, resolver, lister })
    metadataFileName = `${version}.metadata.json`
  }
  const url = `${tableUrl}/metadata/${metadataFileName}`
  try {
    const text = await resolveText(resolver, url)
    return { metadata: JSON.parse(text), metadataFileName }
  } catch (err) {
    // v{N}.metadata.json failed, try listing to find the real filename
    try {
      const metadataDir = `${tableUrl}/metadata`
      const files = await lister(metadataDir)
      const match = findMetadataFile(files, metadataFileName)
      if (match) {
        const text = await resolveText(resolver, `${metadataDir}/${match}`)
        return { metadata: JSON.parse(text), metadataFileName: match }
      }
    } catch { /* lister failed, fall through */ }
    throw new Error(`failed to get iceberg metadata: ${/** @type {Error} */ (err).message}`)
  }
}

/**
 * Find a metadata file matching a version from a list of filenames.
 * Handles both vN.metadata.json and NNNNN-uuid.metadata.json naming conventions.
 *
 * @param {string[]} files
 * @param {string} metadataFileName - e.g. "v1.metadata.json"
 * @returns {string | undefined}
 */
function findMetadataFile(files, metadataFileName) {
  // Direct match
  if (files.includes(metadataFileName)) return metadataFileName
  // Extract version number from vN.metadata.json
  const version = metadataFileVersionNumber(metadataFileName)
  if (version === undefined) return undefined
  const versionNum = String(version).padStart(5, '0')
  const matches = files
    .filter(f => metadataFileVersionNumber(f) === version)
    .sort((a, b) => metadataFilePreference(a, versionNum) - metadataFilePreference(b, versionNum))
  return matches[0]
}

/**
 * Authoritatively discover the highest committed metadata version for a file
 * catalog table on S3. `version-hint.text` is treated as a starting guess only:
 *
 * 1. Try to read `version-hint.text`; if it parses, start from that version.
 * 2. Probe forward (`v<N+1>.metadata.json`, `v<N+2>.metadata.json`, …) until
 *    the next probe is missing. The loop terminates as soon as a gap appears.
 * 3. If the hint is missing or stale (its named version isn't readable), fall
 *    back to listing `metadata/` and picking the largest `vN.metadata.json`.
 *
 * The S3 correctness rule: the highest existing `vN.metadata.json` is the
 * current commit. Lower-numbered versions and `version-hint.text` are stale
 * caches.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {Resolver} [options.resolver]
 * @param {Lister} [options.lister]
 * @param {number} [options.maxProbe] - Hard cap on forward probes; defaults to 64. Above the cap the function falls back to `lister`.
 * @returns {Promise<{ version: number, metadata: TableMetadata, metadataFileName: string, metadataLocation: string }>}
 */
export async function loadLatestFileCatalogMetadata({ tableUrl, resolver, lister, maxProbe = 64 }) {
  resolver ??= urlResolver()
  lister ??= s3Lister()

  /** @type {number | undefined} */
  let hintVersion
  try {
    const text = await resolveText(resolver, `${tableUrl}/metadata/version-hint.text`)
    const parsed = parseInt(text)
    if (!isNaN(parsed)) hintVersion = parsed
  } catch { /* hint missing — that's fine */ }

  if (hintVersion !== undefined && hintVersion >= 0) {
    let lastFound = await tryReadVersion(resolver, tableUrl, hintVersion)
    if (lastFound) {
      let probe = hintVersion + 1
      const limit = hintVersion + maxProbe
      while (probe <= limit) {
        const next = await tryReadVersion(resolver, tableUrl, probe)
        if (!next) break
        lastFound = next
        probe++
      }
      if (probe <= limit) return lastFound
      // hit the cap — fall through to list-based fallback for safety
    }
  }

  // Fallback: list `metadata/` and pick the highest vN we see.
  const files = await lister(`${tableUrl}/metadata`)
  let highest = -1
  /** @type {string | undefined} */
  let highestFile
  for (const file of files) {
    const v = metadataFileVersionNumber(file)
    if (v === undefined) continue
    if (v > highest) {
      highest = v
      highestFile = file
    }
  }
  if (highest < 0 || !highestFile) {
    throw new Error(`no metadata files found at ${tableUrl}/metadata`)
  }
  const metadataLocation = `${tableUrl}/metadata/${highestFile}`
  const text = await resolveText(resolver, metadataLocation)
  return {
    version: highest,
    metadata: JSON.parse(text),
    metadataFileName: highestFile,
    metadataLocation,
  }
}

/**
 * Try reading `vN.metadata.json` for a specific version; returns undefined on
 * any read failure (treated as "version not present"). Probing stops at the
 * first gap, which mirrors how Iceberg writers always create sequentially
 * numbered metadata files.
 *
 * @param {Resolver} resolver
 * @param {string} tableUrl
 * @param {number} version
 * @returns {Promise<{ version: number, metadata: TableMetadata, metadataFileName: string, metadataLocation: string } | undefined>}
 */
async function tryReadVersion(resolver, tableUrl, version) {
  const fileName = `v${version}.metadata.json`
  const metadataLocation = `${tableUrl}/metadata/${fileName}`
  try {
    const text = await resolveText(resolver, metadataLocation)
    return { version, metadata: JSON.parse(text), metadataFileName: fileName, metadataLocation }
  } catch {
    return undefined
  }
}

/**
 * @param {string} file
 * @param {string} paddedVersion
 * @returns {number}
 */
function metadataFilePreference(file, paddedVersion) {
  if (file === `v${Number(paddedVersion)}.metadata.json`) return 0
  if (file === `v${Number(paddedVersion)}.gz.metadata.json`) return 1
  if (file === `v${Number(paddedVersion)}.metadata.json.gz`) return 2
  if (file.startsWith(`${paddedVersion}-`) && file.endsWith('.metadata.json')) return 3
  if (file.startsWith(`${paddedVersion}-`) && file.endsWith('.gz.metadata.json')) return 4
  if (file.startsWith(`${paddedVersion}-`) && file.endsWith('.metadata.json.gz')) return 5
  return 6
}
