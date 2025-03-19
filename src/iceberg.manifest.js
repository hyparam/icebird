import { fetchAvroRecords } from './iceberg.fetch.js'

/**
 * Returns manifest entries for the current snapshot.
 *
 * @import {IcebergMetadata, Manifest, ManifestEntry} from './types.js'
 * @typedef {{ url: string, entries: ManifestEntry[] }[]} ManifestList
 * @param {IcebergMetadata} metadata
 * @returns {Promise<ManifestList>}
 */
export async function icebergManifests(metadata) {
  const currentSnapshotId = metadata['current-snapshot-id']
  if (!currentSnapshotId || currentSnapshotId < 0) {
    throw new Error('No current snapshot id found in table metadata')
  }
  const snapshot = metadata.snapshots.find(s => s['snapshot-id'] === currentSnapshotId)
  if (!snapshot) {
    throw new Error(`Snapshot ${currentSnapshotId} not found in metadata`)
  }

  // Get manifest URLs from snapshot
  let manifestUrls = []
  if (snapshot['manifest-list']) {
    // Fetch manifest list and extract manifest URLs
    const manifestListUrl = snapshot['manifest-list']
    const records = /** @type {Manifest[]} */ (await fetchAvroRecords(manifestListUrl))
    manifestUrls = records.map(rec => rec.manifest_path)
  } else if (snapshot.manifests) {
    manifestUrls = snapshot.manifests.map(m => m.manifest_path)
  } else {
    throw new Error('No manifest information found in snapshot')
  }

  return await fetchManifests(manifestUrls)
}

/**
 * Fetch manifest entries from a list of manifest URLs in parallel.
 *
 * @param {string[]} manifestUrls - list of manifest URLs
 * @returns {Promise<ManifestList>}
 */
async function fetchManifests(manifestUrls) {
  // Fetch manifest entries in parallel
  return await Promise.all(manifestUrls.map(async url => {
    const entries = /** @type {ManifestEntry[]} */ (await fetchAvroRecords(url))
    return { url, entries }
  }))
}

/**
 * @import {DataFile} from './types.js'
 * @param {string[]} dataManifestUrls
 * @returns {Promise<DataFile[]>}
 */
export async function fetchDataFilesFromManifests(dataManifestUrls) {
  const manifests = await fetchManifests(dataManifestUrls)
  /** @type {DataFile[]} */
  const dataFiles = []
  for (const { entries } of manifests) {
    for (const entry of entries) {
      dataFiles.push(entry.data_file)
    }
  }
  return dataFiles
}

/**
 * Returns manifest URLs for the current snapshot separated into data and delete manifests.
 *
 * @param {ManifestList} manifests
 * @returns {{dataManifestUrls: string[], deleteManifestUrls: string[]}}
 */
export function getDataUrls(manifests) {
  // Separate manifest entries into data and delete urls
  const dataManifestUrls = []
  const deleteManifestUrls = []
  for (const { url, entries } of manifests) {
    for (const entry of entries) {
      if (entry.data_file.content) {
        deleteManifestUrls.push(url)
      } else {
        dataManifestUrls.push(url)
      }
    }
  }

  return { dataManifestUrls, deleteManifestUrls }
}
