import { fetchAvroRecords } from './iceberg.fetch.js'

/**
 * Returns manifest entries for the current snapshot.
 *
 * @import {TableMetadata, Manifest, ManifestEntry} from '../src/types.js'
 * @typedef {{ url: string, entries: ManifestEntry[] }[]} ManifestList
 * @param {TableMetadata} metadata
 * @param {RequestInit} [requestInit]
 * @returns {Promise<ManifestList>}
 */
export async function icebergManifests(metadata, requestInit) {
  const currentSnapshotId = metadata['current-snapshot-id']
  if (!currentSnapshotId || currentSnapshotId < 0) {
    throw new Error('No current snapshot id found in table metadata')
  }
  const snapshot = metadata.snapshots?.find(s => s['snapshot-id'] === currentSnapshotId)
  if (!snapshot) {
    throw new Error(`Snapshot ${currentSnapshotId} not found in metadata`)
  }

  // Get manifest URLs from snapshot
  let manifests = []
  if (snapshot['manifest-list']) {
    // Fetch manifest list and extract manifest URLs
    const manifestListUrl = snapshot['manifest-list']
    manifests = /** @type {Manifest[]} */ (await fetchAvroRecords(manifestListUrl, requestInit))
  } else if (snapshot.manifests) {
    // Use manifest URLs directly from snapshot
    manifests = snapshot.manifests
  } else {
    throw new Error('No manifest information found in snapshot')
  }

  return await fetchManifests(manifests)
}

/**
 * Fetch manifest entries from a list of manifests in parallel.
 *
 * @param {Manifest[]} manifests
 * @param {RequestInit} [requestInit]
 * @returns {Promise<ManifestList>}
 */
async function fetchManifests(manifests, requestInit) {
  // Fetch manifest entries in parallel
  return await Promise.all(manifests.map(async manifest => {
    const url = manifest.manifest_path
    const entries = /** @type {ManifestEntry[]} */ (await fetchAvroRecords(url, requestInit))

    // Inherit sequence number from manifest if not present in entry
    for (const entry of entries) {
      if (entry.sequence_number === undefined) {
        // When reading v1 manifests with no sequence number column,
        // sequence numbers for all files must default to 0.
        entry.sequence_number = manifest.sequence_number ?? 0n
      }

      if (entry.status === 1) {
        // only ADDED can inherit sequence number
        if (entry.sequence_number === undefined) {
          entry.sequence_number = manifest.sequence_number
        }
        if (entry.file_sequence_number === undefined) {
          entry.file_sequence_number = manifest.sequence_number
        }
      } else {
        if (entry.sequence_number === undefined || entry.file_sequence_number === undefined) {
          // spec violation "Sequence‑number inheritance"
          throw new Error('iceberg manifest entry missing sequence number')
        }
      }
    }

    return { url, entries }
  }))
}

/**
 * Split manifest entries into data and delete manifests.
 *
 * @param {ManifestList} manifests
 * @returns {{dataEntries: ManifestEntry[], deleteEntries: ManifestEntry[]}}
 */
export function splitManifestEntries(manifests) {
  const dataEntries = []
  const deleteEntries = []
  for (const { entries } of manifests) {
    for (const entry of entries) {
      if (entry.status === 2) continue // skip logically deleted
      if (entry.data_file.content) {
        deleteEntries.push(entry)
      } else {
        dataEntries.push(entry)
      }
    }
  }

  return { dataEntries, deleteEntries }
}
