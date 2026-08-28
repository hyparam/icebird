import { avroMetadata } from './avro/avro.metadata.js'
import { avroRead } from './avro/avro.read.js'
import { fetchAvroRecords, urlResolver } from './fetch.js'

/**
 * Returns manifest entries for a snapshot. Defaults to the current snapshot;
 * pass `snapshotId` to time-travel to a prior snapshot in the metadata's
 * snapshot log.
 *
 * @import {Resolver, TableMetadata, Manifest, ManifestEntry, Snapshot} from '../src/types.js'
 * @typedef {{ url: string, entries: ManifestEntry[] }[]} ManifestList
 * @param {object} options
 * @param {TableMetadata} options.metadata
 * @param {Resolver} [options.resolver]
 * @param {number | bigint} [options.snapshotId] - Optional snapshot id; defaults to `current-snapshot-id`.
 * @returns {Promise<ManifestList>}
 */
export async function icebergManifests({ metadata, resolver, snapshotId }) {
  resolver ??= urlResolver()
  const rawTarget = snapshotId ?? metadata['current-snapshot-id']
  if (rawTarget == null || rawTarget < 0) {
    throw new Error('No current snapshot id found in table metadata')
  }
  // Snapshot ids can be either number (small) or BigInt (>2^53 from the
  // lossless metadata parser, or supplied by the caller). Normalize to
  // BigInt for the lookup so user-passed numbers match bigint metadata ids
  // and vice versa.
  const targetId = BigInt(rawTarget)
  const snapshot = metadata.snapshots?.find(s => BigInt(s['snapshot-id']) === targetId)
  if (!snapshot) {
    throw new Error(`Snapshot ${rawTarget} not found in metadata`)
  }

  // Get manifest URLs from snapshot
  let manifests = []
  if (snapshot['manifest-list']) {
    // Fetch manifest list and extract manifest URLs
    const manifestListUrl = snapshot['manifest-list']
    manifests = /** @type {Manifest[]} */ (await fetchAvroRecords(manifestListUrl, resolver))
  } else if (snapshot.manifests) {
    // v1 snapshots list manifests inline instead of pointing at a manifest list
    manifests = await resolveInlineManifests(snapshot, resolver)
  } else {
    throw new Error('No manifest information found in snapshot')
  }

  return await fetchManifests(manifests, resolver)
}

/**
 * Turn a v1 snapshot's inline `manifests` (a list of manifest file locations)
 * into manifest list records. Each manifest is read once to recover the
 * length, spec id, and file counts a manifest list needs, so a table upgraded
 * from v1 can still be read and appended to without rewriting its inherited
 * metadata.
 *
 * @param {Snapshot} snapshot
 * @param {Resolver} resolver
 * @returns {Promise<Manifest[]>}
 */
export async function resolveInlineManifests(snapshot, resolver) {
  const inline = snapshot.manifests ?? []
  return await Promise.all(inline.map(async manifestPath => {
    const ab = await resolver.reader(manifestPath)
    const buffer = await ab.slice(0, ab.byteLength)
    const reader = { view: new DataView(buffer), offset: 0 }
    const { metadata, syncMarker } = await avroMetadata(reader)
    const entries = /** @type {ManifestEntry[]} */ (await avroRead({ reader, metadata, syncMarker }))
    const counts = [0, 0, 0]
    const rows = [0n, 0n, 0n]
    for (const entry of entries) {
      counts[entry.status]++
      rows[entry.status] += BigInt(entry.data_file.record_count)
    }
    return {
      manifest_path: manifestPath,
      manifest_length: BigInt(ab.byteLength),
      partition_spec_id: Number(metadata['partition-spec-id'] ?? 0),
      content: 0,
      added_snapshot_id: BigInt(snapshot['snapshot-id']),
      added_files_count: counts[1],
      existing_files_count: counts[0],
      deleted_files_count: counts[2],
      added_rows_count: rows[1],
      existing_rows_count: rows[0],
      deleted_rows_count: rows[2],
    }
  }))
}

/**
 * Fetch manifest entries from a list of manifests in parallel.
 *
 * @param {Manifest[]} manifests
 * @param {Resolver} resolver
 * @returns {Promise<ManifestList>}
 */
async function fetchManifests(manifests, resolver) {
  // Fetch manifest entries in parallel
  return await Promise.all(manifests.map(async manifest => {
    const url = manifest.manifest_path
    const entries = /** @type {ManifestEntry[]} */ (
      await fetchAvroRecords(url, resolver, Number(manifest.manifest_length))
    )

    // Inherit sequence number from manifest if not present in entry
    for (const entry of entries) {
      entry.partition_spec_id = manifest.partition_spec_id ?? 0
      if (entry.snapshot_id == null) entry.snapshot_id = manifest.added_snapshot_id

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
    assignFirstRowIds(manifest, entries)

    return { url, entries }
  }))
}

/**
 * Apply v3 first-row-id inheritance from the manifest list into data file
 * entries. Delete files never inherit row IDs.
 *
 * @param {Manifest} manifest
 * @param {ManifestEntry[]} entries
 */
function assignFirstRowIds(manifest, entries) {
  if (manifest.content !== 0 || manifest.first_row_id == null) return

  let nextFirstRowId = BigInt(manifest.first_row_id)
  for (const entry of entries) {
    const dataFile = entry.data_file
    if (dataFile.content !== 0) continue
    if (dataFile.first_row_id == null) {
      dataFile.first_row_id = nextFirstRowId
      nextFirstRowId += BigInt(dataFile.record_count)
    }
  }
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
