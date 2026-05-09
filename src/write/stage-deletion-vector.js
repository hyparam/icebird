import { deleteFileAppliesToDataEntry } from '../delete.js'
import { fetchDeleteMaps, translateS3Url } from '../fetch.js'
import { writeDeletionVector } from '../puffin/deletion-vector.js'
import { writePuffinFile } from '../puffin/puffin.js'
import { validateSchemaForVersion } from '../schema.js'
import { uuid4 } from '../utils.js'
import { writeDeleteManifest, writeExistingDeleteManifest } from './manifest.js'
import {
  buildPartitionSummaries,
  buildSnapshotUpdate,
  currentSnapshot,
  loadPriorManifests,
} from './snapshot.js'
import {
  checkWriteFormat,
  newSnapshotId,
} from './stage.js'
import { findDataFileEntries, loadManifestEntries, partitionTupleKey } from './stage-position-delete.js'

/**
 * @import {DataFile, Manifest, ManifestEntry, PartitionSpec, Resolver, Snapshot, StagedUpdate, TableMetadata} from '../../src/types.js'
 */

/**
 * Stage a v3 row-level delete using puffin deletion vectors. Writes one
 * `.puffin` file per referenced data file (each containing a single
 * `deletion-vector-v1` blob) and a delete manifest with one entry per file.
 * Each manifest entry carries `referenced_data_file`, `content_offset`, and
 * `content_size_in_bytes` per the v3 spec.
 *
 * For partitioned tables, each puffin entry inherits its target data file's
 * partition tuple and partition spec id. Tables that have undergone
 * partition-spec evolution emit one delete manifest per spec touched.
 *
 * v2 tables go through the parquet path; this function rejects
 * format-version != 3.
 *
 * Pass the result to a commit function (`fileCatalogCommit`,
 * `restCatalogUpdateTable`).
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata
 * @param {{file_path: string, pos: bigint|number}[]} options.deletes
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @returns {Promise<StagedUpdate>}
 */
export async function icebergStageDeletionVector({ tableUrl, metadata, deletes, resolver }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (!resolver?.writer) throw new Error('resolver.writer is required')
  if (!Array.isArray(deletes) || !deletes.length) {
    throw new Error('deletes must be a non-empty array')
  }
  const writerFn = resolver.writer
  if (metadata['format-version'] !== 3) {
    throw new Error('icebergStageDeletionVector requires format-version 3')
  }
  const formatVersion = /** @type {3} */ (3)
  const defaultSpec = metadata['partition-specs'].find(s => s['spec-id'] === metadata['default-spec-id'])
  if (!defaultSpec) throw new Error('default partition spec not found in metadata')
  const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('current schema not found in metadata')
  validateSchemaForVersion(schema, formatVersion)

  const snapshotId = newSnapshotId()
  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const manifestUuid = uuid4()
  const timestampMs = Date.now()
  checkWriteFormat(metadata.properties?.['write.format.default'])

  // 1. Group deletes by target data file. Always look up the target's
  //    historical partition tuple/spec id; the current default spec may have
  //    evolved since the target file was written.
  const dataFileMap = await findDataFileEntries(metadata, resolver)
  /** @type {Map<string, { positions: Set<bigint>, partition: Record<string, any>, partitionSpecId: number, dataEntry: ManifestEntry }>} */
  const byFile = new Map()
  for (const d of deletes) {
    if (typeof d.file_path !== 'string' || !d.file_path) {
      throw new Error('deletion vector file_path must be a non-empty string')
    }
    if (d.pos === undefined || d.pos === null) {
      throw new Error('deletion vector pos is required')
    }
    const pos = typeof d.pos === 'bigint' ? d.pos : BigInt(d.pos)
    if (pos < 0n) throw new Error(`deletion vector pos must be non-negative: ${pos}`)
    let entry = byFile.get(d.file_path)
    if (!entry) {
      const found = dataFileMap.get(d.file_path)
      if (!found) throw new Error(`target data file not found in current snapshot: ${d.file_path}`)
      entry = {
        positions: new Set(),
        partition: found.partition,
        partitionSpecId: found.partitionSpecId,
        dataEntry: found.entry,
      }
      byFile.set(d.file_path, entry)
    }
    entry.positions.add(pos)
  }

  const priorManifests = await loadPriorManifests(metadata, resolver)
  const priorDeleteManifests = await loadPriorDeleteManifestEntries(priorManifests, resolver)
  const priorDeleteEntries = priorDeleteManifests.flatMap(m => m.entries)
  const priorPositionDeleteEntries = priorDeleteEntries.filter(entry => entry.data_file.content === 1)
  const targetPaths = new Set(byFile.keys())
  /** @type {Set<ManifestEntry>} */
  const obsoletePositionDeleteEntries = new Set()
  if (priorPositionDeleteEntries.length) {
    const { positionDeletesMap } = await fetchDeleteMaps(priorPositionDeleteEntries, resolver)
    /** @type {Map<ManifestEntry, Set<string>>} */
    const positionDeleteTargets = new Map()
    for (const [targetPath, groups] of positionDeletesMap) {
      for (const group of groups) {
        let targets = positionDeleteTargets.get(group.deleteEntry)
        if (!targets) {
          targets = new Set()
          positionDeleteTargets.set(group.deleteEntry, targets)
        }
        targets.add(targetPath)
      }
    }
    for (const [targetPath, info] of byFile) {
      const groups = positionDeletesMap.get(targetPath) ?? []
      for (const group of groups) {
        if (!deleteFileAppliesToDataEntry(info.dataEntry, group.deleteEntry, metadata, 'position')) continue
        for (const pos of group.positions) info.positions.add(pos)
      }
    }
    for (const [entry, coveredTargets] of positionDeleteTargets) {
      if (isDeletionVectorForTarget(entry, targetPaths)) continue
      let allTargetsCovered = true
      let appliesToTarget = false
      for (const targetPath of coveredTargets) {
        if (!targetPaths.has(targetPath)) {
          allTargetsCovered = false
          break
        }
        const info = byFile.get(targetPath)
        if (info && deleteFileAppliesToDataEntry(info.dataEntry, entry, metadata, 'position')) {
          appliesToTarget = true
        }
      }
      if (allTargetsCovered && appliesToTarget) obsoletePositionDeleteEntries.add(entry)
    }
  }

  /** @type {Set<string>} */
  const skipPriorManifestPaths = new Set()
  /** @type {Manifest[]} */
  const replacementManifests = []
  /** @type {string[]} */
  const replacementManifestPaths = []
  let replacementIndex = 0
  let removedDeleteFiles = 0n
  let removedPositionDeletes = 0n
  let removedDvs = 0n
  for (const { manifest, entries } of priorDeleteManifests) {
    const obsolete = entries.filter(entry => isObsoleteDeleteEntry(entry, targetPaths, obsoletePositionDeleteEntries))
    if (!obsolete.length) continue
    skipPriorManifestPaths.add(manifest.manifest_path)
    removedDeleteFiles += BigInt(obsolete.length)
    removedPositionDeletes += obsolete.reduce((sum, entry) => sum + entry.data_file.record_count, 0n)
    removedDvs += BigInt(obsolete.filter(entry => isDeletionVectorForTarget(entry, targetPaths)).length)

    const retained = entries
      .filter(entry => !isObsoleteDeleteEntry(entry, targetPaths, obsoletePositionDeleteEntries))
      .map(entry => ({ ...entry, status: /** @type {0} */ (0) }))
    if (!retained.length) continue

    const spec = metadata['partition-specs'].find(s => s['spec-id'] === manifest.partition_spec_id)
    if (!spec) throw new Error(`partition spec ${manifest.partition_spec_id} not found in metadata`)
    const manifestPath = `${tableUrl}/metadata/${manifestUuid}-r${replacementIndex}.avro`
    const manifestWriter = writerFn(translateS3Url(manifestPath))
    await writeExistingDeleteManifest({
      writer: manifestWriter,
      schema,
      partitionSpec: spec,
      entries: retained,
      formatVersion,
    })
    const existingRows = retained.reduce((sum, entry) => sum + entry.data_file.record_count, 0n)
    replacementManifests.push({
      manifest_path: manifestPath,
      manifest_length: BigInt(manifestWriter.offset),
      partition_spec_id: spec['spec-id'],
      content: 1,
      sequence_number: sequenceNumber,
      min_sequence_number: minEntrySequenceNumber(retained, sequenceNumber),
      added_snapshot_id: snapshotId,
      added_files_count: 0,
      existing_files_count: retained.length,
      deleted_files_count: 0,
      added_rows_count: 0n,
      existing_rows_count: existingRows,
      deleted_rows_count: 0n,
      partitions: manifest.partitions ?? [],
    })
    replacementManifestPaths.push(manifestPath)
    replacementIndex++
  }

  // 2. Write one puffin file per target with a single DV blob, in parallel.
  /** @type {string[]} */
  const writtenPuffinPaths = []
  const writtenDeleteFiles = await Promise.all([...byFile.entries()]
    .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
    .map(async ([targetPath, info]) => {
      const blob = writeDeletionVector(info.positions)
      const puffin = writePuffinFile({
        blobs: [{
          type: 'deletion-vector-v1',
          fields: [],
          snapshotId: -1,
          sequenceNumber: -1,
          data: blob,
          properties: {
            'referenced-data-file': targetPath,
            cardinality: String(info.positions.size),
          },
        }],
      })
      const puffinPath = `${tableUrl}/data/${uuid4()}-deletes.puffin`
      const puffinWriter = writerFn(translateS3Url(puffinPath))
      puffinWriter.appendBytes(puffin)
      await puffinWriter.finish()
      writtenPuffinPaths.push(puffinPath)
      /** @type {DataFile} */
      const deleteFile = {
        content: 1,
        file_path: puffinPath,
        file_format: 'puffin',
        partition: info.partition,
        record_count: BigInt(info.positions.size),
        file_size_in_bytes: BigInt(puffinWriter.offset),
        referenced_data_file: targetPath,
        content_offset: 4n,
        content_size_in_bytes: BigInt(blob.byteLength),
      }
      return { specId: info.partitionSpecId, partition: info.partition, deleteFile }
    }))

  // 3. Write one delete manifest per partition-spec-id touched.
  /** @type {Map<number, { spec: PartitionSpec, files: DataFile[], partitions: Record<string, any>[] }>} */
  const bySpec = new Map()
  for (const f of writtenDeleteFiles) {
    let bucket = bySpec.get(f.specId)
    if (!bucket) {
      const spec = metadata['partition-specs'].find(s => s['spec-id'] === f.specId)
      if (!spec) throw new Error(`partition spec ${f.specId} not found in metadata`)
      bucket = { spec, files: [], partitions: [] }
      bySpec.set(f.specId, bucket)
    }
    bucket.files.push(f.deleteFile)
    bucket.partitions.push(f.partition)
  }

  /** @type {Manifest[]} */
  const newManifests = []
  /** @type {string[]} */
  const writtenManifestPaths = []
  let manifestIndex = 0
  for (const { spec, files, partitions } of bySpec.values()) {
    const manifestPath = `${tableUrl}/metadata/${manifestUuid}-m${manifestIndex}.avro`
    const manifestWriter = writerFn(translateS3Url(manifestPath))
    await writeDeleteManifest({
      writer: manifestWriter,
      schema,
      partitionSpec: spec,
      snapshotId,
      deleteFiles: files,
      formatVersion,
    })
    const manifestLength = BigInt(manifestWriter.offset)
    const totalAddedRows = files.reduce((sum, f) => sum + f.record_count, 0n)
    newManifests.push({
      manifest_path: manifestPath,
      manifest_length: manifestLength,
      partition_spec_id: spec['spec-id'],
      content: 1,
      sequence_number: sequenceNumber,
      min_sequence_number: sequenceNumber,
      added_snapshot_id: snapshotId,
      added_files_count: files.length,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: totalAddedRows,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
      partitions: spec.fields.length ? buildPartitionSummaries(partitions, schema, spec) : [],
    })
    writtenManifestPaths.push(manifestPath)
    manifestIndex++
  }

  // 4. Distinct partition tuples touched (across specs) for changed-partition-count.
  /** @type {Set<string>} */
  const partitionKeys = new Set()
  for (const f of writtenDeleteFiles) {
    partitionKeys.add(`${f.specId}|${partitionTupleKey(f.partition)}`)
  }

  const totalAddedRows = writtenDeleteFiles.reduce((sum, f) => sum + f.deleteFile.record_count, 0n)
  const totalAddedSize = writtenDeleteFiles.reduce((sum, f) => sum + f.deleteFile.file_size_in_bytes, 0n)

  // 5. Build summary and assemble the StagedUpdate
  const prevSummary = currentSnapshot(metadata)?.summary
  const prevTotals = {
    records: BigInt(prevSummary?.['total-records'] ?? '0'),
    size: BigInt(prevSummary?.['total-files-size'] ?? '0'),
    dataFiles: BigInt(prevSummary?.['total-data-files'] ?? '0'),
    deleteFiles: BigInt(prevSummary?.['total-delete-files'] ?? '0'),
    posDeletes: BigInt(prevSummary?.['total-position-deletes'] ?? '0'),
    eqDeletes: BigInt(prevSummary?.['total-equality-deletes'] ?? '0'),
  }
  /** @type {Snapshot['summary']} */
  const summary = {
    operation: 'delete',
    'added-delete-files': String(writtenDeleteFiles.length),
    'added-dvs': String(writtenDeleteFiles.length),
    'added-position-deletes': String(totalAddedRows),
    'added-files-size': String(totalAddedSize),
    'changed-partition-count': String(partitionKeys.size),
    'total-records': String(prevTotals.records),
    'total-files-size': String(prevTotals.size + totalAddedSize),
    'total-data-files': String(prevTotals.dataFiles),
    'total-delete-files': String(prevTotals.deleteFiles + BigInt(writtenDeleteFiles.length) - removedDeleteFiles),
    'total-position-deletes': String(prevTotals.posDeletes + totalAddedRows - removedPositionDeletes),
    'total-equality-deletes': String(prevTotals.eqDeletes),
  }
  if (removedDeleteFiles > 0n) {
    summary['removed-delete-files'] = String(removedDeleteFiles)
    summary['removed-position-deletes'] = String(removedPositionDeletes)
    if (removedDvs > 0n) summary['removed-dvs'] = String(removedDvs)
  }
  return await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId, sequenceNumber, manifestUuid, timestampMs, formatVersion,
    newManifests: [...replacementManifests, ...newManifests],
    summary,
    writtenFiles: [...replacementManifestPaths, ...writtenPuffinPaths, ...writtenManifestPaths],
    priorManifests,
    skipPriorManifestPaths,
  })
}

/**
 * @param {Manifest[]} manifests
 * @param {Resolver} resolver
 * @returns {Promise<{ manifest: Manifest, entries: ManifestEntry[] }[]>}
 */
async function loadPriorDeleteManifestEntries(manifests, resolver) {
  const groups = await Promise.all(manifests.map(async manifest => {
    if (manifest.content !== 1) return
    const entries = await loadManifestEntries(manifest, resolver)
    return { manifest, entries }
  }))
  return groups.filter(g => g !== undefined)
}

/**
 * @param {ManifestEntry} entry
 * @param {Set<string>} targetPaths
 * @returns {boolean}
 */
function isDeletionVectorForTarget(entry, targetPaths) {
  return entry.data_file.content === 1 &&
    entry.data_file.file_format.toLowerCase() === 'puffin' &&
    entry.data_file.referenced_data_file !== undefined &&
    targetPaths.has(entry.data_file.referenced_data_file)
}

/**
 * @param {ManifestEntry} entry
 * @param {Set<string>} targetPaths
 * @param {Set<ManifestEntry>} obsoletePositionDeleteEntries
 * @returns {boolean}
 */
function isObsoleteDeleteEntry(entry, targetPaths, obsoletePositionDeleteEntries) {
  return isDeletionVectorForTarget(entry, targetPaths) ||
    obsoletePositionDeleteEntries.has(entry)
}

/**
 * @param {ManifestEntry[]} entries
 * @param {bigint} fallback
 * @returns {bigint}
 */
function minEntrySequenceNumber(entries, fallback) {
  let min = fallback
  for (const entry of entries) {
    if (entry.sequence_number !== undefined && entry.sequence_number < min) min = entry.sequence_number
  }
  return min
}
