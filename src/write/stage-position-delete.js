import { fetchAvroRecords, translateS3Url } from '../fetch.js'
import { validateSchemaForVersion } from '../schema.js'
import { bytesToHex, uuid4 } from '../utils.js'
import { writePositionDeleteFile } from './delete-file.js'
import { writeDeleteManifest } from './manifest.js'
import {
  buildPartitionSummaries,
  buildSnapshotUpdate,
  currentSnapshot,
} from './snapshot.js'
import {
  checkWriteFormat,
  newSnapshotId,
  resolveParquetCodec,
} from './stage.js'

/**
 * @import {DataFile, Manifest, ManifestEntry, PartitionSpec, Resolver, Snapshot, StagedUpdate, TableMetadata} from '../../src/types.js'
 */

/**
 * Stage a row-level delete that writes a v2 parquet position-delete file,
 * the delete manifest covering it, and a new snapshot whose operation is
 * `delete`. Prior manifests are carried forward so existing data and delete
 * files remain visible.
 *
 * For partitioned tables, each delete's target data file is looked up in the
 * current snapshot's data manifests, deletes are grouped by `(spec-id,
 * partition tuple)`, and one delete file is written per group. Delete files
 * inherit each target's partition spec id, so tables that have undergone
 * partition spec evolution emit one delete manifest per spec touched.
 *
 * Pass the result to a commit function (`fileCatalogCommit`,
 * `restCatalogUpdateTable`).
 *
 * NOTE: under `commitWithRetry`, the WHOLE bundle (parquet + manifest +
 * manifest list) is re-staged on every conflict. Unlike append, the delete
 * path's "prepare" phase has correctness dependencies on the freshest
 * metadata (target-file lookup, partition-spec inheritance), so a clean
 * prepare/stage split needs more care than just hoisting writes outside
 * the loop. Heavy concurrent delete workloads will see the same O(N²)
 * orphan blow-up that append used to have. Tracked as a follow-up.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata
 * @param {{file_path: string, pos: bigint|number}[]} options.deletes
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @returns {Promise<StagedUpdate>}
 */
export async function icebergStagePositionDelete({ tableUrl, metadata, deletes, resolver }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (!resolver?.writer) throw new Error('resolver.writer is required')
  if (!Array.isArray(deletes) || !deletes.length) {
    throw new Error('deletes must be a non-empty array')
  }
  const writerFn = resolver.writer
  if (metadata['format-version'] !== 2 && metadata['format-version'] !== 3) {
    throw new Error(`unsupported format-version: ${metadata['format-version']}`)
  }
  if (metadata['format-version'] === 3) {
    throw new Error('format-version 3 tables must use deletion vectors, not new position delete files')
  }
  const formatVersion = /** @type {2|3} */ (metadata['format-version'])
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
  const codec = resolveParquetCodec(metadata.properties?.['write.parquet.compression-codec'])

  // 1. Group deletes by the target file's historical (spec-id, partition).
  const partitionMap = await findDataFilePartitions(metadata, resolver)
  /** @type {Map<string, { specId: number, partition: Record<string, any>, deletes: typeof deletes }>} */
  const groups = new Map()
  for (const d of deletes) {
    const found = partitionMap.get(d.file_path)
    if (!found) throw new Error(`target data file not found in current snapshot: ${d.file_path}`)
    const { partition, partitionSpecId: specId } = found
    const key = `${specId}|${partitionTupleKey(partition)}`
    let group = groups.get(key)
    if (!group) {
      group = { specId, partition, deletes: [] }
      groups.set(key, group)
    }
    group.deletes.push(d)
  }

  // 2. Write one parquet position-delete file per group, in parallel.
  const writtenDeleteFiles = await Promise.all([...groups.values()].map(async group => {
    const uniqueTargets = new Set(group.deletes.map(d => d.file_path))
    const referencedDataFile = uniqueTargets.size === 1 ? group.deletes[0].file_path : undefined
    const deletePath = `${tableUrl}/data/${uuid4()}-deletes.parquet`
    const deleteWriter = writerFn(translateS3Url(deletePath))
    const stats = await writePositionDeleteFile({ writer: deleteWriter, deletes: group.deletes, codec })
    /** @type {DataFile} */
    const deleteFile = {
      content: 1,
      file_path: deletePath,
      file_format: 'parquet',
      partition: group.partition,
      record_count: stats.record_count,
      file_size_in_bytes: BigInt(deleteWriter.offset),
      value_counts: stats.value_counts,
      null_value_counts: stats.null_value_counts,
      lower_bounds: stats.lower_bounds,
      upper_bounds: stats.upper_bounds,
      sort_order_id: 0,
      referenced_data_file: referencedDataFile,
    }
    return { specId: group.specId, partition: group.partition, deleteFile, path: deletePath }
  }))

  // 3. Write one delete manifest per partition-spec-id touched. Most tables
  //    only ever touch one spec; spec-evolved tables may need several.
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

  // 4. Build summary and assemble the StagedUpdate. Data totals carry forward;
  //    delete totals bump.
  const prevSummary = currentSnapshot(metadata)?.summary
  const prevTotals = {
    records: BigInt(prevSummary?.['total-records'] ?? '0'),
    size: BigInt(prevSummary?.['total-files-size'] ?? '0'),
    dataFiles: BigInt(prevSummary?.['total-data-files'] ?? '0'),
    deleteFiles: BigInt(prevSummary?.['total-delete-files'] ?? '0'),
    posDeletes: BigInt(prevSummary?.['total-position-deletes'] ?? '0'),
    eqDeletes: BigInt(prevSummary?.['total-equality-deletes'] ?? '0'),
  }
  const addedSize = writtenDeleteFiles.reduce((sum, f) => sum + f.deleteFile.file_size_in_bytes, 0n)
  const addedPosDeletes = writtenDeleteFiles.reduce((sum, f) => sum + f.deleteFile.record_count, 0n)
  /** @type {Snapshot['summary']} */
  const summary = {
    operation: 'delete',
    'added-delete-files': String(writtenDeleteFiles.length),
    'added-position-deletes': String(addedPosDeletes),
    'added-files-size': String(addedSize),
    'changed-partition-count': String(groups.size),
    'total-records': String(prevTotals.records),
    'total-files-size': String(prevTotals.size + addedSize),
    'total-data-files': String(prevTotals.dataFiles),
    'total-delete-files': String(prevTotals.deleteFiles + BigInt(writtenDeleteFiles.length)),
    'total-position-deletes': String(prevTotals.posDeletes + addedPosDeletes),
    'total-equality-deletes': String(prevTotals.eqDeletes),
  }
  return await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId, sequenceNumber, manifestUuid, timestampMs, formatVersion,
    newManifests,
    summary,
    writtenFiles: [...writtenDeleteFiles.map(f => f.path), ...writtenManifestPaths],
  })
}

/**
 * Fetch manifest entries and apply the same inheritance rules used by reads.
 *
 * @param {Manifest} manifest
 * @param {Resolver} resolver
 * @returns {Promise<ManifestEntry[]>}
 */
export async function loadManifestEntries(manifest, resolver) {
  const entries = /** @type {ManifestEntry[]} */ (await fetchAvroRecords(manifest.manifest_path, resolver))
  for (const entry of entries) {
    entry.partition_spec_id = manifest.partition_spec_id ?? 0
    if (entry.sequence_number === undefined) {
      entry.sequence_number = manifest.sequence_number ?? 0n
    }
    if (entry.status === 1 && entry.file_sequence_number === undefined) {
      entry.file_sequence_number = manifest.sequence_number ?? 0n
    }
  }
  return entries
}

/**
 * Read every data manifest entry reachable from the current snapshot and
 * index it by `data_file.file_path`. Used by delete stagers to look up each
 * target data file's partition tuple and partition spec id, which a delete
 * manifest entry must inherit.
 *
 * Status === 2 (logically deleted) entries are skipped; the read scanner
 * skips them too, so deletes against them would have no effect.
 *
 * @param {TableMetadata} metadata
 * @param {Resolver} resolver
 * @returns {Promise<Map<string, { partition: Record<string, any>, partitionSpecId: number }>>}
 */
async function findDataFilePartitions(metadata, resolver) {
  const entries = await findDataFileEntries(metadata, resolver)
  /** @type {Map<string, { partition: Record<string, any>, partitionSpecId: number }>} */
  const out = new Map()
  for (const [path, found] of entries) {
    out.set(path, { partition: found.partition, partitionSpecId: found.partitionSpecId })
  }
  return out
}

/**
 * Read every data manifest entry reachable from the current snapshot and
 * index it by `data_file.file_path`, preserving sequence and partition
 * metadata for delete planning.
 *
 * @param {TableMetadata} metadata
 * @param {Resolver} resolver
 * @returns {Promise<Map<string, { partition: Record<string, any>, partitionSpecId: number, entry: ManifestEntry }>>}
 */
export async function findDataFileEntries(metadata, resolver) {
  const snap = currentSnapshot(metadata)
  if (!snap?.['manifest-list']) return new Map()
  const manifests = /** @type {Manifest[]} */ (await fetchAvroRecords(snap['manifest-list'], resolver))
  /** @type {Map<string, { partition: Record<string, any>, partitionSpecId: number, entry: ManifestEntry }>} */
  const out = new Map()
  await Promise.all(manifests.map(async manifest => {
    if (manifest.content !== 0) return
    const entries = await loadManifestEntries(manifest, resolver)
    for (const entry of entries) {
      if (entry.status === 2) continue
      const f = entry.data_file
      if (f.content !== 0) continue
      out.set(f.file_path, {
        partition: /** @type {Record<string, any>} */ (f.partition ?? {}),
        partitionSpecId: manifest.partition_spec_id ?? 0,
        entry,
      })
    }
  }))
  return out
}

/**
 * Stable string key for a partition tuple. Spec field order is fixed by the
 * caller passing pre-sorted keys; we serialize each value with a type tag so
 * `1` (int) and `1n` (long) hash to different keys.
 *
 * @param {Record<string, any>} partition
 * @returns {string}
 */
export function partitionTupleKey(partition) {
  const keys = Object.keys(partition).sort()
  return keys.map(k => `${k}=${valueTag(partition[k])}`).join(',')
}

/**
 * @param {any} v
 * @returns {string}
 */
function valueTag(v) {
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'bigint') return `b:${v.toString()}`
  if (v instanceof Date) return `d:${v.getTime()}`
  if (v instanceof Uint8Array) return `x:${bytesToHex(v)}`
  return `${typeof v}:${String(v)}`
}
