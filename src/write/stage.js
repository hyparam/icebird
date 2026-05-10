import { translateS3Url } from '../fetch.js'
import { validateSchemaForVersion } from '../schema.js'
import { uuid4 } from '../utils.js'
import { writeParquet } from './parquet.js'
import { writeDataManifest } from './manifest.js'
import { groupByPartition } from './partition.js'
import {
  buildPartitionSummaries,
  buildSnapshotUpdate,
  currentSnapshot,
} from './snapshot.js'
import { computeColumnStats } from './stats.js'

/**
 * @import {CompressionCodec} from 'hyparquet'
 * @import {FieldSummary, Manifest, PreparedAppend, Resolver, Snapshot, StagedUpdate, TableMetadata, TableRequirement, TableUpdate} from '../../src/types.js'
 */

/**
 * Pre-stage an append: write the parquet data files and the data manifest,
 * then return a `PreparedAppend` that `stageSnapshotForAppend` can use to
 * build a snapshot per attempt without re-writing any of these bytes.
 *
 * The spec (v3 §"Manifest Inheritance") is explicit: data files and manifest
 * files are written before sequence numbers are known so optimistic-commit
 * retries don't require rewriting them. This function is the "phase 1" half
 * of that pattern; the per-attempt manifest list / metadata.json work lives
 * in `stageSnapshotForAppend`.
 *
 * Splitting the staging in two keeps the orphan blow-up under contention
 * down to one extra manifest list per failed attempt instead of one extra
 * parquet, manifest, AND manifest list — a meaningful S3 cost and PUT-rate
 * win for high-fan-out append workloads.
 *
 * Only supports v2/v3 tables. Partitioning is supported with identity, void,
 * bucket[N], truncate[W], year, month, day, and hour transforms.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL of the table.
 * @param {TableMetadata} options.metadata - Current table metadata. Used for
 *   schema, partition spec, format version, and write properties; the
 *   sequence number and parent snapshot are read from the freshest metadata
 *   per attempt in `stageSnapshotForAppend`.
 * @param {Record<string, any>[]} options.records - Rows to append.
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @returns {Promise<PreparedAppend>}
 */
export async function prepareAppend({ tableUrl, metadata, records, resolver }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (!resolver?.writer) throw new Error('resolver.writer is required')
  const writerFn = resolver.writer
  if (metadata['format-version'] !== 2 && metadata['format-version'] !== 3) {
    throw new Error(`unsupported format-version: ${metadata['format-version']}`)
  }
  const formatVersion = /** @type {2|3} */ (metadata['format-version'])
  const partitionSpec = metadata['partition-specs'].find(s => s['spec-id'] === metadata['default-spec-id'])
  if (!partitionSpec) throw new Error('default partition spec not found in metadata')
  const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('current schema not found in metadata')
  validateSchemaForVersion(schema, formatVersion)

  // snapshotId is picked once and reused across retries. The metadata.json
  // commit is what makes the snapshot id "claimed"; failed attempts never
  // commit, so reusing the id across attempts cannot corrupt anything. The
  // duplicate-snapshot-id check in applyUpdates (`commit.js`) guards against
  // colliding with a snapshot that was already committed by some other writer.
  const snapshotId = newSnapshotId(metadata)
  const manifestUuid = uuid4()
  checkWriteFormat(metadata.properties?.['write.format.default'])
  const codec = resolveParquetCodec(metadata.properties?.['write.parquet.compression-codec'])

  const groups = partitionSpec.fields.length
    ? groupByPartition(records, schema, partitionSpec)
    : [{ partition: {}, records }]
  const writtenDataFiles = await Promise.all(groups.map(async group => {
    const dataPath = `${tableUrl}/data/${uuid4()}.parquet`
    const dataWriter = writerFn(translateS3Url(dataPath))
    await writeParquet({ writer: dataWriter, schema, records: group.records, codec })
    const stats = computeColumnStats(group.records, schema)
    return {
      partition: group.partition,
      records: group.records,
      dataFile: {
        content: /** @type {0} */ (0),
        file_path: dataPath,
        file_format: /** @type {'parquet'} */ ('parquet'),
        partition: group.partition,
        record_count: BigInt(group.records.length),
        file_size_in_bytes: BigInt(dataWriter.offset),
        value_counts: stats.value_counts,
        null_value_counts: stats.null_value_counts,
        nan_value_counts: stats.nan_value_counts,
        lower_bounds: stats.lower_bounds,
        upper_bounds: stats.upper_bounds,
        sort_order_id: 0,
      },
      path: dataPath,
    }
  }))

  const manifestPath = `${tableUrl}/metadata/${manifestUuid}-m0.avro`
  const manifestWriter = writerFn(translateS3Url(manifestPath))
  await writeDataManifest({
    writer: manifestWriter,
    schema,
    partitionSpec,
    snapshotId,
    dataFiles: writtenDataFiles.map(f => f.dataFile),
    formatVersion,
  })
  const manifestLength = BigInt(manifestWriter.offset)
  const addedRowCount = writtenDataFiles.reduce((sum, f) => sum + BigInt(f.records.length), 0n)
  const addedFilesSize = writtenDataFiles.reduce((sum, f) => sum + f.dataFile.file_size_in_bytes, 0n)
  const partitions = buildPartitionSummaries(
    writtenDataFiles.map(f => f.dataFile.partition),
    schema,
    partitionSpec
  )

  return {
    snapshotId,
    manifestUuid,
    formatVersion,
    manifestPath,
    manifestLength,
    partitionSpecId: partitionSpec['spec-id'],
    partitions,
    addedDataFilesCount: writtenDataFiles.length,
    addedRowCount,
    addedFilesSize,
    recordsCount: records.length,
    writtenFiles: [...writtenDataFiles.map(f => f.path), manifestPath],
  }
}

/**
 * Build the snapshot, manifest list, and `StagedUpdate` for a previously
 * `prepareAppend`'d operation against the freshest table metadata. Safe to
 * call repeatedly inside a retry loop: each call re-derives the sequence
 * number and parent from `metadata`, writes a NEW manifest list (the only
 * file that genuinely depends on which version we're committing under),
 * and returns a fresh `StagedUpdate`. The data files and the manifest from
 * the prepare phase are reused unchanged.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata - The latest loaded metadata. Used
 *   for sequence number, parent snapshot, prior manifests, and totals.
 * @param {PreparedAppend} options.prepared
 * @param {Resolver} options.resolver
 * @returns {Promise<StagedUpdate>}
 */
export async function stageSnapshotForAppend({ tableUrl, metadata, prepared, resolver }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (!resolver?.writer) throw new Error('resolver.writer is required')
  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const timestampMs = Date.now()

  /** @type {Manifest} */
  const newManifest = {
    manifest_path: prepared.manifestPath,
    manifest_length: prepared.manifestLength,
    partition_spec_id: prepared.partitionSpecId,
    content: 0,
    sequence_number: sequenceNumber,
    min_sequence_number: sequenceNumber,
    added_snapshot_id: prepared.snapshotId,
    added_files_count: prepared.addedDataFilesCount,
    existing_files_count: 0,
    deleted_files_count: 0,
    added_rows_count: prepared.addedRowCount,
    existing_rows_count: 0n,
    deleted_rows_count: 0n,
    partitions: prepared.partitions,
  }

  const prevSummary = currentSnapshot(metadata)?.summary
  const prevTotals = {
    records: BigInt(prevSummary?.['total-records'] ?? '0'),
    size: BigInt(prevSummary?.['total-files-size'] ?? '0'),
    files: BigInt(prevSummary?.['total-data-files'] ?? '0'),
  }
  /** @type {Snapshot['summary']} */
  const summary = {
    operation: 'append',
    'added-data-files': String(prepared.addedDataFilesCount),
    'added-records': String(prepared.recordsCount),
    'added-files-size': String(prepared.addedFilesSize),
    'changed-partition-count': String(prepared.addedDataFilesCount),
    'total-records': String(prevTotals.records + BigInt(prepared.recordsCount)),
    'total-files-size': String(prevTotals.size + prepared.addedFilesSize),
    'total-data-files': String(prevTotals.files + BigInt(prepared.addedDataFilesCount)),
    'total-delete-files': '0',
    'total-position-deletes': '0',
    'total-equality-deletes': '0',
  }
  return await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId: prepared.snapshotId,
    sequenceNumber,
    manifestUuid: prepared.manifestUuid,
    timestampMs,
    formatVersion: prepared.formatVersion,
    newManifests: [newManifest],
    summary,
    // Already accounted for by prepareAppend's writtenFiles. Only the new
    // manifest list (added inside buildSnapshotUpdate) is added here.
    writtenFiles: [],
  })
}

/**
 * Bundled prepare + stage. Equivalent to the prior single-call API; left in
 * place so callers that don't run inside a retry loop (e.g. `icebergTransaction`)
 * can use one call. Inside `commitWithRetry`, prefer `prepareAppend` outside
 * the loop and `stageSnapshotForAppend` inside, so retries don't re-write
 * data and manifest files.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata
 * @param {Record<string, any>[]} options.records
 * @param {Resolver} options.resolver
 * @returns {Promise<StagedUpdate>}
 */
export async function icebergStageAppend({ tableUrl, metadata, records, resolver }) {
  const prepared = await prepareAppend({ tableUrl, metadata, records, resolver })
  const staged = await stageSnapshotForAppend({ tableUrl, metadata, prepared, resolver })
  // Surface the prepare-phase writes alongside the manifest list so callers
  // can clean up everything on commit failure.
  return { ...staged, writtenFiles: [...prepared.writtenFiles, ...staged.writtenFiles] }
}

/**
 * Stage a `set-snapshot-ref` update to point a branch or tag at an existing
 * snapshot. The same primitive powers rollback (set `main` to a prior
 * snapshot), tagging, fast-forwarding feature branches, and adjusting ref
 * retention.
 *
 * Pure: produces a `StagedUpdate` to pass into a commit function
 * (`fileCatalogCommit`, `restCatalogUpdateTable`). The returned `snapshot` is
 * the target snapshot the ref will point at after commit.
 *
 * @param {object} options
 * @param {TableMetadata} options.metadata - Current table metadata.
 * @param {string} options.ref - Ref name (e.g. 'main', a tag name, a branch name).
 * @param {number} options.snapshotId - Target snapshot id; must already exist in `metadata.snapshots`.
 * @param {'branch'|'tag'} [options.type] - Defaults to 'branch'.
 * @param {number} [options.minSnapshotsToKeep] - Branch retention; rejected for tags.
 * @param {number} [options.maxSnapshotAgeMs] - Branch retention; rejected for tags.
 * @param {number} [options.maxRefAgeMs]
 * @returns {StagedUpdate}
 */
export function icebergStageSetRef({ metadata, ref, snapshotId, type = 'branch', minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs }) {
  if (!ref) throw new Error('ref is required')
  if (type !== 'branch' && type !== 'tag') throw new Error(`unknown ref type: ${type}`)
  if (type === 'tag' && (minSnapshotsToKeep !== undefined || maxSnapshotAgeMs !== undefined)) {
    throw new Error('tags do not support min-snapshots-to-keep or max-snapshot-age-ms')
  }
  const snapshot = metadata.snapshots?.find(s => s['snapshot-id'] === snapshotId)
  if (!snapshot) throw new Error(`snapshot ${snapshotId} not found in metadata.snapshots`)

  // Existing ref value for the CAS check. Legacy tables may have set
  // current-snapshot-id without a populated refs.main, so fall back to it.
  const existingRef = metadata.refs?.[ref]
  let currentSnapshotId = existingRef?.['snapshot-id'] ?? null
  if (currentSnapshotId === null && ref === 'main') {
    currentSnapshotId = metadata['current-snapshot-id'] ?? null
  }
  if (existingRef && existingRef.type !== type) {
    throw new Error(`ref ${ref} is a ${existingRef.type}, cannot set as ${type}`)
  }

  /** @type {TableRequirement[]} */
  const requirements = [
    { type: 'assert-table-uuid', uuid: metadata['table-uuid'] },
    { type: 'assert-ref-snapshot-id', ref, 'snapshot-id': currentSnapshotId },
  ]

  /** @type {TableUpdate} */
  const update = {
    action: 'set-snapshot-ref',
    'ref-name': ref,
    type,
    'snapshot-id': snapshotId,
  }
  if (minSnapshotsToKeep !== undefined) update['min-snapshots-to-keep'] = minSnapshotsToKeep
  if (maxSnapshotAgeMs !== undefined) update['max-snapshot-age-ms'] = maxSnapshotAgeMs
  if (maxRefAgeMs !== undefined) update['max-ref-age-ms'] = maxRefAgeMs

  return {
    snapshot,
    requirements,
    updates: [update],
    writtenFiles: [],
  }
}

/**
 * Stage a `remove-snapshots` update to expire one or more snapshots from the
 * table. Pure: produces a `StagedUpdate` to pass into a commit function.
 * Validates that every id exists, that none are referenced by a branch or
 * tag, and that the legacy `current-snapshot-id` is not in the removal list.
 *
 * Data files belonging to expired snapshots are not deleted from storage
 * here. That is the responsibility of a separate maintenance pass that can
 * compute reachability across the surviving snapshots.
 *
 * @param {object} options
 * @param {TableMetadata} options.metadata - Current table metadata.
 * @param {number[]} options.snapshotIds - Snapshot ids to expire.
 * @returns {StagedUpdate}
 */
export function icebergStageExpireSnapshots({ metadata, snapshotIds }) {
  if (!Array.isArray(snapshotIds) || snapshotIds.length === 0) {
    throw new Error('snapshotIds must be a non-empty array')
  }
  const removeIds = new Set(snapshotIds)
  const snapshots = metadata.snapshots ?? []
  for (const id of removeIds) {
    if (!snapshots.some(s => s['snapshot-id'] === id)) {
      throw new Error(`snapshot ${id} not found in metadata.snapshots`)
    }
  }

  const refs = metadata.refs ?? {}
  for (const [name, ref] of Object.entries(refs)) {
    if (removeIds.has(ref['snapshot-id'])) {
      throw new Error(`snapshot ${ref['snapshot-id']} is referenced by ${ref.type} ${name}`)
    }
  }
  // Legacy tables may carry current-snapshot-id without a populated refs.main.
  const currentId = metadata['current-snapshot-id']
  if (currentId !== undefined && currentId !== null && removeIds.has(currentId) && !refs.main) {
    throw new Error(`snapshot ${currentId} is the current snapshot`)
  }

  /** @type {TableRequirement[]} */
  const requirements = [
    { type: 'assert-table-uuid', uuid: metadata['table-uuid'] },
    { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': refs.main?.['snapshot-id'] ?? currentId ?? null },
  ]

  /** @type {TableUpdate} */
  const update = { action: 'remove-snapshots', 'snapshot-ids': [...removeIds] }

  // The snapshot field on StagedUpdate is non-optional; surface the current
  // snapshot so callers reading `staged.snapshot` after an expire still see
  // the live tip. Synthesize a minimal placeholder for tables with no
  // snapshots left after the operation.
  const tip = currentSnapshot(metadata) ?? snapshots[0]
  if (!tip) throw new Error('cannot expire snapshots from a table with no snapshots')

  return {
    snapshot: tip,
    requirements,
    updates: [update],
    writtenFiles: [],
  }
}

/**
 * Reject `write.format.default` values other than parquet. Iceberg also
 * defines `avro` and `orc`, but Icebird only writes parquet today.
 *
 * @param {string|undefined} value
 */
export function checkWriteFormat(value) {
  if (value === undefined) return
  if (value.toLowerCase() !== 'parquet') {
    throw new Error(`unsupported write.format.default: ${value}`)
  }
}

/**
 * Map an Iceberg `write.parquet.compression-codec` property to the
 * hyparquet-writer codec. Only `snappy` (the default) and
 * `none`/`uncompressed` are supported today, since hyparquet-writer
 * ships no other compressors.
 *
 * @param {string|undefined} value
 * @returns {CompressionCodec|undefined}
 */
export function resolveParquetCodec(value) {
  if (value === undefined) return undefined
  switch (value.toLowerCase()) {
  case 'snappy': return 'SNAPPY'
  case 'none':
  case 'uncompressed': return 'UNCOMPRESSED'
  default:
    throw new Error(`unsupported write.parquet.compression-codec: ${value}`)
  }
}

/**
 * Generate a positive snapshot id that fits in a JS Number. When `metadata`
 * is supplied, re-roll until the id is not already present in
 * `metadata.snapshots`. The 53-bit space gives a non-trivial birthday
 * collision rate at logging scale, and a duplicate id silently corrupts
 * ref resolution and read planning.
 *
 * @param {TableMetadata} [metadata] - Current table metadata; ids in `snapshots` are skipped.
 * @returns {bigint}
 */
export function newSnapshotId(metadata) {
  const used = new Set((metadata?.snapshots ?? []).map(s => BigInt(s['snapshot-id'])))
  const arr = new BigInt64Array(1)
  // Re-roll on collision. The cap prevents an infinite loop when the RNG is
  // mocked to a constant (tests) or wedged: falling through to the throw
  // makes the failure loud rather than silent.
  for (let attempt = 0; attempt < 32; attempt++) {
    globalThis.crypto.getRandomValues(arr)
    const masked = arr[0] & 0x1fffffffffffffn // 53 bits
    const id = masked === 0n ? 1n : masked
    if (!used.has(id)) return id
  }
  throw new Error('newSnapshotId: failed to find an unused id after 32 attempts')
}
