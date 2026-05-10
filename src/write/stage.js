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
 * @import {Manifest, Resolver, Snapshot, StagedUpdate, TableMetadata, TableRequirement, TableUpdate} from '../../src/types.js'
 */

/**
 * Build the data files, manifest, and manifest list for an append, then
 * return the catalog-agnostic `StagedUpdate` payload (snapshot plus the
 * requirements/updates a catalog must apply to commit it).
 *
 * No metadata.json or version-hint is written. Pass the result to a commit
 * function (`fileCatalogCommit`, or a future `restCatalogCommitTable`).
 *
 * Only supports v2/v3 tables. Partitioning is supported with identity, void,
 * bucket[N], truncate[W], year, month, day, and hour transforms.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL of the table.
 * @param {TableMetadata} options.metadata - Current table metadata.
 * @param {Record<string, any>[]} options.records - Rows to append.
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @returns {Promise<StagedUpdate>}
 */
export async function icebergStageAppend({ tableUrl, metadata, records, resolver }) {
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

  const snapshotId = newSnapshotId(metadata)
  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const manifestUuid = uuid4()
  const timestampMs = Date.now()
  checkWriteFormat(metadata.properties?.['write.format.default'])
  const codec = resolveParquetCodec(metadata.properties?.['write.parquet.compression-codec'])

  // 1. Group records by partition tuple, then write one parquet per group in parallel.
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

  // 2. Write a single manifest covering every new data file
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
  const totalAddedRows = writtenDataFiles.reduce((sum, f) => sum + BigInt(f.records.length), 0n)
  const totalAddedSize = writtenDataFiles.reduce((sum, f) => sum + f.dataFile.file_size_in_bytes, 0n)

  const partitions = buildPartitionSummaries(
    writtenDataFiles.map(f => f.dataFile.partition),
    schema,
    partitionSpec
  )

  /** @type {Manifest} */
  const newManifest = {
    manifest_path: manifestPath,
    manifest_length: manifestLength,
    partition_spec_id: partitionSpec['spec-id'],
    content: 0,
    sequence_number: sequenceNumber,
    min_sequence_number: sequenceNumber,
    added_snapshot_id: snapshotId,
    added_files_count: writtenDataFiles.length,
    existing_files_count: 0,
    deleted_files_count: 0,
    added_rows_count: totalAddedRows,
    existing_rows_count: 0n,
    deleted_rows_count: 0n,
    partitions,
  }

  // 3. Build the snapshot summary and assemble the StagedUpdate
  const prevSummary = currentSnapshot(metadata)?.summary
  const prevTotals = {
    records: BigInt(prevSummary?.['total-records'] ?? '0'),
    size: BigInt(prevSummary?.['total-files-size'] ?? '0'),
    files: BigInt(prevSummary?.['total-data-files'] ?? '0'),
  }
  /** @type {Snapshot['summary']} */
  const summary = {
    operation: 'append',
    'added-data-files': String(writtenDataFiles.length),
    'added-records': String(records.length),
    'added-files-size': String(totalAddedSize),
    'changed-partition-count': String(writtenDataFiles.length),
    'total-records': String(prevTotals.records + BigInt(records.length)),
    'total-files-size': String(prevTotals.size + totalAddedSize),
    'total-data-files': String(prevTotals.files + BigInt(writtenDataFiles.length)),
    'total-delete-files': '0',
    'total-position-deletes': '0',
    'total-equality-deletes': '0',
  }
  return await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId, sequenceNumber, manifestUuid, timestampMs, formatVersion,
    newManifests: [newManifest],
    summary,
    writtenFiles: [...writtenDataFiles.map(f => f.path), manifestPath],
  })
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
