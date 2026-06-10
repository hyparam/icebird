import { uuid4 } from '../utils.js'
import { icebergRead } from '../read.js'
import { writeDataManifest } from './manifest.js'
import { writeParquet } from './parquet.js'
import { groupByPartition } from './partition.js'
import { buildSortComparator } from './sort.js'
import {
  buildPartitionSummaries,
  buildSnapshotUpdate,
  currentSnapshot,
  loadPriorManifests,
} from './snapshot.js'
import { computeColumnStats } from './stats.js'
import { checkWriteFormat, newSnapshotId, resolveParquetCodec } from './stage.js'

/**
 * @import {Manifest, Resolver, Schema, Snapshot, StagedUpdate, TableMetadata} from '../../src/types.js'
 */

/**
 * Stage a compaction / rewrite of a table's current snapshot. Reads every live
 * row (applying all delete files), orders the rows by the declared sort order,
 * regroups them under the target partition spec, and writes new sorted data
 * files. The result is committed as a `replace` snapshot that supersedes all of
 * the prior snapshot's data and delete manifests.
 *
 * Because globally-sorted rows are written in order, the per-file bounds of the
 * sort key are tight; with `targetFileRows` set, consecutive output files have
 * non-overlapping sort-key ranges (clean splits assume distinct keys at the
 * boundary). Row contents and counts are preserved (modulo deleted rows and
 * order); deletes are consumed, so the new snapshot has no delete files.
 *
 * On v3 tables, row lineage is preserved: each surviving row's `_row_id` and
 * `_last_updated_sequence_number` are materialized as explicit columns in the
 * rewritten files (the global sort breaks positional derivation, so stored
 * values are required). When every live row carries lineage, the new manifest's
 * `first_row_id` is pinned to the minimum carried `_row_id` so no new row ids
 * are consumed (`next-row-id` does not advance). When some rows lack lineage
 * (a table upgraded from v2 whose pre-upgrade rows were never assigned ids),
 * the manifest is left for commit-time assignment per the spec: stored ids
 * still win on read, null rows get derived ids, and `next-row-id` advances by
 * the manifest's row count.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata - Current (freshest) table metadata.
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @param {number} [options.sortOrderId] - Sort order id to apply; defaults to the table default.
 * @param {number} [options.partitionSpecId] - Target partition spec id; defaults to `default-spec-id`.
 * @param {number} [options.targetFileRows] - Max rows per output file (split large partitions).
 * @returns {Promise<StagedUpdate>}
 */
export async function icebergStageRewrite({
  tableUrl, metadata, resolver, sortOrderId, partitionSpecId, targetFileRows,
}) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (!resolver?.writer) throw new Error('resolver.writer is required')
  const writerFn = resolver.writer
  if (metadata['format-version'] !== 2 && metadata['format-version'] !== 3) {
    throw new Error(`unsupported format-version: ${metadata['format-version']}`)
  }
  const formatVersion = /** @type {2|3} */ (metadata['format-version'])
  if (targetFileRows !== undefined && !(targetFileRows > 0)) {
    throw new Error('targetFileRows must be a positive number')
  }

  const snapshot = currentSnapshot(metadata)
  if (!snapshot) throw new Error('no current snapshot to rewrite')

  const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('current schema not found in metadata')
  const specId = partitionSpecId ?? metadata['default-spec-id']
  const partitionSpec = metadata['partition-specs'].find(s => s['spec-id'] === specId)
  if (!partitionSpec) throw new Error(`partition spec ${specId} not found in metadata`)

  // Resolve the sort order to apply (table default unless overridden).
  const orderId = sortOrderId ?? metadata['default-sort-order-id'] ?? 0
  const sortOrder = (metadata['sort-orders'] ?? []).find(o => o['order-id'] === orderId)
  if (sortOrderId !== undefined && !sortOrder) {
    throw new Error(`sort order ${sortOrderId} not found in metadata`)
  }
  const comparator = buildSortComparator(sortOrder, schema)
  const appliedSortOrderId = comparator ? orderId : 0

  checkWriteFormat(metadata.properties?.['write.format.default'])
  const codec = resolveParquetCodec(metadata.properties?.['write.parquet.compression-codec'])

  // Read every live row (deletes applied), then sort globally. For v3 tables
  // the rows carry `_row_id` / `_last_updated_sequence_number` (derived or
  // stored by the read path).
  const liveRows = await icebergRead({ tableUrl, metadata, resolver })
  const sortedRows = comparator ? [...liveRows].sort(comparator) : liveRows

  const rowLineage = formatVersion >= 3
  // Preserve mode requires complete lineage: rows from pre-upgrade v2
  // snapshots read with null ids and need commit-time assignment instead.
  const allLineage = rowLineage && liveRows.length > 0 &&
    liveRows.every(r => r._row_id != null && r._last_updated_sequence_number != null)
  const minRowId = allLineage
    ? liveRows.reduce((min, r) => r._row_id < min ? r._row_id : min, liveRows[0]._row_id)
    : undefined

  // Regroup under the target partition spec (re-derives tuples from values, so
  // files written under an older spec are rewritten under the new one).
  const groups = partitionSpec.fields.length
    ? groupByPartition(sortedRows, schema, partitionSpec)
    : [{ partition: {}, records: sortedRows }]

  const snapshotId = newSnapshotId(metadata)
  const manifestUuid = uuid4()

  // For v3, materialize the carried lineage as explicit columns in the
  // rewritten files (reserved field ids per spec). The extended schema is
  // passed to the parquet writer only: stats, partitioning, and the manifest's
  // embedded schema stay user-only.
  /** @type {Schema} */
  const writeSchema = rowLineage
    ? {
      ...schema,
      fields: [
        ...schema.fields,
        { id: 2147483540, name: '_row_id', required: false, type: 'long' },
        { id: 2147483539, name: '_last_updated_sequence_number', required: false, type: 'long' },
      ],
    }
    : schema

  /** @type {{ partition: Record<string, any>, dataFile: any, path: string }[]} */
  const writtenDataFiles = []
  for (const group of groups) {
    const chunks = targetFileRows ? chunkRecords(group.records, targetFileRows) : [group.records]
    for (const chunk of chunks) {
      if (chunk.length === 0) continue
      const dataPath = `${tableUrl}/data/${uuid4()}.parquet`
      const dataWriter = writerFn(dataPath)
      await writeParquet({ writer: dataWriter, schema: writeSchema, records: chunk, codec })
      const stats = computeColumnStats(chunk, schema)
      writtenDataFiles.push({
        partition: group.partition,
        dataFile: {
          content: /** @type {0} */ (0),
          file_path: dataPath,
          file_format: /** @type {'parquet'} */ ('parquet'),
          partition: group.partition,
          record_count: BigInt(chunk.length),
          file_size_in_bytes: BigInt(dataWriter.offset),
          value_counts: stats.value_counts,
          null_value_counts: stats.null_value_counts,
          nan_value_counts: stats.nan_value_counts,
          lower_bounds: stats.lower_bounds,
          upper_bounds: stats.upper_bounds,
          sort_order_id: appliedSortOrderId,
        },
        path: dataPath,
      })
    }
  }

  const manifestPath = `${tableUrl}/metadata/${manifestUuid}-m0.avro`
  const manifestWriter = writerFn(manifestPath)
  await writeDataManifest({
    writer: manifestWriter,
    schema,
    partitionSpec,
    snapshotId,
    dataFiles: writtenDataFiles.map(f => f.dataFile),
    formatVersion,
  })
  const manifestLength = BigInt(manifestWriter.offset)

  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const timestampMs = Date.now()
  const addedRowCount = writtenDataFiles.reduce((sum, f) => sum + f.dataFile.record_count, 0n)
  const addedFilesSize = writtenDataFiles.reduce((sum, f) => sum + f.dataFile.file_size_in_bytes, 0n)
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
    added_rows_count: addedRowCount,
    existing_rows_count: 0n,
    deleted_rows_count: 0n,
    partitions,
  }
  if (allLineage) {
    // Every rewritten row carries a materialized `_row_id`, so the manifest
    // does not need a fresh id range from `assignFirstRowIds`. Pin its
    // first_row_id to the smallest carried id: ids are distinct and below
    // next-row-id, so min + row count never exceeds next-row-id and no new
    // ids are consumed (`next-row-id` does not advance for a pure rewrite).
    newManifest.first_row_id = minRowId
  }

  // Supersede every prior manifest (data + delete) for the rewritten snapshot.
  const priorManifests = await loadPriorManifests(metadata, resolver)
  const skipPriorManifestPaths = new Set(priorManifests.map(m => m.manifest_path))
  let deletedDataFiles = 0
  let deletedRecords = 0n
  for (const m of priorManifests) {
    if (m.content !== 0) continue
    deletedDataFiles += (m.added_files_count ?? 0) + (m.existing_files_count ?? 0)
    deletedRecords += BigInt(m.added_rows_count ?? 0) + BigInt(m.existing_rows_count ?? 0)
  }

  /** @type {Snapshot['summary']} */
  const summary = {
    operation: 'replace',
    'added-data-files': String(writtenDataFiles.length),
    'added-records': String(addedRowCount),
    'added-files-size': String(addedFilesSize),
    'deleted-data-files': String(deletedDataFiles),
    'deleted-records': String(deletedRecords),
    'total-records': String(addedRowCount),
    'total-files-size': String(addedFilesSize),
    'total-data-files': String(writtenDataFiles.length),
    'total-delete-files': '0',
    'total-position-deletes': '0',
    'total-equality-deletes': '0',
  }

  const staged = await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId, sequenceNumber, manifestUuid, timestampMs, formatVersion,
    newManifests: [newManifest],
    summary,
    writtenFiles: [...writtenDataFiles.map(f => f.path), manifestPath],
    priorManifests,
    skipPriorManifestPaths,
  })
  return staged
}

/**
 * Split an array into chunks of at most `size` elements.
 *
 * @template T
 * @param {T[]} arr
 * @param {number} size
 * @returns {T[][]}
 */
function chunkRecords(arr, size) {
  /** @type {T[][]} */
  const out = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}
