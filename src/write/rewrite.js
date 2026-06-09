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
 * @import {Manifest, Resolver, Snapshot, StagedUpdate, TableMetadata} from '../../src/types.js'
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
 * v2 only for now: a v3 rewrite would have to preserve `_row_id` row lineage
 * rather than let `assignFirstRowIds` renumber the rewritten rows.
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
  const formatVersion = metadata['format-version']
  if (formatVersion !== 2) {
    throw new Error(`icebergRewrite supports format-version 2 only (got ${formatVersion}); v3 row lineage is not yet handled`)
  }
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

  // Read every live row (deletes applied), then sort globally.
  const liveRows = await icebergRead({ tableUrl, metadata, resolver })
  const sortedRows = comparator ? [...liveRows].sort(comparator) : liveRows

  // Regroup under the target partition spec (re-derives tuples from values, so
  // files written under an older spec are rewritten under the new one).
  const groups = partitionSpec.fields.length
    ? groupByPartition(sortedRows, schema, partitionSpec)
    : [{ partition: {}, records: sortedRows }]

  const snapshotId = newSnapshotId(metadata)
  const manifestUuid = uuid4()

  /** @type {{ partition: Record<string, any>, dataFile: any, path: string }[]} */
  const writtenDataFiles = []
  for (const group of groups) {
    const chunks = targetFileRows ? chunkRecords(group.records, targetFileRows) : [group.records]
    for (const chunk of chunks) {
      if (chunk.length === 0) continue
      const dataPath = `${tableUrl}/data/${uuid4()}.parquet`
      const dataWriter = writerFn(dataPath)
      await writeParquet({ writer: dataWriter, schema, records: chunk, codec })
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
