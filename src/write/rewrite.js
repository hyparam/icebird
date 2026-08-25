import { concat } from 'hyparquet/src/utils.js'
import { fetchDeleteMaps } from '../fetch.js'
import { icebergManifests, splitManifestEntries } from '../manifest.js'
import { readDataFile } from '../read.js'
import { uuid4 } from '../utils.js'
import { writeDataManifest, writeExistingDataManifest } from './manifest.js'
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
 * @import {Manifest, ManifestEntry, Resolver, Schema, Snapshot, StagedUpdate, TableMetadata} from '../../src/types.js'
 */

/**
 * Stage a compaction / rewrite of a table's current snapshot. Reads the live
 * rows of the files being rewritten (applying the delete files that target
 * them), orders those rows by the declared sort order, regroups them under the
 * target partition spec, and writes new sorted data files. The result is
 * committed as a `replace` snapshot.
 *
 * By default every data file is rewritten and the new snapshot supersedes all
 * of the prior snapshot's data and delete manifests. Pass `files` to rewrite
 * only that subset: every other data file is carried forward untouched, with
 * its original data and file sequence numbers, so cost scales with the bytes
 * in `files` rather than with the table. That lets a maintenance pass compact
 * one fragmented partition's small files while leaving an arbitrarily large
 * table's converged files alone.
 *
 * In subset mode, prior manifests that reference none of the named files are
 * reused as-is; one that does is replaced by a manifest carrying its surviving
 * entries as EXISTING. The superseded files are dropped from the manifest list
 * and counted in the snapshot summary (physical cleanup is a separate
 * orphan-removal pass). Delete files are not rewritten: one targeting only
 * rewritten data files becomes inert (position deletes bind to the exact
 * superseded path; equality deletes never apply to the new files' higher
 * sequence number) and is reclaimed by the next full rewrite, while one
 * targeting a survivor still applies through the survivor's carried sequence
 * numbers. The rows a consumed delete removed were already dropped here,
 * because the rewrite reads with deletes applied.
 *
 * Because sorted rows are written in order, the per-file bounds of the
 * sort key are tight; with `targetFileRows` set, consecutive output files have
 * non-overlapping sort-key ranges (clean splits assume distinct keys at the
 * boundary). Row contents and counts are preserved (modulo deleted rows and
 * order).
 *
 * On v3 tables, row lineage is preserved: each rewritten row's `_row_id` and
 * `_last_updated_sequence_number` are materialized as explicit columns in the
 * rewritten files (the sort breaks positional derivation, so stored
 * values are required). When every rewritten row carries lineage, the new
 * manifest's `first_row_id` is pinned to the minimum carried `_row_id` so no
 * new row ids are consumed (`next-row-id` does not advance). When some rows
 * lack lineage (a table upgraded from v2 whose pre-upgrade rows were never
 * assigned ids), the manifest is left for commit-time assignment per the
 * spec: stored ids still win on read, null rows get derived ids, and
 * `next-row-id` advances by the manifest's row count.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata - Current (freshest) table metadata.
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @param {string[]} [options.files] - `file_path`s of the live data files to rewrite, exactly as recorded in the current snapshot's manifests; every data file when omitted.
 * @param {number} [options.sortOrderId] - Sort order id to apply; defaults to the table default.
 * @param {number} [options.partitionSpecId] - Target partition spec id for the rewritten files; defaults to `default-spec-id`.
 * @param {number} [options.targetFileRows] - Max rows per output file (split large partitions).
 * @returns {Promise<StagedUpdate>}
 */
export async function icebergStageRewrite({
  tableUrl, metadata, resolver, files, sortOrderId, partitionSpecId, targetFileRows,
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
  /** @type {Set<string> | undefined} */
  let targetPaths
  if (files !== undefined) {
    if (!files.length) throw new Error('files must be a non-empty array of data file paths')
    targetPaths = new Set(files)
    if (targetPaths.size !== files.length) throw new Error('files contains duplicate paths')
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

  // Manifest records (for the list rewrite) and their decoded entries (for
  // victim/survivor partitioning), zipped by path. `icebergManifests` also
  // applies sequence-number and v3 first-row-id inheritance, which both the
  // rewritten rows and the carried-over entries must keep.
  const priorManifests = await loadPriorManifests(metadata, resolver)
  const manifestList = await icebergManifests({ metadata, resolver })
  const entriesByManifestPath = new Map(manifestList.map(m => [m.url, m.entries]))

  /** @type {{ manifest: Manifest, survivors: ManifestEntry[] }[]} */
  const affectedManifests = []
  /** @type {ManifestEntry[]} */
  const victimEntries = []
  let liveDataFiles = 0
  let survivorFiles = 0
  let survivorRecords = 0n
  let survivorBytes = 0n
  for (const manifest of priorManifests) {
    if (manifest.content !== 0) continue
    const entries = entriesByManifestPath.get(manifest.manifest_path) ?? []
    /** @type {ManifestEntry[]} */
    const victims = []
    /** @type {ManifestEntry[]} */
    const survivors = []
    for (const entry of entries) {
      if (entry.status === 2 || entry.data_file.content !== 0) continue
      liveDataFiles++
      if (!targetPaths || targetPaths.has(entry.data_file.file_path)) victims.push(entry)
      else survivors.push(entry)
    }
    for (const entry of survivors) {
      survivorFiles++
      survivorRecords += BigInt(entry.data_file.record_count)
      survivorBytes += BigInt(entry.data_file.file_size_in_bytes)
    }
    if (victims.length) {
      affectedManifests.push({ manifest, survivors })
      concat(victimEntries, victims)
    }
  }
  if (!targetPaths && liveDataFiles === 0) {
    throw new Error('No data manifest files found for current snapshot')
  }
  if (targetPaths) {
    const foundPaths = new Set(victimEntries.map(entry => entry.data_file.file_path))
    for (const path of targetPaths) {
      if (!foundPaths.has(path)) {
        throw new Error(`data file not found in current snapshot: ${path}`)
      }
    }
  }
  // With no survivors left, no delete file has anything to apply to: drop the
  // delete manifests too, consuming every delete, exactly as a full rewrite
  // always has.
  const survivorsExist = survivorFiles > 0

  // Read the victims' live rows, with every applicable delete applied, so the
  // rewritten files need no delete files of their own. For v3 tables the rows
  // carry `_row_id` / `_last_updated_sequence_number` (derived or stored by
  // the read path).
  const { deleteEntries } = splitManifestEntries(manifestList)
  const { positionDeletesMap, equalityDeleteGroups } = await fetchDeleteMaps(deleteEntries, resolver)
  const rowLineage = formatVersion >= 3
  const victimRowArrays = await Promise.all(victimEntries.map(async entry => {
    /** @type {Record<string, any>[]} */
    const rows = []
    for await (const batch of readDataFile({
      dataEntry: entry,
      fileRowStart: 0,
      fileRowEnd: Number(entry.data_file.record_count),
      schema,
      metadata,
      resolver,
      rowLineage,
      positionDeletesMap,
      equalityDeleteGroups,
    })) {
      concat(rows, batch)
    }
    return rows
  }))
  const liveRows = victimRowArrays.flat()
  const sortedRows = comparator ? [...liveRows].sort(comparator) : liveRows

  // Preserve mode requires complete lineage across the rewritten rows: rows
  // from pre-upgrade v2 snapshots read with null ids and need commit-time
  // assignment instead. Survivor files keep their stored or manifest-derived
  // ids untouched either way.
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

  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const timestampMs = Date.now()

  /** @type {Manifest[]} */
  const newManifests = []
  /** @type {string[]} */
  const writtenFiles = writtenDataFiles.map(f => f.path)
  /** @type {Set<string>} */
  const skipPriorManifestPaths = new Set()

  // Replace each affected prior manifest with one carrying its survivors as
  // EXISTING entries (original snapshot ids and sequence numbers, so which
  // delete files apply to them cannot shift). A manifest whose every entry was
  // rewritten is dropped with no replacement. Victims are omitted rather than
  // written as DELETED entries, matching how snapshot expiry treats removed
  // files; physical cleanup is a separate orphan-removal pass.
  let replacementIndex = 0
  for (const { manifest, survivors } of affectedManifests) {
    skipPriorManifestPaths.add(manifest.manifest_path)
    if (!survivors.length) continue
    const spec = metadata['partition-specs'].find(s => s['spec-id'] === manifest.partition_spec_id)
    if (!spec) throw new Error(`partition spec ${manifest.partition_spec_id} not found in metadata`)
    const retained = survivors.map(entry => ({ ...entry, status: /** @type {0} */ (0) }))
    const manifestPath = `${tableUrl}/metadata/${manifestUuid}-r${replacementIndex}.avro`
    const manifestWriter = writerFn(manifestPath)
    await writeExistingDataManifest({
      writer: manifestWriter,
      schema,
      partitionSpec: spec,
      entries: retained,
      formatVersion,
    })
    const existingRows = retained.reduce((sum, entry) => sum + BigInt(entry.data_file.record_count), 0n)
    /** @type {Manifest} */
    const replacement = {
      manifest_path: manifestPath,
      manifest_length: BigInt(manifestWriter.offset),
      partition_spec_id: spec['spec-id'],
      content: 0,
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
    }
    // Carried entries keep their explicit per-file first_row_id (inherited on
    // read), so pin the manifest to their minimum: a null here would make
    // commit-time assignment hand fresh row ids to rows that already own ids.
    const carriedIds = retained.map(entry => entry.data_file.first_row_id).filter(id => id != null)
    if (rowLineage && carriedIds.length === retained.length) {
      replacement.first_row_id = carriedIds.reduce((min, id) => id < min ? id : min)
    }
    newManifests.push(replacement)
    writtenFiles.push(manifestPath)
    replacementIndex++
  }

  const addedRowCount = writtenDataFiles.reduce((sum, f) => sum + f.dataFile.record_count, 0n)
  const addedFilesSize = writtenDataFiles.reduce((sum, f) => sum + f.dataFile.file_size_in_bytes, 0n)
  {
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
    /** @type {Manifest} */
    const newManifest = {
      manifest_path: manifestPath,
      manifest_length: BigInt(manifestWriter.offset),
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
      partitions: buildPartitionSummaries(
        writtenDataFiles.map(f => f.dataFile.partition),
        schema,
        partitionSpec
      ),
    }
    if (allLineage) {
      // Every rewritten row carries a materialized `_row_id`, so the manifest
      // does not need a fresh id range from `assignFirstRowIds`. Pin its
      // first_row_id to the smallest carried id: ids are distinct and below
      // next-row-id, so min + row count never exceeds next-row-id and no new
      // ids are consumed (`next-row-id` does not advance for a pure rewrite).
      newManifest.first_row_id = minRowId
    }
    newManifests.push(newManifest)
    writtenFiles.push(manifestPath)
  }

  // Supersede the delete manifests only when nothing they target survives.
  if (!survivorsExist) {
    for (const manifest of priorManifests) {
      if (manifest.content !== 0) skipPriorManifestPaths.add(manifest.manifest_path)
    }
  }
  const deletedDataFiles = victimEntries.length
  const deletedRecords = victimEntries.reduce((sum, entry) => sum + BigInt(entry.data_file.record_count), 0n)
  let totalDeleteFiles = 0
  let totalPositionDeletes = 0n
  let totalEqualityDeletes = 0n
  if (survivorsExist) {
    for (const entry of deleteEntries) {
      totalDeleteFiles++
      if (entry.data_file.content === 1) totalPositionDeletes += BigInt(entry.data_file.record_count)
      else totalEqualityDeletes += BigInt(entry.data_file.record_count)
    }
  }

  /** @type {Snapshot['summary']} */
  const summary = {
    operation: 'replace',
    'added-data-files': String(writtenDataFiles.length),
    'added-records': String(addedRowCount),
    'added-files-size': String(addedFilesSize),
    'deleted-data-files': String(deletedDataFiles),
    'deleted-records': String(deletedRecords),
    'total-records': String(survivorRecords + addedRowCount),
    'total-files-size': String(survivorBytes + addedFilesSize),
    'total-data-files': String(survivorFiles + writtenDataFiles.length),
    'total-delete-files': String(totalDeleteFiles),
    'total-position-deletes': String(totalPositionDeletes),
    'total-equality-deletes': String(totalEqualityDeletes),
  }

  return await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId, sequenceNumber, manifestUuid, timestampMs, formatVersion,
    newManifests,
    summary,
    writtenFiles,
    priorManifests,
    skipPriorManifestPaths,
  })
}

/**
 * Smallest data sequence number among carried-over entries, or `fallback`
 * for an empty list.
 *
 * @param {ManifestEntry[]} entries
 * @param {bigint} fallback
 * @returns {bigint}
 */
function minEntrySequenceNumber(entries, fallback) {
  let min = fallback
  for (const entry of entries) {
    const seq = entry.sequence_number
    if (seq != null && BigInt(seq) < min) min = BigInt(seq)
  }
  return min
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
