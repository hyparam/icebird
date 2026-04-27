import { fetchAvroRecords, translateS3Url } from '../fetch.js'
import { uuid4 } from '../utils.js'
import { writeParquet } from './parquet.js'
import { writeDataManifest } from './manifest.js'
import { writeManifestList } from './manifest-list.js'
import { groupByPartition } from './partition.js'
import { computeColumnStats } from './stats.js'

/**
 * @import {Manifest, Resolver, Snapshot, StagedUpdate, TableMetadata, TableRequirement} from '../../src/types.js'
 */

/**
 * Build the data files, manifest, and manifest list for an append, then
 * return the catalog-agnostic `StagedUpdate` payload (snapshot plus the
 * requirements/updates a catalog must apply to commit it).
 *
 * No metadata.json or version-hint is written — pass the result to a commit
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
  const rowLineage = formatVersion >= 3
  const partitionSpec = metadata['partition-specs'].find(s => s['spec-id'] === metadata['default-spec-id'])
  if (!partitionSpec) throw new Error('default partition spec not found in metadata')
  const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('current schema not found in metadata')

  const snapshotId = newSnapshotId()
  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const manifestUuid = uuid4()
  const timestampMs = Date.now()
  const firstRowId = rowLineage ? BigInt(metadata['next-row-id'] ?? 0) : undefined

  // 1. Group records by partition tuple, then write one parquet per group in parallel.
  const groups = partitionSpec.fields.length
    ? groupByPartition(records, schema, partitionSpec)
    : [{ partition: {}, records }]
  const writtenDataFiles = await Promise.all(groups.map(group => {
    const dataPath = `${tableUrl}/data/${uuid4()}.parquet`
    const dataWriter = writerFn(translateS3Url(dataPath))
    writeParquet({ writer: dataWriter, schema, records: group.records })
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
  writeDataManifest({
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
  }

  // 3. Carry forward manifests from prior snapshot, then write new manifest list
  const priorManifests = await loadPriorManifests(metadata, resolver)
  const allManifests = [newManifest, ...priorManifests]
  const addedRows = rowLineage ? assignFirstRowIds(allManifests, firstRowId ?? 0n) : 0n
  const manifestListPath = `${tableUrl}/metadata/snap-${snapshotId}-1-${manifestUuid}.avro`
  const listWriter = writerFn(translateS3Url(manifestListPath))
  writeManifestList({ writer: listWriter, snapshotId, sequenceNumber, manifests: allManifests, formatVersion })

  // 4. Build the new snapshot
  const prevSummary = currentSnapshot(metadata)?.summary
  const prevTotals = {
    records: BigInt(prevSummary?.['total-records'] ?? '0'),
    size: BigInt(prevSummary?.['total-files-size'] ?? '0'),
    files: BigInt(prevSummary?.['total-data-files'] ?? '0'),
  }
  /** @type {Snapshot} */
  const snapshot = {
    'snapshot-id': Number(snapshotId),
    'sequence-number': Number(sequenceNumber),
    'timestamp-ms': timestampMs,
    'manifest-list': manifestListPath,
    summary: {
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
    },
    'schema-id': metadata['current-schema-id'],
  }
  if (rowLineage) {
    snapshot['first-row-id'] = toMetadataLong(firstRowId ?? 0n)
    snapshot['added-rows'] = toMetadataLong(addedRows)
  }
  const parentId = metadata['current-snapshot-id']
  if (parentId !== undefined) snapshot['parent-snapshot-id'] = parentId
  /** @type {TableRequirement[]} */
  const requirements = [
    { type: 'assert-table-uuid', uuid: metadata['table-uuid'] },
    {
      type: 'assert-ref-snapshot-id',
      ref: 'main',
      'snapshot-id': metadata['current-snapshot-id'] ?? null,
    },
  ]
  if (rowLineage) {
    requirements.push({
      type: 'assert-next-row-id',
      'next-row-id': toMetadataLong(metadata['next-row-id'] ?? 0),
    })
  }

  return {
    snapshot,
    requirements,
    updates: [
      { action: 'add-snapshot', snapshot },
      {
        action: 'set-snapshot-ref',
        'ref-name': 'main',
        type: 'branch',
        'snapshot-id': snapshot['snapshot-id'],
      },
    ],
    writtenFiles: [...writtenDataFiles.map(f => f.path), manifestPath, manifestListPath],
  }
}

/**
 * @param {TableMetadata} metadata
 * @returns {Snapshot|undefined}
 */
function currentSnapshot(metadata) {
  const id = metadata['current-snapshot-id']
  if (id === undefined) return undefined
  return metadata.snapshots?.find(s => s['snapshot-id'] === id)
}

/**
 * @param {TableMetadata} metadata
 * @param {Resolver} resolver
 * @returns {Promise<Manifest[]>}
 */
async function loadPriorManifests(metadata, resolver) {
  const snap = currentSnapshot(metadata)
  if (!snap?.['manifest-list']) return []
  return /** @type {Manifest[]} */ (await fetchAvroRecords(snap['manifest-list'], resolver))
}

/**
 * Assign v3 first_row_id values to data manifests that do not already have
 * them and return the number of newly assigned row IDs.
 *
 * @param {Manifest[]} manifests
 * @param {bigint} firstRowId
 * @returns {bigint}
 */
function assignFirstRowIds(manifests, firstRowId) {
  let nextFirstRowId = firstRowId
  let assignedRows = 0n
  for (const manifest of manifests) {
    if (manifest.content !== 0) {
      manifest.first_row_id = undefined
      continue
    }

    const rowIdRange = BigInt(manifest.added_rows_count ?? 0) + BigInt(manifest.existing_rows_count ?? 0)
    if (manifest.first_row_id == null) {
      manifest.first_row_id = nextFirstRowId
      nextFirstRowId += rowIdRange
      assignedRows += rowIdRange
    } else {
      const manifestEnd = BigInt(manifest.first_row_id) + rowIdRange
      if (manifestEnd > nextFirstRowId) nextFirstRowId = manifestEnd
    }
  }
  return assignedRows
}

/**
 * @param {number|bigint} value
 * @returns {number}
 */
function toMetadataLong(value) {
  const out = Number(value)
  if (!Number.isSafeInteger(out)) {
    throw new Error(`metadata long exceeds JavaScript safe integer range: ${value}`)
  }
  return out
}

/**
 * Generate a positive snapshot id that fits in a JS Number.
 *
 * @returns {bigint}
 */
function newSnapshotId() {
  const arr = new BigInt64Array(1)
  globalThis.crypto.getRandomValues(arr)
  const masked = arr[0] & 0x1fffffffffffffn // 53 bits
  return masked === 0n ? 1n : masked
}
