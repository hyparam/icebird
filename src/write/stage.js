import { fetchAvroRecords, translateS3Url } from '../fetch.js'
import { uuid4 } from '../utils.js'
import { writeParquet } from './parquet.js'
import { writeDataManifest } from './manifest.js'
import { writeManifestList } from './manifest-list.js'
import { computeColumnStats } from './stats.js'

/**
 * @import {Manifest, Resolver, Snapshot, StagedUpdate, TableMetadata} from '../../src/types.js'
 */

/**
 * Build the data files, manifest, and manifest list for an unpartitioned
 * append, then return the catalog-agnostic `StagedUpdate` payload (snapshot
 * plus the requirements/updates a catalog must apply to commit it).
 *
 * No metadata.json or version-hint is written — pass the result to a commit
 * function (`fileCatalogCommit`, or a future `restCatalogCommitTable`).
 *
 * Only supports v2 tables with a flat unpartitioned schema and primitive
 * column types.
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
  if (metadata['format-version'] !== 2) {
    throw new Error(`unsupported format-version: ${metadata['format-version']}`)
  }
  const partitionSpec = metadata['partition-specs'].find(s => s['spec-id'] === metadata['default-spec-id'])
  if (!partitionSpec || partitionSpec.fields.length) {
    throw new Error('icebergStageAppend only supports unpartitioned tables')
  }
  const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('current schema not found in metadata')

  const snapshotId = newSnapshotId()
  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const dataUuid = uuid4()
  const manifestUuid = uuid4()
  const timestampMs = Date.now()

  // 1. Write parquet data file
  const dataPath = `${tableUrl}/data/${dataUuid}.parquet`
  const dataWriter = resolver.writer(translateS3Url(dataPath))
  writeParquet({ writer: dataWriter, schema, records })
  const dataFileSize = BigInt(dataWriter.offset)

  // 2. Write manifest
  const stats = computeColumnStats(records, schema)
  const manifestPath = `${tableUrl}/metadata/${manifestUuid}-m0.avro`
  const manifestWriter = resolver.writer(translateS3Url(manifestPath))
  writeDataManifest({
    writer: manifestWriter,
    schema,
    snapshotId,
    dataFile: {
      content: 0,
      file_path: dataPath,
      file_format: 'parquet',
      partition: {},
      record_count: BigInt(records.length),
      file_size_in_bytes: dataFileSize,
      value_counts: stats.value_counts,
      null_value_counts: stats.null_value_counts,
      nan_value_counts: stats.nan_value_counts,
      lower_bounds: stats.lower_bounds,
      upper_bounds: stats.upper_bounds,
      sort_order_id: 0,
    },
  })
  const manifestLength = BigInt(manifestWriter.offset)

  /** @type {Manifest} */
  const newManifest = {
    manifest_path: manifestPath,
    manifest_length: manifestLength,
    partition_spec_id: 0,
    content: 0,
    sequence_number: sequenceNumber,
    min_sequence_number: sequenceNumber,
    added_snapshot_id: snapshotId,
    added_files_count: 1,
    existing_files_count: 0,
    deleted_files_count: 0,
    added_rows_count: BigInt(records.length),
    existing_rows_count: 0n,
    deleted_rows_count: 0n,
  }

  // 3. Carry forward manifests from prior snapshot, then write new manifest list
  const priorManifests = await loadPriorManifests(metadata, resolver)
  const allManifests = [newManifest, ...priorManifests]
  const manifestListPath = `${tableUrl}/metadata/snap-${snapshotId}-1-${manifestUuid}.avro`
  const listWriter = resolver.writer(translateS3Url(manifestListPath))
  writeManifestList({ writer: listWriter, snapshotId, sequenceNumber, manifests: allManifests })

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
      'added-data-files': '1',
      'added-records': String(records.length),
      'added-files-size': String(dataFileSize),
      'changed-partition-count': '1',
      'total-records': String(prevTotals.records + BigInt(records.length)),
      'total-files-size': String(prevTotals.size + dataFileSize),
      'total-data-files': String(prevTotals.files + 1n),
      'total-delete-files': '0',
      'total-position-deletes': '0',
      'total-equality-deletes': '0',
    },
    'schema-id': metadata['current-schema-id'],
  }
  const parentId = metadata['current-snapshot-id']
  if (parentId !== undefined) snapshot['parent-snapshot-id'] = parentId

  return {
    snapshot,
    requirements: [
      { type: 'assert-table-uuid', uuid: metadata['table-uuid'] },
      {
        type: 'assert-ref-snapshot-id',
        ref: 'main',
        'snapshot-id': metadata['current-snapshot-id'] ?? null,
      },
    ],
    updates: [
      { action: 'add-snapshot', snapshot },
      {
        action: 'set-snapshot-ref',
        'ref-name': 'main',
        type: 'branch',
        'snapshot-id': snapshot['snapshot-id'],
      },
    ],
    writtenFiles: [dataPath, manifestPath, manifestListPath],
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
