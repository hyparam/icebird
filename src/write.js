
/**
 * @import {Writer} from 'hyparquet-writer/src/types.js'
 * @import {Manifest, ManifestEntry, TableMetadata} from '../src/types.js'
 */

import { parquetMetadata } from 'hyparquet'
import { avroWrite } from './avro/avro.write.js'
import { icebergLatestVersion, icebergMetadata } from './metadata.js'
import { manifestEntrySchema, manifestSchema } from './schemas.js'
import { uuid4 } from './utils.js'

/**
 * Replace all data in an iceberg table with new parquet file.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base S3 URL of the table.
 * @param {(file: string) => Writer} options.writerFactory - Function to create writers for files in storage.
 * @param {ArrayBuffer} options.parquetBuffer - Buffer containing the new parquet data to write.
 * @returns {Promise<void>} Resolves when the replacement is complete.
 */
export async function icebergReplace({ tableUrl, writerFactory, parquetBuffer }) {
  if (!tableUrl) throw new Error('tableUrl is required')

  // get latest metadata version (TODO: allow it to be passed in)
  const latestVersion = await icebergLatestVersion({ tableUrl })
  const latestVersionNumber = Number(latestVersion.replace('v', ''))
  if (isNaN(latestVersionNumber)) {
    throw new Error(`expected version to be a number, got ${latestVersion}`)
  }
  const metadataFileName = `v${latestVersionNumber + 1}.metadata.json`
  const previousMetadata = await icebergMetadata({ tableUrl, metadataFileName })
  const nextSequenceNumber = previousMetadata['last-sequence-number'] + 1
  const uuid = uuid4()
  const snapshotId = newId()

  const deleted_files_count = Number(previousMetadata.snapshots?.[0]?.summary['total-data-files'] ?? 0)
  const deleted_rows_count = BigInt(previousMetadata.snapshots?.[0]?.summary['total-records'] ?? 0)

  // TODO: check schema compatibility
  const sourceMetadata = parquetMetadata(parquetBuffer)

  // write data file to 0000-<uuid>.parquet
  const file_path = `${tableUrl}/data/0000-${uuid}.parquet`
  const dataWriter = writerFactory(file_path)
  dataWriter.appendBuffer(parquetBuffer)
  dataWriter.finish()

  // write manifest file to <uuid>.avro
  /** @type {ManifestEntry[]} */
  const manifestEntries = [{
    status: 1, // 1=added
    snapshot_id: undefined, // inherit
    sequence_number: undefined, // inherit
    file_sequence_number: undefined, // inherit
    data_file: {
      content: 0, // 0=data
      file_path,
      file_format: 'parquet',
      partition: {}, // unpartitioned
      record_count: BigInt(sourceMetadata.num_rows),
      file_size_in_bytes: BigInt(parquetBuffer.byteLength),
      split_offsets: undefined,
    },
  }]
  const manifest_path = `metadata/${snapshotId}.avro`
  const manifestWriter = writerFactory(`${tableUrl}/${manifest_path}`)
  avroWrite({ writer: manifestWriter, schema: manifestEntrySchema, records: manifestEntries })
  manifestWriter.finish()
  const manifest_length = BigInt(manifestWriter.offset)

  // write manifest list to snap-<snapshotId>.avro
  const manifestListPath = `metadata/snap-${snapshotId}.avro`
  const manifestListUrl = `${tableUrl}/${manifestListPath}`
  const manifestListWriter = writerFactory(manifestListUrl)
  /** @type {Manifest[]} */
  const records = [{
    manifest_path,
    manifest_length,
    partition_spec_id: previousMetadata['default-spec-id'],
    content: 0, // 0=data
    sequence_number: BigInt(nextSequenceNumber),
    min_sequence_number: BigInt(nextSequenceNumber),
    added_snapshot_id: snapshotId,
    added_files_count: 1,
    existing_files_count: 0,
    deleted_files_count,
    added_rows_count: sourceMetadata.num_rows,
    existing_rows_count: 0n,
    deleted_rows_count,
    partitions: undefined, // unpartitioned
    first_row_id: undefined,
  }]
  avroWrite({
    writer: manifestListWriter,
    schema: manifestSchema,
    records,
  })

  // write metadata file to vN.metadata.json
  const metadataUrl = `${tableUrl}/metadata/${metadataFileName}`
  const metadataWriter = writerFactory(metadataUrl)
  /** @type {TableMetadata} */
  const metadata = {
    'format-version': 2,
    'table-uuid': previousMetadata['table-uuid'],
    location: tableUrl,
    'last-sequence-number': nextSequenceNumber,
    'last-updated-ms': Date.now(),
    'last-column-id': previousMetadata['last-column-id'],
    'current-schema-id': previousMetadata['current-schema-id'], // TODO: update schema if needed
    schemas: previousMetadata.schemas, // TODO: update schemas if needed
    'default-spec-id': previousMetadata['default-spec-id'],
    'partition-specs': previousMetadata['partition-specs'],
    'last-partition-id': previousMetadata['last-partition-id'],
    // properties: { 'write.parquet.compression-codec': 'snappy' }, // TODO: add properties
    'current-snapshot-id': Number(snapshotId),
    snapshots: [
      ...previousMetadata.snapshots ?? [],
      {
        'snapshot-id': Number(snapshotId), // can't write bigint to json (TODO: use string)
        'timestamp-ms': Date.now(),
        'sequence-number': nextSequenceNumber,
        'manifest-list': manifestListPath,
        summary: {
          operation: 'overwrite',
          'added-data-files': '1',
          'added-records': sourceMetadata.num_rows.toString(),
          'added-files-size': parquetBuffer.byteLength.toString(),
          'changed-partition-count': '0',
          'total-records': sourceMetadata.num_rows.toString(),
          'total-files-size': parquetBuffer.byteLength.toString(),
          'total-data-files': '1',
          'total-delete-files': '0',
          'total-position-deletes': '0',
          'total-equality-deletes': '0',
        },
      },
    ],
    'sort-orders': previousMetadata['sort-orders'],
    'default-sort-order-id': previousMetadata['default-sort-order-id'],
    statistics: [],
  }
  // write metadata as json
  const metadataBytes = new TextEncoder().encode(JSON.stringify(metadata, null, 2))
  metadataWriter.appendBytes(metadataBytes)
  metadataWriter.finish()

  // update version-hint.text
  const versionHintUrl = `${tableUrl}/version-hint.text`
  const versionHintWriter = writerFactory(versionHintUrl)
  const versionHintBytes = new TextEncoder().encode(String(latestVersionNumber + 1))
  versionHintWriter.appendBytes(versionHintBytes)
  versionHintWriter.finish()

  // TODO: commit to catalog (update metadata_location)
}

/**
 * new snapshot-id: microsecond wall-clock, then 12 random bits
 * @returns {bigint} new snapshot id
 */
export function newId() {
  const micros = BigInt(Date.now()) * 1000n
  const rand = BigInt(Math.floor(Math.random() * 4096)) // 12 bits
  return micros << 12n | rand
}
