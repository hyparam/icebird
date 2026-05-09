import { deleteFileAppliesToDataEntry } from '../delete.js'
import { fetchAvroRecords, fetchDeleteMaps, translateS3Url } from '../fetch.js'
import { writeDeletionVector } from '../puffin/deletion-vector.js'
import { writePuffinFile } from '../puffin/puffin.js'
import { validateSchemaForVersion } from '../schema.js'
import { uuid4 } from '../utils.js'
import { writePositionDeleteFile } from './delete-file.js'
import { writeParquet } from './parquet.js'
import { writeDataManifest, writeDeleteManifest, writeExistingDeleteManifest } from './manifest.js'
import { writeManifestList } from './manifest-list.js'
import { groupByPartition } from './partition.js'
import { computeColumnStats, computeFieldSummary } from './stats.js'
import { transformResultType } from './transform.js'

/**
 * @import {CompressionCodec} from 'hyparquet'
 * @import {DataFile, FieldSummary, Manifest, ManifestEntry, PartitionSpec, Resolver, Schema, Snapshot, StagedUpdate, TableMetadata, TableRequirement, TableUpdate} from '../../src/types.js'
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
  const partitionSpec = metadata['partition-specs'].find(s => s['spec-id'] === metadata['default-spec-id'])
  if (!partitionSpec) throw new Error('default partition spec not found in metadata')
  const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('current schema not found in metadata')
  validateSchemaForVersion(schema, formatVersion)

  const snapshotId = newSnapshotId()
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

  // 4. Distinct partition tuples touched (across specs) — for changed-partition-count.
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
 * Stage a `set-snapshot-ref` update to point a branch or tag at an existing
 * snapshot. The same primitive powers rollback (set `main` to a prior
 * snapshot), tagging, fast-forwarding feature branches, and adjusting ref
 * retention.
 *
 * Pure — produces a `StagedUpdate` to pass into a commit function
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
 * table. Pure — produces a `StagedUpdate` to pass into a commit function.
 * Validates that every id exists, that none are referenced by a branch or
 * tag, and that the legacy `current-snapshot-id` is not in the removal list.
 *
 * Data files belonging to expired snapshots are not deleted from storage
 * here — that is the responsibility of a separate maintenance pass that can
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
 * Fetch manifest entries and apply the same inheritance rules used by reads.
 *
 * @param {Manifest} manifest
 * @param {Resolver} resolver
 * @returns {Promise<ManifestEntry[]>}
 */
async function loadManifestEntries(manifest, resolver) {
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
 * Carry forward priors from the current snapshot, prepend the new manifests,
 * assign v3 row IDs across the combined list, write the new manifest list,
 * and assemble the Snapshot + StagedUpdate. Each caller still builds its own
 * `summary` (the per-operation counters differ); everything else around it is
 * shared.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata
 * @param {Resolver} options.resolver
 * @param {bigint} options.snapshotId
 * @param {bigint} options.sequenceNumber
 * @param {string} options.manifestUuid
 * @param {number} options.timestampMs
 * @param {2|3} options.formatVersion
 * @param {Manifest[]} options.newManifests - Prepended to priors before write.
 * @param {Snapshot['summary']} options.summary - Caller-built operation summary.
 * @param {string[]} options.writtenFiles - Files this stage already wrote (data, manifests).
 * @param {Manifest[]} [options.priorManifests] - Already loaded prior manifests.
 * @param {Set<string>} [options.skipPriorManifestPaths] - Prior manifests to omit from the new list.
 * @returns {Promise<StagedUpdate>}
 */
async function buildSnapshotUpdate({
  tableUrl, metadata, resolver,
  snapshotId, sequenceNumber, manifestUuid, timestampMs, formatVersion,
  newManifests, summary, writtenFiles, priorManifests, skipPriorManifestPaths,
}) {
  const writerFn = resolver.writer
  if (!writerFn) throw new Error('resolver.writer is required')
  const rowLineage = formatVersion >= 3
  const firstRowId = rowLineage ? BigInt(metadata['next-row-id'] ?? 0) : 0n

  priorManifests ??= await loadPriorManifests(metadata, resolver)
  if (skipPriorManifestPaths?.size) {
    priorManifests = priorManifests.filter(manifest => !skipPriorManifestPaths.has(manifest.manifest_path))
  }
  // Append the new manifests after priors so reads preserve append order.
  // Iceberg's scan semantics don't pin an order, but our Promise.all scanner
  // returns rows in dataEntries order, which is manifest-list order.
  const allManifests = [...priorManifests, ...newManifests]
  const addedRows = rowLineage ? assignFirstRowIds(allManifests, firstRowId) : 0n
  const manifestListPath = `${tableUrl}/metadata/snap-${snapshotId}-1-${manifestUuid}.avro`
  const listWriter = writerFn(translateS3Url(manifestListPath))
  await writeManifestList({ writer: listWriter, snapshotId, sequenceNumber, manifests: allManifests, formatVersion })

  /** @type {Snapshot} */
  const snapshot = {
    'snapshot-id': Number(snapshotId),
    'sequence-number': Number(sequenceNumber),
    'timestamp-ms': timestampMs,
    'manifest-list': manifestListPath,
    summary,
    'schema-id': metadata['current-schema-id'],
  }
  if (rowLineage) {
    snapshot['first-row-id'] = toMetadataLong(firstRowId)
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
    writtenFiles: [...writtenFiles, manifestListPath],
  }
}

/**
 * Build per-partition-field summaries (in spec order) from the partition
 * tuples of each data file. Values are already in the transform's
 * result-type form, so we serialize bounds against that type.
 *
 * @param {Record<string, any>[]} partitions
 * @param {Schema} schema
 * @param {PartitionSpec} partitionSpec
 * @returns {FieldSummary[]}
 */
function buildPartitionSummaries(partitions, schema, partitionSpec) {
  return partitionSpec.fields.map(pf => {
    const sourceField = schema.fields.find(f => f.id === pf['source-id'])
    if (!sourceField) throw new Error(`partition source field id ${pf['source-id']} not found`)
    const resultType = transformResultType(pf.transform, sourceField.type)
    const values = partitions.map(p => p[pf.name])
    return computeFieldSummary(values, resultType)
  })
}

/**
 * Reject `write.format.default` values other than parquet. Iceberg also
 * defines `avro` and `orc`, but Icebird only writes parquet today.
 *
 * @param {string|undefined} value
 */
function checkWriteFormat(value) {
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
function resolveParquetCodec(value) {
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
async function findDataFileEntries(metadata, resolver) {
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
function partitionTupleKey(partition) {
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
  if (v instanceof Uint8Array) return `x:${[...v].map(b => b.toString(16).padStart(2, '0')).join('')}`
  return `${typeof v}:${String(v)}`
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
