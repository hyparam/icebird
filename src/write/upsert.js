import { concat } from 'hyparquet/src/utils.js'
import { deleteFileAppliesToDataEntry } from '../delete.js'
import { fetchDeleteMaps } from '../fetch.js'
import { icebergManifests, splitManifestEntries } from '../manifest.js'
import { fileMightMatch, partitionMightMatch } from '../prune.js'
import { readDataFile } from '../read.js'
import { uuid4 } from '../utils.js'
import { buildSnapshotUpdate, currentSnapshot } from './snapshot.js'
import { prepareAppend, preparedManifest, resolveParquetCodec } from './stage.js'
import { partitionTupleKey, writePositionDeletes } from './stage-position-delete.js'

/**
 * @import {DataFile, Field, ManifestEntry, Resolver, Schema, Snapshot, StagedUpdate, TableMetadata} from '../../src/types.js'
 */

/**
 * Stage an upsert against the current snapshot as one `overwrite` snapshot:
 * position deletes for every live row whose `joinCols` values match an
 * incoming record, plus an append of all `records`. The commit is guarded by
 * the loaded table uuid, `main` snapshot, current schema, default partition
 * spec, and default sort order, so it fails rather than duplicating rows if
 * the table changed after it was scanned.
 *
 * Format-version 2 only. Join columns default to the schema's
 * `identifier-field-ids` and must be top-level `int`, `long`, or `string`
 * fields; every record needs a non-null value for each, and no two records
 * may share a key.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata
 * @param {Record<string, any>[]} options.records
 * @param {string[]} [options.joinCols] - Defaults to the schema's identifier fields.
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @returns {Promise<StagedUpdate>}
 */
export async function icebergStageUpsert({ tableUrl, metadata, records, joinCols, resolver }) {
  if (metadata['format-version'] !== 2) {
    throw new Error(`upsert supports format-version 2 only, got ${metadata['format-version']}`)
  }
  const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('current schema not found in metadata')
  const keyFields = resolveJoinFields(schema, joinCols)
  /** @type {Set<string>} */
  const keys = new Set()
  for (const record of records) {
    for (const field of keyFields) {
      const value = record[field.name]
      if (value === undefined || value === null) throw new Error(`record is missing join column ${field.name}`)
      if (!keyValueValid(value, field.type)) {
        throw new Error(`invalid ${field.type} value for join column ${field.name}: ${String(value)}`)
      }
    }
    const key = encodeKey(keyFields.map(f => record[f.name]))
    if (keys.has(key)) throw new Error(`duplicate join key in records: ${key}`)
    keys.add(key)
  }

  const { deletes, candidates } = await findMatchingRows({ metadata, resolver, schema, keyFields, records, keys })
  // Write the data first: a record the writer rejects leaves no delete files.
  const prepared = await prepareAppend({ tableUrl, metadata, records, resolver })
  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const newManifests = [preparedManifest(prepared, sequenceNumber)]
  const writtenFiles = [...prepared.writtenFiles]
  /** @type {DataFile[]} */
  let deleteFiles = []
  // Distinct (spec-id, partition tuple) across new data and delete files.
  const partitionKeys = new Set(prepared.dataFilePartitions.map(p => `${prepared.partitionSpecId}|${partitionTupleKey(p)}`))
  if (deletes.length) {
    const partitionMap = new Map(candidates.map(e => [e.data_file.file_path, {
      partition: e.data_file.partition ?? {}, partitionSpecId: e.partition_spec_id ?? 0,
    }]))
    const written = await writePositionDeletes({
      tableUrl, metadata, schema, deletes, partitionMap, resolver,
      snapshotId: prepared.snapshotId, sequenceNumber, manifestUuid: uuid4(),
      codec: resolveParquetCodec(metadata.properties?.['write.parquet.compression-codec']),
    })
    deleteFiles = written.deleteFiles
    for (const key of written.partitionKeys) partitionKeys.add(key)
    newManifests.push(...written.newManifests)
    writtenFiles.push(...written.writtenFiles)
  }

  const prevSummary = currentSnapshot(metadata)?.summary
  const prevTotals = {
    records: BigInt(prevSummary?.['total-records'] ?? '0'),
    size: BigInt(prevSummary?.['total-files-size'] ?? '0'),
    dataFiles: BigInt(prevSummary?.['total-data-files'] ?? '0'),
    deleteFiles: BigInt(prevSummary?.['total-delete-files'] ?? '0'),
    posDeletes: BigInt(prevSummary?.['total-position-deletes'] ?? '0'),
    eqDeletes: BigInt(prevSummary?.['total-equality-deletes'] ?? '0'),
  }
  const deleteSize = deleteFiles.reduce((sum, f) => sum + f.file_size_in_bytes, 0n)
  const posDeletes = BigInt(deletes.length)
  const addedSize = prepared.addedFilesSize + deleteSize
  /** @type {Snapshot['summary']} */
  const summary = {
    operation: 'overwrite',
    'added-data-files': String(prepared.addedDataFilesCount),
    'added-records': String(prepared.recordsCount),
    'added-delete-files': String(deleteFiles.length),
    'added-position-deletes': String(posDeletes),
    'changed-partition-count': String(partitionKeys.size),
    'added-files-size': String(addedSize),
    'total-records': String(prevTotals.records + BigInt(prepared.recordsCount)),
    'total-files-size': String(prevTotals.size + addedSize),
    'total-data-files': String(prevTotals.dataFiles + BigInt(prepared.addedDataFilesCount)),
    'total-delete-files': String(prevTotals.deleteFiles + BigInt(deleteFiles.length)),
    'total-position-deletes': String(prevTotals.posDeletes + posDeletes),
    'total-equality-deletes': String(prevTotals.eqDeletes),
  }
  const staged = await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId: prepared.snapshotId, sequenceNumber, manifestUuid: prepared.manifestUuid,
    timestampMs: Date.now(), formatVersion: 2, newManifests, summary, writtenFiles,
  })
  staged.requirements.push(
    { type: 'assert-current-schema-id', 'current-schema-id': metadata['current-schema-id'] },
    { type: 'assert-default-spec-id', 'default-spec-id': metadata['default-spec-id'] },
    { type: 'assert-default-sort-order-id', 'default-sort-order-id': metadata['default-sort-order-id'] }
  )
  return staged
}

/**
 * @param {Schema} schema
 * @param {string[] | undefined} joinCols
 * @returns {Field[]}
 */
function resolveJoinFields(schema, joinCols) {
  joinCols ??= (schema['identifier-field-ids'] ?? []).map(id => {
    const field = schema.fields.find(f => f.id === id)
    if (!field) throw new Error(`identifier field id ${id} is not a top-level column`)
    return field.name
  })
  if (!joinCols.length) throw new Error('joinCols is required when the schema has no identifier-field-ids')
  return joinCols.map(name => {
    const field = schema.fields.find(f => f.name === name)
    if (!field) throw new Error(`join column not found in current schema: ${name}`)
    if (field.type !== 'int' && field.type !== 'long' && field.type !== 'string') {
      throw new Error(`unsupported join column type for ${name}: ${JSON.stringify(field.type)}`)
    }
    return field
  })
}

/**
 * @param {any} value
 * @param {Field['type']} type
 * @returns {boolean}
 */
function keyValueValid(value, type) {
  if (type === 'string') return typeof value === 'string'
  if (type === 'long') return typeof value === 'bigint'
  return Number.isInteger(value) && value >= -(2 ** 31) && value < 2 ** 31
}

/**
 * @param {any[]} values
 * @returns {string}
 */
function encodeKey(values) {
  return JSON.stringify(values.map(v => typeof v === 'bigint' ? v.toString() : v))
}

/**
 * Find the physical positions of live rows matching an incoming key, reading
 * candidate files the same way `icebergRead` does (applicable deletes, field
 * id projection, partition values and defaults for columns not in the file).
 *
 * @param {object} options
 * @param {TableMetadata} options.metadata
 * @param {Resolver} options.resolver
 * @param {Schema} options.schema
 * @param {Field[]} options.keyFields
 * @param {Record<string, any>[]} options.records
 * @param {Set<string>} options.keys
 * @returns {Promise<{deletes: {file_path: string, pos: bigint}[], candidates: ManifestEntry[]}>}
 */
async function findMatchingRows({ metadata, resolver, schema, keyFields, records, keys }) {
  if (currentSnapshot(metadata) === undefined || !records.length) return { deletes: [], candidates: [] }
  /** @type {Record<string, {$in: any[]}>} */
  const filter = {}
  for (const field of keyFields) {
    filter[field.name] = { $in: [...new Set(records.map(r => r[field.name]))] }
  }
  const manifests = await icebergManifests({ metadata, resolver, filter })
  const { dataEntries, deleteEntries } = splitManifestEntries(manifests)
  const candidates = dataEntries.filter(entry =>
    partitionMightMatch(filter, entry, schema, metadata) && fileMightMatch(filter, entry, schema))
  // Only fetch delete files that apply to a candidate.
  const applicable = deleteEntries.filter(deleteEntry => candidates.some(entry => deleteFileAppliesToDataEntry(
    entry, deleteEntry, metadata, deleteEntry.data_file.content === 2 ? 'equality' : 'position')))
  const { positionDeletesMap, equalityDeleteGroups } = await fetchDeleteMaps(applicable, resolver)
  const wantedColumns = keyFields.map(f => f.name)

  const perFile = await Promise.all(candidates.map(async entry => {
    /** @type {{file_path: string, pos: bigint}[]} */
    const deletes = []
    for await (const rows of readDataFile({
      dataEntry: entry,
      fileRowStart: 0,
      fileRowEnd: Number(entry.data_file.record_count),
      schema, metadata, resolver,
      rowLineage: false,
      positionDeletesMap, equalityDeleteGroups, wantedColumns, filter,
      positions: true,
    })) {
      for (const row of rows) {
        // A long column promoted from int reads as a number.
        const values = keyFields.map(f => f.type === 'long' && typeof row[f.name] === 'number' ? BigInt(row[f.name]) : row[f.name])
        if (keys.has(encodeKey(values))) deletes.push({ file_path: entry.data_file.file_path, pos: row._pos })
      }
    }
    return deletes
  }))
  /** @type {{file_path: string, pos: bigint}[]} */
  const deletes = []
  for (const d of perFile) concat(deletes, d)
  return { deletes, candidates }
}
