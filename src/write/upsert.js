import { parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { applicablePositionDeletes } from '../delete.js'
import { fetchDeleteMaps, readParquetMetadata } from '../fetch.js'
import { icebergManifests, splitManifestEntries } from '../manifest.js'
import { fileMightMatch, partitionMightMatch } from '../prune.js'
import { fileIcebergSchema, tableNameMappings } from '../read.js'
import { sanitize } from '../utils.js'
import { applyUpdates } from './commit.js'
import { normalizeCurrentSnapshotId } from './snapshot.js'
import { icebergStageAppend } from './stage.js'
import { icebergStagePositionDelete } from './stage-position-delete.js'

/**
 * @import {Field, Resolver, Schema, StagedUpdate, TableMetadata, TableRequirement} from '../../src/types.js'
 */

/**
 * Stage an upsert against the current snapshot: stage position deletes for
 * every live row whose `joinCols` values match an incoming record, then
 * stage an append of all `records` on top. Both snapshots are guarded by the
 * loaded table uuid, `main` snapshot, current schema, default partition spec,
 * and default sort order, so the commit fails rather than duplicating rows
 * if the table changed after it was scanned.
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

  const deletes = await findMatchingRows({ metadata, resolver, schema, keyFields, records, keys })
  /** @type {TableRequirement[]} */
  const requirements = [
    { type: 'assert-table-uuid', uuid: metadata['table-uuid'] },
    { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': normalizeCurrentSnapshotId(metadata['current-snapshot-id']) },
    { type: 'assert-current-schema-id', 'current-schema-id': metadata['current-schema-id'] },
    { type: 'assert-default-spec-id', 'default-spec-id': metadata['default-spec-id'] },
    { type: 'assert-default-sort-order-id', 'default-sort-order-id': metadata['default-sort-order-id'] },
  ]
  if (!deletes.length) {
    const appended = await icebergStageAppend({ tableUrl, metadata, records, resolver })
    return { ...appended, requirements }
  }
  const deleted = await icebergStagePositionDelete({ tableUrl, metadata, deletes, resolver })
  const appended = await icebergStageAppend({ tableUrl, metadata: applyUpdates(metadata, deleted.updates), records, resolver })
  return {
    snapshot: appended.snapshot,
    requirements,
    updates: [...deleted.updates, ...appended.updates],
    writtenFiles: [...deleted.writtenFiles, ...appended.writtenFiles],
  }
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
  return Number.isInteger(value)
}

/**
 * @param {any[]} values
 * @returns {string}
 */
function encodeKey(values) {
  return JSON.stringify(values.map(v => typeof v === 'bigint' ? v.toString() : v))
}

/**
 * Find the physical positions of live rows matching an incoming key. Rows
 * are read without applying deletes so each row's index is its parquet
 * position; rows already removed by a position delete are skipped.
 *
 * @param {object} options
 * @param {TableMetadata} options.metadata
 * @param {Resolver} options.resolver
 * @param {Schema} options.schema
 * @param {Field[]} options.keyFields
 * @param {Record<string, any>[]} options.records
 * @param {Set<string>} options.keys
 * @returns {Promise<{file_path: string, pos: bigint}[]>}
 */
async function findMatchingRows({ metadata, resolver, schema, keyFields, records, keys }) {
  if (normalizeCurrentSnapshotId(metadata['current-snapshot-id']) === null || !records.length) return []
  /** @type {Record<string, {$in: any[]}>} */
  const filter = {}
  for (const field of keyFields) {
    filter[field.name] = { $in: [...new Set(records.map(r => r[field.name]))] }
  }
  const manifests = await icebergManifests({ metadata, resolver, filter })
  const { dataEntries, deleteEntries } = splitManifestEntries(manifests)
  const candidates = dataEntries.filter(entry =>
    partitionMightMatch(filter, entry, schema, metadata) && fileMightMatch(filter, entry, schema))
  if (!candidates.length) return []
  const { positionDeletesMap } = await fetchDeleteMaps(deleteEntries, resolver)
  const nameMappings = tableNameMappings(metadata)

  const perFile = await Promise.all(candidates.map(async entry => {
    const { file_path, file_size_in_bytes } = entry.data_file
    const deleted = applicablePositionDeletes(entry, positionDeletesMap.get(file_path), metadata)
    const file = await resolver.reader(file_path, Number(file_size_in_bytes))
    const parquetMetadata = await readParquetMetadata(file)
    // Resolve physical columns by field id, so renamed columns still match.
    const fileSchema = fileIcebergSchema(parquetMetadata, schema, nameMappings)
    const columns = keyFields.map(field => {
      const physical = fileSchema.fields.find(f => f.id === field.id)
      if (!physical) throw new Error(`join column ${field.name} not found in data file ${file_path}`)
      return sanitize(physical.name)
    })
    const rows = await parquetReadObjects({ file, metadata: parquetMetadata, columns, compressors, utf8: false })
    /** @type {{file_path: string, pos: bigint}[]} */
    const matches = []
    for (let i = 0; i < rows.length; i++) {
      const pos = BigInt(i)
      if (deleted.has(pos)) continue
      // A long column promoted from int stores INT32 values.
      const values = columns.map((column, c) => {
        const value = rows[i][column]
        return keyFields[c].type === 'long' && typeof value === 'number' ? BigInt(value) : value
      })
      if (keys.has(encodeKey(values))) matches.push({ file_path, pos })
    }
    return matches
  }))
  return perFile.flat()
}
