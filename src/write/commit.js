import { translateS3Url } from '../fetch.js'
import { validateSchemaForVersion } from '../schema.js'

/**
 * @import {Field, Resolver, Schema, SnapshotRef, SortOrder, StagedUpdate, TableMetadata, TableRequirement, TableUpdate} from '../../src/types.js'
 */

/**
 * Commit a `StagedUpdate` against a file-based catalog: verify requirements
 * against the current metadata, apply updates, and write the next
 * `vN.metadata.json` and `version-hint.text`.
 *
 * Note: this is not concurrency-safe. A second writer racing against this one
 * can clobber the metadata file. A safe-CAS variant (conditional PUT / rename)
 * is a future drop-in replacement.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata - Current metadata, used for the CAS check.
 * @param {StagedUpdate} options.staged
 * @param {Resolver} options.resolver
 * @returns {Promise<TableMetadata>} The new metadata, already persisted.
 */
export async function fileCatalogCommit({ tableUrl, metadata, staged, resolver }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (!resolver?.writer) throw new Error('resolver.writer is required')

  checkRequirements(metadata, staged.requirements)
  const updated = applyUpdates(metadata, staged.updates)

  const priorMetadataLog = metadata['metadata-log'] ?? []
  const currentVersion = deriveCurrentVersion(priorMetadataLog)
  const newVersion = currentVersion + 1
  const currentMetadataPath = `${tableUrl}/metadata/v${currentVersion}.metadata.json`
  const newMetadataPath = `${tableUrl}/metadata/v${newVersion}.metadata.json`

  const appendedLog = [
    ...priorMetadataLog,
    { 'timestamp-ms': metadata['last-updated-ms'], 'metadata-file': currentMetadataPath },
  ]
  const max = Number(updated.properties?.['write.metadata.previous-versions-max'] ?? 100)
  const trimmedLog = max > 0 && appendedLog.length > max
    ? appendedLog.slice(-max)
    : appendedLog

  /** @type {TableMetadata} */
  const newMetadata = {
    ...updated,
    'metadata-log': trimmedLog,
  }

  const metaWriter = resolver.writer(translateS3Url(newMetadataPath))
  metaWriter.appendBytes(new TextEncoder().encode(JSON.stringify(newMetadata, null, 2)))
  metaWriter.finish()

  // version-hint last so a partial write doesn't surface a torn commit
  const hintWriter = resolver.writer(translateS3Url(`${tableUrl}/version-hint.text`))
  hintWriter.appendBytes(new TextEncoder().encode(String(newVersion)))
  hintWriter.finish()

  return newMetadata
}

/**
 * Derive the version number of the metadata being committed. The numeric
 * `vN.metadata.json` convention pairs the version with the `metadata-log`
 * length only when the log has never been truncated; once truncation kicks
 * in (`write.metadata.previous-versions-max`), parse the latest log entry's
 * filename instead. Falls back to length+1 for non-`vN` filenames written
 * by other tools.
 *
 * @param {{ 'timestamp-ms': number, 'metadata-file': string }[]} priorMetadataLog
 * @returns {number}
 */
function deriveCurrentVersion(priorMetadataLog) {
  if (priorMetadataLog.length === 0) return 1
  const last = priorMetadataLog[priorMetadataLog.length - 1]['metadata-file']
  const match = last.match(/v(\d+)\.metadata\.json$/)
  if (match) return Number(match[1]) + 1
  return priorMetadataLog.length + 1
}

/**
 * Verify each requirement against the current metadata. Throws on the first
 * mismatch with a message suitable for surfacing to the caller.
 *
 * @param {TableMetadata} metadata
 * @param {TableRequirement[]} requirements
 */
export function checkRequirements(metadata, requirements) {
  for (const req of requirements) {
    if (req.type === 'assert-create') {
      // assert-create asserts the table does not yet exist; this commit path
      // always operates on existing metadata, so it can never be satisfied here
      throw new Error('requirement failed: assert-create against an existing table')
    } else if (req.type === 'assert-table-uuid') {
      if (metadata['table-uuid'] !== req.uuid) {
        throw new Error(`requirement failed: table-uuid expected ${req.uuid}, got ${metadata['table-uuid']}`)
      }
    } else if (req.type === 'assert-ref-snapshot-id') {
      const refs = metadata.refs ?? {}
      /** @type {number | null} */
      let current = refs[req.ref]?.['snapshot-id'] ?? null
      // legacy tables may have current-snapshot-id without a populated refs.main
      if (current === null && req.ref === 'main') {
        current = metadata['current-snapshot-id'] ?? null
      }
      if (current !== req['snapshot-id']) {
        throw new Error(`requirement failed: ref ${req.ref} expected snapshot ${req['snapshot-id']}, got ${current}`)
      }
    } else if (req.type === 'assert-next-row-id') {
      const current = Number(metadata['next-row-id'] ?? 0)
      if (current !== req['next-row-id']) {
        throw new Error(`requirement failed: next-row-id expected ${req['next-row-id']}, got ${current}`)
      }
    } else if (req.type === 'assert-current-schema-id') {
      const current = metadata['current-schema-id']
      if (current !== req['current-schema-id']) {
        throw new Error(`requirement failed: current-schema-id expected ${req['current-schema-id']}, got ${current}`)
      }
    } else if (req.type === 'assert-last-assigned-field-id') {
      const current = metadata['last-column-id']
      if (current !== req['last-assigned-field-id']) {
        throw new Error(`requirement failed: last-assigned-field-id expected ${req['last-assigned-field-id']}, got ${current}`)
      }
    } else if (req.type === 'assert-last-assigned-partition-id') {
      const current = metadata['last-partition-id']
      if (current !== req['last-assigned-partition-id']) {
        throw new Error(`requirement failed: last-assigned-partition-id expected ${req['last-assigned-partition-id']}, got ${current}`)
      }
    } else if (req.type === 'assert-default-spec-id') {
      const current = metadata['default-spec-id']
      if (current !== req['default-spec-id']) {
        throw new Error(`requirement failed: default-spec-id expected ${req['default-spec-id']}, got ${current}`)
      }
    } else if (req.type === 'assert-default-sort-order-id') {
      const current = metadata['default-sort-order-id']
      if (current !== req['default-sort-order-id']) {
        throw new Error(`requirement failed: default-sort-order-id expected ${req['default-sort-order-id']}, got ${current}`)
      }
    } else {
      throw new Error(`unknown requirement: ${JSON.stringify(req)}`)
    }
  }
}

/**
 * Apply updates to produce the next metadata. Pure — no I/O.
 *
 * Setting `main` (a branch ref) also bumps `current-snapshot-id` and appends
 * to `snapshot-log`, matching server behaviour described in the spec.
 *
 * For `add-schema` / `set-current-schema` the spec sentinel `schema-id: -1`
 * is supported — `add-schema` assigns the next free id, and
 * `set-current-schema` resolves to the most recently added schema.
 *
 * @param {TableMetadata} metadata
 * @param {TableUpdate[]} updates
 * @returns {TableMetadata}
 */
export function applyUpdates(metadata, updates) {
  /** @type {TableMetadata} */
  let next = { ...metadata }
  for (const up of updates) {
    if (up.action === 'add-snapshot') {
      const snap = up.snapshot
      next = {
        ...next,
        snapshots: [...next.snapshots ?? [], snap],
        'last-sequence-number': Math.max(next['last-sequence-number'] ?? 0, snap['sequence-number']),
        'last-updated-ms': snap['timestamp-ms'],
      }
      if (next['format-version'] >= 3 && snap['first-row-id'] !== undefined && snap['added-rows'] !== undefined) {
        const nextRowId = snap['first-row-id'] + snap['added-rows']
        next['next-row-id'] = Math.max(Number(next['next-row-id'] ?? 0), nextRowId)
      }
    } else if (up.action === 'set-properties') {
      next = { ...next, properties: { ...next.properties, ...up.updates } }
    } else if (up.action === 'remove-properties') {
      const properties = { ...next.properties }
      for (const key of up.removals) delete properties[key]
      next = { ...next, properties }
    } else if (up.action === 'add-schema') {
      const schemas = next.schemas ?? []
      let schemaId = up.schema['schema-id']
      if (schemaId === -1) {
        schemaId = schemas.reduce((m, s) => Math.max(m, s['schema-id']), -1) + 1
      } else if (schemas.some(s => s['schema-id'] === schemaId)) {
        throw new Error(`add-schema: schema-id ${schemaId} already exists`)
      }
      /** @type {Schema} */
      const newSchema = { ...up.schema, 'schema-id': schemaId }
      validateSchemaForVersion(newSchema, next['format-version'])
      const priorLastColumnId = next['last-column-id'] ?? 0
      for (const field of newSchema.fields) {
        if (field.id > priorLastColumnId && field.required && field['initial-default'] === undefined) {
          throw new Error(`add-schema: required field ${field.name} (id ${field.id}) needs an initial-default`)
        }
      }
      next = {
        ...next,
        schemas: [...schemas, newSchema],
        'last-column-id': Math.max(priorLastColumnId, maxFieldId(newSchema.fields)),
      }
    } else if (up.action === 'set-current-schema') {
      let id = up['schema-id']
      const schemas = next.schemas ?? []
      if (id === -1) {
        if (schemas.length === 0) throw new Error('set-current-schema: table has no schemas')
        id = schemas[schemas.length - 1]['schema-id']
      } else if (!schemas.some(s => s['schema-id'] === id)) {
        throw new Error(`set-current-schema: schema-id ${id} not found`)
      }
      next = { ...next, 'current-schema-id': id }
    } else if (up.action === 'add-sort-order') {
      const orders = next['sort-orders'] ?? []
      let orderId = up['sort-order']['order-id']
      if (orderId === -1) {
        orderId = orders.reduce((m, o) => Math.max(m, o['order-id']), -1) + 1
      } else if (orders.some(o => o['order-id'] === orderId)) {
        throw new Error(`add-sort-order: order-id ${orderId} already exists`)
      }
      /** @type {SortOrder} */
      const newOrder = { ...up['sort-order'], 'order-id': orderId }
      next = { ...next, 'sort-orders': [...orders, newOrder] }
    } else if (up.action === 'set-default-sort-order') {
      let id = up['sort-order-id']
      const orders = next['sort-orders'] ?? []
      if (id === -1) {
        if (orders.length === 0) throw new Error('set-default-sort-order: table has no sort orders')
        id = orders[orders.length - 1]['order-id']
      } else if (!orders.some(o => o['order-id'] === id)) {
        throw new Error(`set-default-sort-order: sort-order-id ${id} not found`)
      }
      next = { ...next, 'default-sort-order-id': id }
    } else if (up.action === 'set-snapshot-ref') {
      /** @type {SnapshotRef} */
      const ref = { 'snapshot-id': up['snapshot-id'], type: up.type }
      if (up['min-snapshots-to-keep'] !== undefined) ref['min-snapshots-to-keep'] = up['min-snapshots-to-keep']
      if (up['max-snapshot-age-ms'] !== undefined) ref['max-snapshot-age-ms'] = up['max-snapshot-age-ms']
      if (up['max-ref-age-ms'] !== undefined) ref['max-ref-age-ms'] = up['max-ref-age-ms']
      next = { ...next, refs: { ...next.refs, [up['ref-name']]: ref } }
      if (up['ref-name'] === 'main' && up.type === 'branch') {
        next['current-snapshot-id'] = up['snapshot-id']
        next['snapshot-log'] = [
          ...next['snapshot-log'] ?? [],
          { 'timestamp-ms': next['last-updated-ms'], 'snapshot-id': up['snapshot-id'] },
        ]
      }
    } else {
      throw new Error(`unknown update: ${JSON.stringify(up)}`)
    }
  }
  return next
}

/**
 * Highest field id at the top level of a schema. Mirrors `create.js` and is
 * intentionally non-recursive — nested-type ids are not yet supported on
 * schema add.
 *
 * @param {Field[]} fields
 * @returns {number}
 */
function maxFieldId(fields = []) {
  let max = 0
  for (const f of fields) {
    if (f.id > max) max = f.id
  }
  return max
}
