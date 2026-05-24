import { maxFieldId, validateSchemaForVersion } from '../schema.js'
import { parseDecimalType } from './conversions.js'
import { validatePartitionSpecForWrite } from './partition.js'

/**
 * @import {Field, IcebergType, PartitionSpec, Resolver, Schema, SnapshotRef, SortOrder, StagedUpdate, TableMetadata, TableRequirement, TableUpdate} from '../../src/types.js'
 */

/**
 * Commit a `StagedUpdate` against a file-based catalog: verify requirements
 * against the current metadata, apply updates, and write the next
 * `vN.metadata.json` and `version-hint.text`.
 *
 * Without `conditionalCommits`, this overwrites.
 * A second writer racing against this one can clobber the metadata file.
 *
 * With `conditionalCommits`, the new metadata file is created with
 * `If-None-Match: *`. A second writer racing for the same `vN+1` sees a
 * 412/409 from the resolver and the error surfaces to the caller (this
 * slice does not yet retry). `version-hint.text` becomes a best-effort
 * cache for all file-catalog commits: it is still written after the
 * metadata file, but a failed hint write does not invalidate the commit.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata - Current metadata, used for the CAS check.
 * @param {string} [options.metadataFileName] - Actual filename the metadata
 *   was loaded from. Recorded verbatim in the `metadata-log` entry for the
 *   prior version so rollback / log walks land on a real file even when the
 *   prior writer used `NNNNN-<uuid>.metadata.json` instead of `vN.metadata.json`.
 * @param {number} [options.currentVersion] - If known, the on-disk version of `metadata`. Bypasses deriving from `metadata-log`, which can be empty/stale on foreign-written tables.
 * @param {StagedUpdate} options.staged
 * @param {Resolver} options.resolver
 * @param {boolean} [options.conditionalCommits] - When true, write the metadata file with `ifNoneMatch: '*'`.
 * @returns {Promise<TableMetadata>} The new metadata, already persisted.
 */
export async function fileCatalogCommit({ tableUrl, metadata, metadataFileName, currentVersion, staged, resolver, conditionalCommits }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (!resolver?.writer) throw new Error('resolver.writer is required')

  checkRequirements(metadata, staged.requirements)
  const hasSnapshotUpdate = staged.updates.some(up => up.action === 'add-snapshot')
  const baseMetadata = hasSnapshotUpdate
    ? metadata
    : { ...metadata, 'last-updated-ms': Date.now() }
  const updated = applyUpdates(baseMetadata, staged.updates)

  const priorMetadataLog = metadata['metadata-log'] ?? []
  const derivedVersion = currentVersion ?? deriveCurrentVersion(priorMetadataLog)
  const newVersion = derivedVersion + 1
  const currentMetadataPath = metadataFileName
    ? `${tableUrl}/metadata/${metadataFileName}`
    : `${tableUrl}/metadata/v${derivedVersion}.metadata.json`
  const newMetadataPath = `${tableUrl}/metadata/v${newVersion}.metadata.json`

  const appendedLog = [
    ...priorMetadataLog,
    { 'timestamp-ms': metadata['last-updated-ms'], 'metadata-file': currentMetadataPath },
  ]
  const max = Number(updated.properties?.['write.metadata.previous-versions-max'] ?? 100)
  const droppedLog = max > 0 && appendedLog.length > max
    ? appendedLog.slice(0, appendedLog.length - max)
    : []
  const trimmedLog = droppedLog.length > 0 ? appendedLog.slice(-max) : appendedLog

  /** @type {TableMetadata} */
  const newMetadata = {
    ...updated,
    'metadata-log': trimmedLog,
  }

  // Metadata file creation is the commit point. With conditionalCommits on,
  // a second writer racing for the same v<newVersion> sees a 412/409 from
  // the resolver; this slice surfaces that error to the caller.
  const metaWriter = conditionalCommits
    ? resolver.writer(newMetadataPath, { ifNoneMatch: '*' })
    : resolver.writer(newMetadataPath)
  metaWriter.appendBytes(new TextEncoder().encode(JSON.stringify(newMetadata, null, 2)))
  await metaWriter.finish()

  // version-hint last so a partial write doesn't surface a torn commit.
  // The hint is a cache: a failed hint write does not invalidate the durable
  // v<newVersion>.metadata.json above.
  try {
    const hintWriter = resolver.writer(`${tableUrl}/metadata/version-hint.text`)
    hintWriter.appendBytes(new TextEncoder().encode(String(newVersion)))
    await hintWriter.finish()
  } catch { /* version-hint.text is best-effort. */ }

  // Best-effort cleanup of metadata files dropped from the log when the
  // table opts in via `write.metadata.delete-after-commit.enabled`. Failures
  // (404, permission, etc.) must not surface; the commit itself succeeded.
  const deleteEnabled = updated.properties?.['write.metadata.delete-after-commit.enabled'] === 'true'
  if (deleteEnabled && droppedLog.length > 0 && resolver.deleter) {
    const { deleter } = resolver
    await Promise.allSettled(droppedLog.map(entry => deleter(entry['metadata-file'])))
  }

  return newMetadata
}

/**
 * Derive the version number of the metadata being committed by inspecting
 * the most recent `metadata-log` entry's filename. Two naming conventions
 * are accepted:
 * - `vN.metadata.json`: what Icebird itself writes; numbering starts at 1.
 * - `NNNNN-<uuid>.metadata.json`: iceberg-java / iceberg-rust / pyiceberg;
 *   numbering starts at 0.
 * Both shapes carry the prior version explicitly, so the new current version
 * is just `prior + 1`. Falls back to `length + 1` for unrecognized filenames
 * to preserve legacy behavior.
 *
 * @param {{ 'timestamp-ms': number, 'metadata-file': string }[]} priorMetadataLog
 * @returns {number}
 */
function deriveCurrentVersion(priorMetadataLog) {
  if (priorMetadataLog.length === 0) return 1
  const last = priorMetadataLog[priorMetadataLog.length - 1]['metadata-file']
  const basename = last.split('/').pop() ?? ''
  const match = basename.match(/^(?:v(\d+)|0*(\d+)-[0-9a-f-]+)\.metadata\.json$/)
  if (match) return Number(match[1] ?? match[2]) + 1
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
      /** @type {number | bigint | null} */
      let current = refs[req.ref]?.['snapshot-id'] ?? null
      // legacy tables may have current-snapshot-id without a populated refs.main
      if (current === null && req.ref === 'main') {
        current = metadata['current-snapshot-id'] ?? null
      }
      const expected = req['snapshot-id']
      const matches = current === expected
        || current != null && expected != null && BigInt(current) === BigInt(expected)
      if (!matches) {
        throw new Error(`requirement failed: ref ${req.ref} expected snapshot ${expected}, got ${current}`)
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
 * Apply updates to produce the next metadata. Pure; no I/O.
 *
 * Setting `main` (a branch ref) also bumps `current-snapshot-id` and appends
 * to `snapshot-log`, matching server behaviour described in the spec.
 *
 * For `add-schema` / `set-current-schema` the spec sentinel `schema-id: -1`
 * is supported; `add-schema` assigns the next free id, and
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
      const priorSnapshots = next.snapshots ?? []
      if (priorSnapshots.some(s => s['snapshot-id'] === snap['snapshot-id'])) {
        throw new Error(`add-snapshot: snapshot-id ${snap['snapshot-id']} already exists`)
      }
      next = {
        ...next,
        snapshots: [...priorSnapshots, snap],
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
      const priorAssignedIds = currentAssignedIdIndex(schemas, next['current-schema-id'])
      validateAssignedFieldIds(newSchema, priorAssignedIds, priorLastColumnId)
      validateSchemaEvolution(schemas, newSchema, priorLastColumnId, next['format-version'])
      validateNewRequiredFields(newSchema, priorLastColumnId)
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
    } else if (up.action === 'add-spec') {
      const specs = next['partition-specs'] ?? []
      let specId = up.spec['spec-id']
      if (specId === -1) {
        specId = specs.reduce((m, s) => Math.max(m, s['spec-id']), -1) + 1
      } else if (specs.some(s => s['spec-id'] === specId)) {
        throw new Error(`add-spec: spec-id ${specId} already exists`)
      }
      /** @type {PartitionSpec} */
      const newSpec = { ...up.spec, 'spec-id': specId }
      const currentSchema = currentSchemaForMetadata(next)
      validatePartitionSpecEvolution(specs, newSpec, currentSchema)
      const priorLastPartitionId = next['last-partition-id'] ?? 0
      let nextLastPartitionId = priorLastPartitionId
      for (const f of newSpec.fields) {
        if (f['field-id'] > nextLastPartitionId) nextLastPartitionId = f['field-id']
      }
      next = {
        ...next,
        'partition-specs': [...specs, newSpec],
        'last-partition-id': nextLastPartitionId,
      }
    } else if (up.action === 'set-default-spec') {
      let id = up['spec-id']
      const specs = next['partition-specs'] ?? []
      if (id === -1) {
        if (specs.length === 0) throw new Error('set-default-spec: table has no partition specs')
        id = specs[specs.length - 1]['spec-id']
      } else if (!specs.some(s => s['spec-id'] === id)) {
        throw new Error(`set-default-spec: spec-id ${id} not found`)
      }
      next = { ...next, 'default-spec-id': id }
    } else if (up.action === 'remove-snapshots') {
      const removeIds = new Set(up['snapshot-ids'])
      const snapshots = (next.snapshots ?? []).filter(s => !removeIds.has(s['snapshot-id']))
      // Per spec: when snapshots are expired, drop snapshot-log entries that
      // reference them. The remaining log keeps the linear history of the
      // surviving snapshots.
      const log = (next['snapshot-log'] ?? []).filter(e => !removeIds.has(e['snapshot-id']))
      next = { ...next, snapshots, 'snapshot-log': log }
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
 * @typedef {{ kind: string, path: string }} AssignedFieldId
 */

/**
 * @param {Schema[]} schemas
 * @param {number} currentSchemaId
 * @returns {Map<number, AssignedFieldId>}
 */
function currentAssignedIdIndex(schemas, currentSchemaId) {
  const currentSchema = schemas.find(s => s['schema-id'] === currentSchemaId) ?? schemas[schemas.length - 1]
  const assignedIds = new Map()
  if (currentSchema) indexAssignedFieldIds(currentSchema.fields, '', assignedIds)
  return assignedIds
}

/**
 * @param {Field[]} fields
 * @param {string} prefix
 * @param {Map<number, AssignedFieldId>} assignedIds
 */
function indexAssignedFieldIds(fields, prefix, assignedIds) {
  for (const field of fields) {
    const path = prefix ? `${prefix}.${field.name}` : field.name
    assignedIds.set(field.id, { kind: 'field', path })
    indexAssignedTypeIds(field.type, path, assignedIds)
  }
}

/**
 * @param {IcebergType} type
 * @param {string} path
 * @param {Map<number, AssignedFieldId>} assignedIds
 */
function indexAssignedTypeIds(type, path, assignedIds) {
  if (typeof type === 'string') return
  if (type.type === 'struct') {
    indexAssignedFieldIds(type.fields, path, assignedIds)
  } else if (type.type === 'list') {
    assignedIds.set(type['element-id'], { kind: 'list element', path: `${path}.element` })
    indexAssignedTypeIds(type.element, `${path}.element`, assignedIds)
  } else if (type.type === 'map') {
    assignedIds.set(type['key-id'], { kind: 'map key', path: `${path}.key` })
    assignedIds.set(type['value-id'], { kind: 'map value', path: `${path}.value` })
    indexAssignedTypeIds(type.key, `${path}.key`, assignedIds)
    indexAssignedTypeIds(type.value, `${path}.value`, assignedIds)
  }
}

/**
 * @param {Schema} schema
 * @param {Map<number, AssignedFieldId>} priorAssignedIds
 * @param {number} priorLastColumnId
 */
function validateAssignedFieldIds(schema, priorAssignedIds, priorLastColumnId) {
  validateAssignedFields(schema.fields, '', priorAssignedIds, priorLastColumnId)
}

/**
 * @param {Field[]} fields
 * @param {string} prefix
 * @param {Map<number, AssignedFieldId>} priorAssignedIds
 * @param {number} priorLastColumnId
 */
function validateAssignedFields(fields, prefix, priorAssignedIds, priorLastColumnId) {
  for (const field of fields) {
    const path = prefix ? `${prefix}.${field.name}` : field.name
    validateAssignedId(field.id, 'field', path, priorAssignedIds, priorLastColumnId)
    validateAssignedTypeIds(field.type, path, priorAssignedIds, priorLastColumnId)
  }
}

/**
 * @param {IcebergType} type
 * @param {string} path
 * @param {Map<number, AssignedFieldId>} priorAssignedIds
 * @param {number} priorLastColumnId
 */
function validateAssignedTypeIds(type, path, priorAssignedIds, priorLastColumnId) {
  if (typeof type === 'string') return
  if (type.type === 'struct') {
    validateAssignedFields(type.fields, path, priorAssignedIds, priorLastColumnId)
  } else if (type.type === 'list') {
    validateAssignedId(type['element-id'], 'list element', `${path}.element`, priorAssignedIds, priorLastColumnId)
    validateAssignedTypeIds(type.element, `${path}.element`, priorAssignedIds, priorLastColumnId)
  } else if (type.type === 'map') {
    validateAssignedId(type['key-id'], 'map key', `${path}.key`, priorAssignedIds, priorLastColumnId)
    validateAssignedId(type['value-id'], 'map value', `${path}.value`, priorAssignedIds, priorLastColumnId)
    validateAssignedTypeIds(type.key, `${path}.key`, priorAssignedIds, priorLastColumnId)
    validateAssignedTypeIds(type.value, `${path}.value`, priorAssignedIds, priorLastColumnId)
  }
}

/**
 * @param {number} id
 * @param {string} kind
 * @param {string} path
 * @param {Map<number, AssignedFieldId>} priorAssignedIds
 * @param {number} priorLastColumnId
 */
function validateAssignedId(id, kind, path, priorAssignedIds, priorLastColumnId) {
  if (id > priorLastColumnId) return
  const prior = priorAssignedIds.get(id)
  if (!prior) {
    throw new Error(`add-schema: ${kind} ${path} uses unassigned id ${id} (last-column-id ${priorLastColumnId})`)
  }
  if (prior.kind !== kind) {
    throw new Error(`add-schema: ${kind} ${path} uses id ${id} previously assigned to ${prior.kind} ${prior.path}`)
  }
}

/**
 * @param {Schema} schema
 * @param {number} priorLastColumnId
 */
function validateNewRequiredFields(schema, priorLastColumnId) {
  for (const field of schema.fields) {
    if (field.id > priorLastColumnId && field.required) {
      if (field['initial-default'] == null) {
        throw new Error(`add-schema: required field ${field.name} (id ${field.id}) needs a non-null initial-default`)
      }
      if (field['write-default'] == null) {
        throw new Error(`add-schema: required field ${field.name} (id ${field.id}) needs a non-null write-default`)
      }
    }
  }
}

/**
 * Validate schema evolution rules that require comparing the new schema with
 * prior schemas: existing field ids may be renamed/reordered, but their
 * immutable defaults and types must remain valid.
 *
 * @param {Schema[]} schemas
 * @param {Schema} newSchema
 * @param {number} priorLastColumnId
 * @param {number} formatVersion
 */
function validateSchemaEvolution(schemas, newSchema, priorLastColumnId, formatVersion) {
  for (const field of newSchema.fields) {
    if (field.id > priorLastColumnId) continue
    const prior = latestFieldById(schemas, field.id)
    if (!prior) continue
    if (!canPromoteType(prior.type, field.type, formatVersion)) {
      throw new Error(`add-schema: cannot promote field ${field.name} from ${typeToString(prior.type)} to ${typeToString(field.type)}`)
    }
    if (!defaultsEqual(prior['initial-default'], field['initial-default'])) {
      throw new Error(`add-schema: initial-default for field ${field.name} cannot change`)
    }
  }
}

/**
 * @param {Schema[]} schemas
 * @param {number} id
 * @returns {Field|undefined}
 */
function latestFieldById(schemas, id) {
  for (let i = schemas.length - 1; i >= 0; i--) {
    const field = schemas[i].fields.find(f => f.id === id)
    if (field) return field
  }
}

/**
 * @param {IcebergType} from
 * @param {IcebergType} to
 * @param {number} formatVersion
 * @returns {boolean}
 */
function canPromoteType(from, to, formatVersion) {
  if (typesEqual(from, to)) return true
  if (typeof from !== 'string' || typeof to !== 'string') return false
  if (formatVersion >= 3 && from === 'unknown') return true
  if (from === 'int' && to === 'long') return true
  if (from === 'float' && to === 'double') return true
  if (formatVersion >= 3 && from === 'date' && (to === 'timestamp' || to === 'timestamp_ns')) return true
  return decimalPromotionAllowed(from, to)
}

/**
 * @param {IcebergType} a
 * @param {IcebergType} b
 * @returns {boolean}
 */
function typesEqual(a, b) {
  if (typeof a === 'string' || typeof b === 'string') return a === b
  return JSON.stringify(a) === JSON.stringify(b)
}

/**
 * @param {string} from
 * @param {string} to
 * @returns {boolean}
 */
function decimalPromotionAllowed(from, to) {
  const a = parseDecimalType(from)
  const b = parseDecimalType(to)
  return Boolean(a && b && b.precision > a.precision && b.scale === a.scale)
}

/**
 * @param {IcebergType} type
 * @returns {string}
 */
function typeToString(type) {
  return typeof type === 'string' ? type : JSON.stringify(type)
}

/**
 * @param {any} a
 * @param {any} b
 * @returns {boolean}
 */
function defaultsEqual(a, b) {
  if (Object.is(a, b)) return true
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object') return false
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false
    for (let i = 0; i < a.length; i++) {
      if (!defaultsEqual(a[i], b[i])) return false
    }
    return true
  }
  const aKeys = Object.keys(a)
  const bKeys = Object.keys(b)
  if (aKeys.length !== bKeys.length) return false
  for (const key of aKeys) {
    if (!Object.hasOwn(b, key)) return false
    if (!defaultsEqual(a[key], b[key])) return false
  }
  return true
}

/**
 * @param {PartitionSpec[]} specs
 * @param {PartitionSpec} newSpec
 * @param {Schema} schema
 */
function validatePartitionSpecEvolution(specs, newSpec, schema) {
  validateWritablePartitionSpec(newSpec, schema)
  if (specs.some(spec => partitionSpecsEquivalent(spec, newSpec))) {
    throw new Error('add-spec: equivalent partition spec already exists')
  }
  for (const field of newSpec.fields) {
    const equivalent = equivalentPartitionField(specs, field)
    if (equivalent && equivalent['field-id'] !== field['field-id']) {
      throw new Error(`add-spec: partition field ${field.name} must reuse field-id ${equivalent['field-id']}`)
    }
  }
}

/**
 * @param {PartitionSpec} spec
 * @param {Schema} schema
 */
function validateWritablePartitionSpec(spec, schema) {
  try {
    validatePartitionSpecForWrite(schema, spec, 'add-spec')
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    if (message.startsWith('unsupported partition transform: ')) {
      throw new Error(`add-spec: ${message}`)
    }
    throw err
  }
}

/**
 * @param {TableMetadata} metadata
 * @returns {Schema}
 */
function currentSchemaForMetadata(metadata) {
  const schema = metadata.schemas?.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('add-spec: current schema not found in metadata')
  return schema
}

/**
 * @param {PartitionSpec[]} specs
 * @param {PartitionSpec['fields'][number]} field
 * @returns {PartitionSpec['fields'][number]|undefined}
 */
function equivalentPartitionField(specs, field) {
  for (const spec of specs) {
    const found = spec.fields.find(existing => partitionFieldsEquivalent(existing, field))
    if (found) return found
  }
}

/**
 * @param {PartitionSpec} a
 * @param {PartitionSpec} b
 * @returns {boolean}
 */
function partitionSpecsEquivalent(a, b) {
  if (a.fields.length !== b.fields.length) return false
  for (let i = 0; i < a.fields.length; i++) {
    if (!partitionFieldsEquivalent(a.fields[i], b.fields[i])) return false
  }
  return true
}

/**
 * @param {PartitionSpec['fields'][number]} a
 * @param {PartitionSpec['fields'][number]} b
 * @returns {boolean}
 */
function partitionFieldsEquivalent(a, b) {
  return a['source-id'] === b['source-id'] &&
    idsListEquivalent(a['source-ids'], b['source-ids']) &&
    a.transform === b.transform &&
    a.name === b.name
}

/**
 * @param {number[]|undefined} a
 * @param {number[]|undefined} b
 * @returns {boolean}
 */
function idsListEquivalent(a, b) {
  if (a === undefined || b === undefined) return a === b
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}
