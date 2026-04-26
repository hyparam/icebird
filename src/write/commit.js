import { translateS3Url } from '../fetch.js'

/**
 * @import {Resolver, SnapshotRef, StagedUpdate, TableMetadata, TableRequirement, TableUpdate} from '../../src/types.js'
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
  const currentVersion = priorMetadataLog.length + 1
  const newVersion = currentVersion + 1
  const currentMetadataPath = `${tableUrl}/metadata/v${currentVersion}.metadata.json`
  const newMetadataPath = `${tableUrl}/metadata/v${newVersion}.metadata.json`

  /** @type {TableMetadata} */
  const newMetadata = {
    ...updated,
    'metadata-log': [
      ...priorMetadataLog,
      { 'timestamp-ms': metadata['last-updated-ms'], 'metadata-file': currentMetadataPath },
    ],
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
 * Verify each requirement against the current metadata. Throws on the first
 * mismatch with a message suitable for surfacing to the caller.
 *
 * @param {TableMetadata} metadata
 * @param {TableRequirement[]} requirements
 */
export function checkRequirements(metadata, requirements) {
  for (const req of requirements) {
    if (req.type === 'assert-table-uuid') {
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
