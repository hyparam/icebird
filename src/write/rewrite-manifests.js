import { fetchManifestEntries } from '../manifest.js'
import { typeName } from '../schema.js'
import { uuid4 } from '../utils.js'
import { writeCarriedManifest } from './manifest.js'
import { compare } from './serde.js'
import { buildPartitionSummaries, buildSnapshotUpdate, currentSnapshot, loadPriorManifests } from './snapshot.js'
import { newSnapshotId } from './stage.js'
import { transformResultType } from './transform.js'

/**
 * @import {IcebergType, Manifest, ManifestEntry, PartitionSpec, Resolver, Schema, Snapshot, StagedUpdate, TableMetadata} from '../../src/types.js'
 */

/**
 * Rows in a rewritten v3 data manifest whose files still lack a
 * `first_row_id`, keyed by the manifest record. Commit-time row id assignment
 * reserves only this many ids (spec "First Row ID Assignment") instead of the
 * manifest's added + existing rows, so carried files that already own ids do
 * not burn fresh id space on every rewrite.
 *
 * @type {WeakMap<Manifest, bigint>}
 */
export const unassignedRowCounts = new WeakMap()

const DEFAULT_TARGET_SIZE_BYTES = 8 * 1024 * 1024

/**
 * Manifests written by `prepareRewriteManifests`, reusable across commit
 * attempts while every manifest they replace is still in the table with
 * unchanged row-ID inheritance and table format version.
 *
 * @typedef {object} PreparedRewriteManifests
 * @property {bigint} snapshotId
 * @property {string} manifestUuid
 * @property {2|3} formatVersion
 * @property {Set<string>} replacedPaths - Data manifests superseded by the rewrite.
 * @property {Map<string, bigint | undefined>} replacedFirstRowIds - Row-ID inheritance used to decode each replaced manifest.
 * @property {Manifest[]} manifests - The rewritten manifests.
 * @property {number} entriesProcessed
 * @property {string[]} writtenFiles
 */

/**
 * Rewrite every data manifest of one partition spec into target-size
 * manifests clustered by partition value (Java's `RewriteManifests` action).
 * Live entries are sorted by partition tuple before packing, so each output
 * manifest covers a narrow partition range and its manifest-list partition
 * summary lets scans skip it. Entries are carried as EXISTING with their
 * original snapshot ids and data / file sequence numbers, so delete
 * applicability is unchanged; DELETED tombstones are dropped. Delete manifests
 * and manifests of other specs are kept as-is.
 *
 * The output manifest count is `ceil(total manifest bytes / targetSizeBytes)`
 * with entries spread evenly across them. All entries of the spec are held in
 * memory to sort them.
 *
 * Returns undefined when there is nothing to rewrite: no data manifest of the
 * spec, or a single manifest that would be rewritten as one.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata
 * @param {Resolver} options.resolver - Resolver with a writer method.
 * @param {number} [options.specId] - Partition spec to rewrite; defaults to `default-spec-id`.
 * @param {number} [options.targetSizeBytes] - Defaults to `commit.manifest.target-size-bytes` (8 MB).
 * @returns {Promise<PreparedRewriteManifests | undefined>}
 */
export async function prepareRewriteManifests({ tableUrl, metadata, resolver, specId, targetSizeBytes }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  if (!resolver?.writer) throw new Error('resolver.writer is required')
  if (metadata['format-version'] !== 2 && metadata['format-version'] !== 3) {
    throw new Error(`unsupported format-version: ${metadata['format-version']}`)
  }
  const formatVersion = /** @type {2|3} */ (metadata['format-version'])
  if (targetSizeBytes !== undefined && !(targetSizeBytes > 0)) {
    throw new Error('targetSizeBytes must be a positive number')
  }
  const target = targetSizeBytes ?? targetSizeProperty(metadata.properties)
  if (!currentSnapshot(metadata)) return undefined

  const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
  if (!schema) throw new Error('current schema not found in metadata')
  const rewriteSpecId = specId ?? metadata['default-spec-id']
  const spec = metadata['partition-specs'].find(s => s['spec-id'] === rewriteSpecId)
  if (!spec) throw new Error(`partition spec ${rewriteSpecId} not found in metadata`)
  if (!canRewriteSpec(schema, spec)) {
    throw new Error(`partition spec ${rewriteSpecId} cannot be rewritten losslessly (timestamp or decimal partition values)`)
  }

  const priors = await loadPriorManifests(metadata, resolver)
  const selected = priors.filter(m => (m.content ?? 0) === 0 && (m.partition_spec_id ?? 0) === rewriteSpecId)
  const totalBytes = selected.reduce((sum, m) => sum + Number(m.manifest_length), 0)
  const manifestCount = Math.max(1, Math.ceil(totalBytes / target))
  if (!selected.length || selected.length === 1 && manifestCount === 1) return undefined

  /** @type {ManifestEntry[]} */
  const entries = []
  for (const manifestEntries of await Promise.all(selected.map(m => fetchManifestEntries(m, resolver)))) {
    for (const entry of manifestEntries) {
      if (entry.status !== 2) entries.push({ ...entry, status: 0 })
    }
  }
  entries.sort(partitionComparator(schema, spec))

  const snapshotId = newSnapshotId(metadata)
  const manifestUuid = uuid4()
  // A placeholder sequence number: each commit attempt sets the real one on
  // the manifest list record. Entries are all EXISTING with explicit sequence
  // numbers, so the manifest files themselves never depend on it.
  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const perManifest = Math.max(1, Math.ceil(entries.length / manifestCount))
  /** @type {Manifest[]} */
  const manifests = []
  /** @type {string[]} */
  const writtenFiles = []
  for (let i = 0; i < entries.length; i += perManifest) {
    const manifestPath = `${tableUrl}/metadata/${manifestUuid}-m${manifests.length}.avro`
    manifests.push(await writeManifestFile({
      resolver, manifestPath, schema, spec, content: 0,
      entries: entries.slice(i, i + perManifest),
      snapshotId, sequenceNumber, formatVersion,
    }))
    writtenFiles.push(manifestPath)
  }

  return {
    snapshotId,
    manifestUuid,
    formatVersion,
    replacedPaths: new Set(selected.map(m => m.manifest_path)),
    replacedFirstRowIds: new Map(selected.map(m => [m.manifest_path, m.first_row_id == null ? undefined : BigInt(m.first_row_id)])),
    manifests,
    entriesProcessed: entries.length,
    writtenFiles,
  }
}

/**
 * Build the `replace` snapshot for prepared manifest rewrites against the
 * freshest metadata. Manifests committed concurrently are carried forward.
 * Returns undefined when a manifest the rewrite replaces is no longer in the
 * table, its inherited first row ID changed, or the table format version
 * changed, in which case the caller must prepare again against this metadata.
 *
 * @param {object} options
 * @param {string} options.tableUrl
 * @param {TableMetadata} options.metadata
 * @param {PreparedRewriteManifests} options.prepared
 * @param {Resolver} options.resolver
 * @returns {Promise<StagedUpdate | undefined>}
 */
export async function stageSnapshotForRewriteManifests({ tableUrl, metadata, prepared, resolver }) {
  if (metadata['format-version'] !== prepared.formatVersion) return undefined
  const priors = await loadPriorManifests(metadata, resolver)
  const priorsByPath = new Map(priors.map(m => [m.manifest_path, m]))
  for (const path of prepared.replacedPaths) {
    const prior = priorsByPath.get(path)
    if (!prior) return undefined
    // An append after a v3 upgrade can assign IDs without replacing the file.
    // Re-decode using the fresh inheritance before sorting the entries again.
    const firstRowId = prior.first_row_id == null ? undefined : BigInt(prior.first_row_id)
    if (firstRowId !== prepared.replacedFirstRowIds.get(path)) return undefined
  }
  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  for (const manifest of prepared.manifests) {
    manifest.sequence_number = sequenceNumber
    // IDs assigned by a failed staging attempt must be reserved again.
    // Manifests whose entries already own IDs retain their inherited IDs.
    if (unassignedRowCounts.has(manifest)) manifest.first_row_id = undefined
  }

  const prevSummary = currentSnapshot(metadata)?.summary
  /** @type {Snapshot['summary']} */
  const summary = {
    operation: 'replace',
    'manifests-created': String(prepared.manifests.length),
    'manifests-replaced': String(prepared.replacedPaths.size),
    'manifests-kept': String(priors.length - prepared.replacedPaths.size),
    'entries-processed': String(prepared.entriesProcessed),
  }
  for (const key of /** @type {const} */ (['total-records', 'total-files-size', 'total-data-files', 'total-delete-files', 'total-position-deletes', 'total-equality-deletes'])) {
    const value = prevSummary?.[key]
    if (value !== undefined) summary[key] = value
  }

  return await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId: prepared.snapshotId,
    sequenceNumber,
    manifestUuid: prepared.manifestUuid,
    timestampMs: Date.now(),
    formatVersion: prepared.formatVersion,
    newManifests: prepared.manifests,
    summary,
    writtenFiles: [],
    priorManifests: priors,
    skipPriorManifestPaths: prepared.replacedPaths,
  })
}

/**
 * Order entries by partition tuple in spec field order, nulls first.
 *
 * @param {Schema} schema
 * @param {PartitionSpec} spec
 * @returns {(a: ManifestEntry, b: ManifestEntry) => number}
 */
function partitionComparator(schema, spec) {
  const fields = spec.fields.map(pf => {
    const source = schema.fields.find(f => f.id === pf['source-id'])
    if (!source) throw new Error(`partition source field id ${pf['source-id']} not found`)
    return { name: pf.name, type: transformResultType(pf.transform, source.type) }
  })
  return (a, b) => {
    for (const { name, type } of fields) {
      const c = comparePartitionValue(a.data_file.partition?.[name], b.data_file.partition?.[name], type)
      if (c) return c
    }
    return 0
  }
}

/**
 * @param {any} a
 * @param {any} b
 * @param {IcebergType} type
 * @returns {number}
 */
function comparePartitionValue(a, b, type) {
  const aNull = a === null || a === undefined
  const bNull = b === null || b === undefined
  if (aNull || bNull) return aNull === bNull ? 0 : aNull ? -1 : 1
  try {
    const c = compare(a, b, type)
    if (!Number.isNaN(c)) return c
  } catch {
    // fall through to a stable textual order
  }
  const sa = String(a)
  const sb = String(b)
  return sa < sb ? -1 : sa > sb ? 1 : 0
}

/**
 * Whether entries of `spec` can be decoded and re-encoded without changing
 * their partition values. The Avro reader decodes timestamps to millisecond
 * `Date`s and decimals to floats, so identity / truncate partitions over those
 * types would be silently altered by a rewrite; such specs are not rewritten.
 *
 * @param {Schema} schema
 * @param {PartitionSpec} spec
 * @returns {boolean}
 */
export function canRewriteSpec(schema, spec) {
  for (const pf of spec.fields) {
    const source = schema.fields.find(f => f.id === pf['source-id'])
    if (!source) return false
    let resultType
    try {
      resultType = typeName(transformResultType(pf.transform, source.type))
    } catch {
      return false
    }
    if (resultType.startsWith('timestamp') || resultType.startsWith('decimal(')) return false
  }
  return true
}

/**
 * Write one manifest of carried entries and build its manifest list record:
 * per-status file and row counts, the minimum data sequence number, partition
 * field summaries recomputed from the entries, and (v3 data manifests) a
 * `first_row_id` pinned to the smallest carried row id when every entry
 * already owns one. Otherwise `first_row_id` is left for commit-time
 * assignment, which hands ids only to the entries that lack them.
 *
 * @param {object} options
 * @param {Resolver} options.resolver
 * @param {string} options.manifestPath
 * @param {Schema} options.schema
 * @param {PartitionSpec} options.spec
 * @param {0|1} options.content
 * @param {ManifestEntry[]} options.entries - Statuses already decided.
 * @param {bigint} options.snapshotId
 * @param {bigint} options.sequenceNumber
 * @param {2|3} options.formatVersion
 * @returns {Promise<Manifest>}
 */
export async function writeManifestFile({
  resolver, manifestPath, schema, spec, content, entries, snapshotId, sequenceNumber, formatVersion,
}) {
  if (!resolver.writer) throw new Error('resolver.writer is required')
  const writer = resolver.writer(manifestPath)
  await writeCarriedManifest({ writer, schema, partitionSpec: spec, snapshotId, entries, content, formatVersion })

  const files = [0, 0, 0]
  const rows = [0n, 0n, 0n]
  let minSequenceNumber = sequenceNumber
  for (const entry of entries) {
    files[entry.status]++
    rows[entry.status] += BigInt(entry.data_file.record_count)
    // ADDED entries of this snapshot inherit `sequenceNumber`.
    if (entry.status !== 1 && entry.sequence_number != null && BigInt(entry.sequence_number) < minSequenceNumber) {
      minSequenceNumber = BigInt(entry.sequence_number)
    }
  }

  /** @type {Manifest} */
  const manifest = {
    manifest_path: manifestPath,
    manifest_length: BigInt(writer.offset),
    partition_spec_id: spec['spec-id'],
    content,
    sequence_number: sequenceNumber,
    min_sequence_number: minSequenceNumber,
    added_snapshot_id: snapshotId,
    added_files_count: files[1],
    existing_files_count: files[0],
    deleted_files_count: files[2],
    added_rows_count: rows[1],
    existing_rows_count: rows[0],
    deleted_rows_count: rows[2],
    partitions: buildPartitionSummaries(
      entries.map(entry => /** @type {Record<string, any>} */ (entry.data_file.partition ?? {})),
      schema,
      spec
    ),
  }
  if (formatVersion >= 3 && content === 0) {
    let unassigned = 0n
    /** @type {bigint | undefined} */
    let minRowId
    for (const entry of entries) {
      if (entry.status === 2) continue
      const id = entry.data_file.first_row_id
      if (id == null) unassigned += BigInt(entry.data_file.record_count)
      else if (minRowId === undefined || BigInt(id) < minRowId) minRowId = BigInt(id)
    }
    if (unassigned) unassignedRowCounts.set(manifest, unassigned)
    else if (minRowId !== undefined) manifest.first_row_id = minRowId
  }
  return manifest
}

/**
 * Read `commit.manifest.target-size-bytes`, defaulting to Java's 8 MB.
 *
 * @param {Record<string, string> | undefined} properties
 * @returns {number}
 */
function targetSizeProperty(properties) {
  const n = Number(properties?.['commit.manifest.target-size-bytes'])
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_TARGET_SIZE_BYTES
}
