import { fetchAvroRecords, translateS3Url } from '../fetch.js'
import { writeManifestList } from './manifest-list.js'
import { computeFieldSummary } from './stats.js'
import { transformResultType } from './transform.js'

/**
 * @import {FieldSummary, Manifest, PartitionSpec, Resolver, Schema, Snapshot, StagedUpdate, TableMetadata, TableRequirement} from '../../src/types.js'
 */

/**
 * @param {TableMetadata} metadata
 * @returns {Snapshot|undefined}
 */
export function currentSnapshot(metadata) {
  const id = metadata['current-snapshot-id']
  if (id === undefined) return undefined
  return metadata.snapshots?.find(s => s['snapshot-id'] === id)
}

/**
 * @param {TableMetadata} metadata
 * @param {Resolver} resolver
 * @returns {Promise<Manifest[]>}
 */
export async function loadPriorManifests(metadata, resolver) {
  const snap = currentSnapshot(metadata)
  if (!snap?.['manifest-list']) return []
  return /** @type {Manifest[]} */ (await fetchAvroRecords(snap['manifest-list'], resolver))
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
export async function buildSnapshotUpdate({
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
export function buildPartitionSummaries(partitions, schema, partitionSpec) {
  return partitionSpec.fields.map(pf => {
    const sourceField = schema.fields.find(f => f.id === pf['source-id'])
    if (!sourceField) throw new Error(`partition source field id ${pf['source-id']} not found`)
    const resultType = transformResultType(pf.transform, sourceField.type)
    const values = partitions.map(p => p[pf.name])
    return computeFieldSummary(values, resultType)
  })
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
