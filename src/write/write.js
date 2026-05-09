import { loadTable } from '../catalog/loadTable.js'
import { restCatalogCreateTable, restCatalogDropTable, restCatalogUpdateTable } from '../catalog/rest.js'
import { icebergCreate } from '../create.js'
import { applyUpdates, fileCatalogCommit } from './commit.js'
import {
  icebergStageAppend,
  icebergStageDeletionVector,
  icebergStageExpireSnapshots,
  icebergStagePositionDelete,
  icebergStageSetRef,
} from './stage.js'

/**
 * @import {Catalog, IcebergTransaction, Lister, PartitionSpec, Resolver, Schema, Snapshot, SortOrder, StagedUpdate, TableMetadata, TableRequirement} from '../../src/types.js'
 */

/**
 * Append rows to a table in one call: load metadata, stage the parquet writes
 * + manifest + new snapshot, commit through the catalog.
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace] - REST catalog only.
 * @param {string} [options.table] - REST catalog only.
 * @param {string} [options.tableUrl] - File catalog only.
 * @param {Resolver} [options.resolver]
 * @param {Record<string, any>[]} options.records
 * @returns {Promise<TableMetadata>}
 */
export async function icebergAppend({ catalog, namespace, table, tableUrl, resolver, records }) {
  const ctx = await loadTable({ catalog, namespace, table, tableUrl, resolver })
  const writer = requireResolver(ctx.resolver, 'icebergAppend')
  const staged = await icebergStageAppend({
    tableUrl: ctx.tableUrl,
    metadata: ctx.metadata,
    records,
    resolver: writer,
  })
  return await commitStaged(catalog, { namespace, table }, ctx, staged)
}

/**
 * Apply row-level position deletes in one call. Picks the v3 puffin deletion
 * vector path on format-version 3 and the v2 parquet position-delete path on
 * format-version 2. New parquet position delete files are rejected for v3
 * tables because v3 writers must use deletion vectors.
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace]
 * @param {string} [options.table]
 * @param {string} [options.tableUrl]
 * @param {Resolver} [options.resolver]
 * @param {{file_path: string, pos: bigint|number}[]} options.deletes
 * @param {'puffin'|'parquet'} [options.mode]
 * @returns {Promise<TableMetadata>}
 */
export async function icebergDelete({ catalog, namespace, table, tableUrl, resolver, deletes, mode }) {
  const ctx = await loadTable({ catalog, namespace, table, tableUrl, resolver })
  const writer = requireResolver(ctx.resolver, 'icebergDelete')
  const formatVersion = ctx.metadata['format-version']
  const effective = mode ?? (formatVersion === 3 ? 'puffin' : 'parquet')
  let staged
  if (effective === 'puffin') {
    staged = await icebergStageDeletionVector({
      tableUrl: ctx.tableUrl,
      metadata: ctx.metadata,
      deletes,
      resolver: writer,
    })
  } else if (effective === 'parquet') {
    staged = await icebergStagePositionDelete({
      tableUrl: ctx.tableUrl,
      metadata: ctx.metadata,
      deletes,
      resolver: writer,
    })
  } else {
    throw new Error(`unknown delete mode: ${effective}`)
  }
  return await commitStaged(catalog, { namespace, table }, ctx, staged)
}

/**
 * Point a branch or tag at an existing snapshot (rollback, tagging, branch
 * fast-forward, retention tweaks).
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace]
 * @param {string} [options.table]
 * @param {string} [options.tableUrl]
 * @param {Resolver} [options.resolver]
 * @param {string} options.ref
 * @param {number} options.snapshotId
 * @param {'branch'|'tag'} [options.type]
 * @param {number} [options.minSnapshotsToKeep]
 * @param {number} [options.maxSnapshotAgeMs]
 * @param {number} [options.maxRefAgeMs]
 * @returns {Promise<TableMetadata>}
 */
export async function icebergSetRef({
  catalog, namespace, table, tableUrl, resolver,
  ref, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs,
}) {
  const ctx = await loadTable({ catalog, namespace, table, tableUrl, resolver })
  const staged = icebergStageSetRef({
    metadata: ctx.metadata,
    ref,
    snapshotId,
    type,
    minSnapshotsToKeep,
    maxSnapshotAgeMs,
    maxRefAgeMs,
  })
  return await commitStaged(catalog, { namespace, table }, ctx, staged)
}

/**
 * Expire one or more snapshots from a table. Data files are not removed from
 * storage; that is a separate maintenance pass.
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace]
 * @param {string} [options.table]
 * @param {string} [options.tableUrl]
 * @param {Resolver} [options.resolver]
 * @param {number[]} options.snapshotIds
 * @returns {Promise<TableMetadata>}
 */
export async function icebergExpireSnapshots({ catalog, namespace, table, tableUrl, resolver, snapshotIds }) {
  const ctx = await loadTable({ catalog, namespace, table, tableUrl, resolver })
  const staged = icebergStageExpireSnapshots({ metadata: ctx.metadata, snapshotIds })
  return await commitStaged(catalog, { namespace, table }, ctx, staged)
}

/**
 * Run multiple staging operations against a single loaded snapshot of the
 * table and commit them atomically. The callback receives a `tx` object whose
 * methods (`append`, `delete`, `setRef`, `expireSnapshots`) mirror the
 * top-level functions but stage rather than commit. Each call advances an
 * in-memory working copy of the metadata so subsequent operations see prior
 * staged snapshots and refs; everything ships in one commit at the end.
 *
 * The CAS preconditions sent to the catalog are taken from the FIRST stage
 * that produces each (type, ref) pair — they reference the original loaded
 * metadata, not the working copy. If the callback throws or stages nothing,
 * no commit is sent (already-written data/manifest files become orphans;
 * cleanup is the caller's responsibility, same as other write paths).
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace] - REST catalog only.
 * @param {string} [options.table] - REST catalog only.
 * @param {string} [options.tableUrl] - File catalog only.
 * @param {Resolver} [options.resolver]
 * @param {(tx: IcebergTransaction) => Promise<void> | void} callback
 * @returns {Promise<TableMetadata>}
 */
export async function icebergTransaction({ catalog, namespace, table, tableUrl, resolver }, callback) {
  const ctx = await loadTable({ catalog, namespace, table, tableUrl, resolver })
  let workingMetadata = ctx.metadata

  /** @type {TableRequirement[]} */
  const allRequirements = []
  /** @type {import('../types.js').TableUpdate[]} */
  const allUpdates = []
  /** @type {string[]} */
  const allWrittenFiles = []
  /** @type {Snapshot | undefined} */
  let lastSnapshot

  /** @param {StagedUpdate} staged */
  function mergeStaged(staged) {
    for (const req of staged.requirements) {
      const key = requirementKey(req)
      if (!allRequirements.some(r => requirementKey(r) === key)) {
        allRequirements.push(req)
      }
    }
    allUpdates.push(...staged.updates)
    allWrittenFiles.push(...staged.writtenFiles)
    workingMetadata = applyUpdates(workingMetadata, staged.updates)
    lastSnapshot = staged.snapshot
  }

  /** @type {IcebergTransaction} */
  const tx = {
    async append({ records }) {
      const writer = requireResolver(ctx.resolver, 'icebergTransaction.append')
      const staged = await icebergStageAppend({
        tableUrl: ctx.tableUrl,
        metadata: workingMetadata,
        records,
        resolver: writer,
      })
      mergeStaged(staged)
    },
    async delete({ deletes, mode }) {
      const writer = requireResolver(ctx.resolver, 'icebergTransaction.delete')
      const formatVersion = workingMetadata['format-version']
      const effective = mode ?? (formatVersion === 3 ? 'puffin' : 'parquet')
      let staged
      if (effective === 'puffin') {
        staged = await icebergStageDeletionVector({
          tableUrl: ctx.tableUrl,
          metadata: workingMetadata,
          deletes,
          resolver: writer,
        })
      } else if (effective === 'parquet') {
        staged = await icebergStagePositionDelete({
          tableUrl: ctx.tableUrl,
          metadata: workingMetadata,
          deletes,
          resolver: writer,
        })
      } else {
        throw new Error(`unknown delete mode: ${effective}`)
      }
      mergeStaged(staged)
    },
    setRef({ ref, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs }) {
      const staged = icebergStageSetRef({
        metadata: workingMetadata,
        ref,
        snapshotId,
        type,
        minSnapshotsToKeep,
        maxSnapshotAgeMs,
        maxRefAgeMs,
      })
      mergeStaged(staged)
    },
    expireSnapshots({ snapshotIds }) {
      const staged = icebergStageExpireSnapshots({ metadata: workingMetadata, snapshotIds })
      mergeStaged(staged)
    },
  }

  await callback(tx)

  if (allUpdates.length === 0) return ctx.metadata
  if (!lastSnapshot) throw new Error('icebergTransaction: no snapshot produced')

  return await commitStaged(catalog, { namespace, table }, ctx, {
    snapshot: lastSnapshot,
    requirements: allRequirements,
    updates: allUpdates,
    writtenFiles: allWrittenFiles,
  })
}

/**
 * Stable key for a TableRequirement so a transaction can dedupe across
 * multiple stages: `assert-ref-snapshot-id` is keyed by ref name, every other
 * type is keyed by type alone (only one of each can apply meaningfully).
 *
 * @param {TableRequirement} req
 * @returns {string}
 */
function requirementKey(req) {
  if (req.type === 'assert-ref-snapshot-id') return `${req.type}:${req.ref}`
  return req.type
}

/**
 * Create a new table. REST: delegates to the catalog's create endpoint.
 * File: writes the initial `v1.metadata.json` and `version-hint.text` under
 * `tableUrl` via `catalog.resolver`.
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace] - REST catalog only.
 * @param {string} [options.table] - REST catalog only.
 * @param {string} [options.tableUrl] - File catalog only; also passed as `location` for REST.
 * @param {Schema} [options.schema]
 * @param {PartitionSpec} [options.partitionSpec]
 * @param {SortOrder} [options.sortOrder]
 * @param {Record<string, string>} [options.properties]
 * @param {2 | 3} [options.formatVersion] - File catalog only.
 * @param {boolean} [options.stageCreate] - REST catalog only.
 * @returns {Promise<TableMetadata>}
 */
export async function icebergCreateTable({
  catalog, namespace, table, tableUrl,
  schema, partitionSpec, sortOrder, properties, formatVersion, stageCreate,
}) {
  if (catalog.type === 'rest') {
    if (!namespace || !table) throw new Error('namespace and table are required for rest catalogs')
    if (!schema) throw new Error('schema is required for rest catalogs')
    const { metadata } = await restCatalogCreateTable(catalog, {
      namespace,
      table,
      schema,
      location: tableUrl,
      partitionSpec,
      writeOrder: sortOrder,
      stageCreate,
      properties,
    })
    return metadata
  }
  if (!tableUrl) throw new Error('tableUrl is required for file catalogs')
  return await icebergCreate({
    tableUrl,
    resolver: catalog.resolver,
    schema,
    formatVersion,
    partitionSpec,
    sortOrder,
    properties,
  })
}

/**
 * Drop a table. REST: delegates to the catalog's drop endpoint, forwarding
 * `purgeRequested`. File: lists `metadata/` (and `data/` when `purgeRequested`)
 * via `lister` and deletes each file via `catalog.resolver.deleter`. File
 * purges do not recurse into partition subdirectories.
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace] - REST catalog only.
 * @param {string} [options.table] - REST catalog only.
 * @param {string} [options.tableUrl] - File catalog only.
 * @param {Lister} [options.lister] - File catalog only; required to enumerate files to delete.
 * @param {boolean} [options.purgeRequested] - REST: forwarded to the server. File: also delete `data/`.
 * @returns {Promise<void>}
 */
export async function icebergDropTable({ catalog, namespace, table, tableUrl, lister, purgeRequested }) {
  if (catalog.type === 'rest') {
    if (!namespace || !table) throw new Error('namespace and table are required for rest catalogs')
    await restCatalogDropTable(catalog, { namespace, table, purgeRequested })
    return
  }
  if (!tableUrl) throw new Error('tableUrl is required for file catalogs')
  if (!lister) throw new Error('lister is required to drop a file catalog table')
  const { deleter } = catalog.resolver
  if (!deleter) throw new Error('resolver.deleter is required to drop a file catalog table')
  const dirs = purgeRequested ? ['metadata', 'data'] : ['metadata']
  for (const dir of dirs) {
    const names = await lister(`${tableUrl}/${dir}`).catch(() => /** @type {string[]} */ ([]))
    await Promise.allSettled(names.map(n => deleter(`${tableUrl}/${dir}/${n}`)))
  }
}

/**
 * Narrow `Resolver | undefined` to `Resolver`, throwing a descriptive error
 * for the callers that need to write data files.
 *
 * @param {Resolver | undefined} resolver
 * @param {string} caller
 * @returns {Resolver}
 */
function requireResolver(resolver, caller) {
  if (!resolver) throw new Error(`${caller}: resolver is required`)
  return resolver
}

/**
 * Commit a staged update through the catalog branch in use.
 *
 * @param {Catalog} catalog
 * @param {{namespace?: string | string[], table?: string}} target
 * @param {{metadata: TableMetadata, metadataFileName: string | undefined, tableUrl: string, resolver: Resolver | undefined}} ctx
 * @param {StagedUpdate} staged
 * @returns {Promise<TableMetadata>}
 */
async function commitStaged(catalog, target, ctx, staged) {
  if (catalog.type === 'rest') {
    const { metadata } = await restCatalogUpdateTable(catalog, {
      namespace: /** @type {string | string[]} */ (target.namespace),
      table: /** @type {string} */ (target.table),
      requirements: staged.requirements,
      updates: staged.updates,
    })
    return metadata
  }
  if (!ctx.resolver) throw new Error('resolver is required to commit to a file catalog')
  return await fileCatalogCommit({
    tableUrl: ctx.tableUrl,
    metadata: ctx.metadata,
    metadataFileName: ctx.metadataFileName,
    staged,
    resolver: ctx.resolver,
  })
}
