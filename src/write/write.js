import { loadTable } from '../catalog/loadTable.js'
import { restCatalogCreateTable, restCatalogDropTable, restCatalogLoadTable, restCatalogUpdateTable } from '../catalog/rest.js'
import { icebergCreate } from '../create.js'
import { loadLatestFileCatalogMetadata } from '../metadata.js'
import { applyUpdates, fileCatalogCommit } from './commit.js'
import { icebergStageDeletionVector } from './stage-deletion-vector.js'
import { icebergStagePositionDelete } from './stage-position-delete.js'
import { icebergStageAppend, icebergStageExpireSnapshots, icebergStageSetRef, prepareAppend, stageSnapshotForAppend } from './stage.js'

/**
 * @import {Catalog, IcebergTransaction, Lister, PartitionSpec, Resolver, Schema, Snapshot, SortOrder, StagedUpdate, TableMetadata, TableRequirement, TableUpdate} from '../../src/types.js'
 */

const DEFAULT_RETRY = Object.freeze({
  maxAttempts: 50,
  initialMs: 50,
  maxMs: 3000,
  factor: 2,
  totalTimeoutMs: 30 * 60 * 1000,
})

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
  // Spec v3 §"Manifest Inheritance": data and manifest files do NOT need to
  // be rewritten on optimistic-commit retry. Prepare them once outside the
  // retry loop; per attempt only the manifest list and metadata.json change.
  // Without this, N concurrent writers each retrying would write O(N²)
  // orphan parquets to S3.
  const prepared = await prepareAppend({
    tableUrl: ctx.tableUrl,
    metadata: ctx.metadata,
    records,
    resolver: requireResolver(ctx.resolver, 'icebergAppend'),
  })
  return await commitWithRetry({
    catalog, target: { namespace, table }, ctx,
    stage: workingCtx => stageSnapshotForAppend({
      tableUrl: workingCtx.tableUrl,
      metadata: workingCtx.metadata,
      prepared,
      resolver: requireResolver(workingCtx.resolver, 'icebergAppend'),
    }),
  })
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
  return await commitWithRetry({
    catalog, target: { namespace, table }, ctx,
    stage: async workingCtx => {
      const writer = requireResolver(workingCtx.resolver, 'icebergDelete')
      const formatVersion = workingCtx.metadata['format-version']
      const effective = mode ?? (formatVersion === 3 ? 'puffin' : 'parquet')
      if (effective === 'puffin') {
        return await icebergStageDeletionVector({
          tableUrl: workingCtx.tableUrl,
          metadata: workingCtx.metadata,
          deletes,
          resolver: writer,
        })
      }
      if (effective === 'parquet') {
        return await icebergStagePositionDelete({
          tableUrl: workingCtx.tableUrl,
          metadata: workingCtx.metadata,
          deletes,
          resolver: writer,
        })
      }
      throw new Error(`unknown delete mode: ${effective}`)
    },
  })
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
  return await commitWithRetry({
    catalog, target: { namespace, table }, ctx,
    stage: workingCtx => icebergStageSetRef({
      metadata: workingCtx.metadata,
      ref,
      snapshotId,
      type,
      minSnapshotsToKeep,
      maxSnapshotAgeMs,
      maxRefAgeMs,
    }),
  })
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
  return await commitWithRetry({
    catalog, target: { namespace, table }, ctx,
    stage: workingCtx => icebergStageExpireSnapshots({ metadata: workingCtx.metadata, snapshotIds }),
  })
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
 * that produces each (type, ref) pair: they reference the original loaded
 * metadata, not the working copy. If the callback throws or stages nothing,
 * no commit is sent. Files written by the callback before it threw are
 * left in place (the caller may still want them); failed-commit orphans
 * are cleaned up best-effort (see below).
 *
 * Failure semantics:
 * - Atomicity: a transaction either commits in full or has no visible
 *   effect on the table.
 * - On a concurrent-commit conflict (catalog returns 412 or 409), this
 *   function throws `IcebergTransactionConflictError`. The transaction
 *   does NOT auto-retry, because the user callback may have side effects
 *   or non-deterministic outputs and cannot be safely re-invoked. Callers
 *   that need retry must wrap this call themselves and ensure the
 *   callback is idempotent.
 * - On commit failure (any reason before the metadata file is durably
 *   committed), the data, manifest, and manifest-list files written during
 *   staging are deleted best-effort via the effective `resolver.deleter`.
 *   Cleanup errors are suppressed so the original commit error surfaces
 *   unchanged. If the resolver has no `deleter`, files remain as orphans.
 *
 * For concurrent append-only workloads, prefer `icebergAppend`: it retries
 * internally on 412/409 without re-uploading data or manifest files.
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace] - REST catalog only.
 * @param {string} [options.table] - REST catalog only.
 * @param {string} [options.tableUrl] - File catalog only.
 * @param {Resolver} [options.resolver]
 * @param {(tx: IcebergTransaction) => Promise<void> | void} callback
 * @returns {Promise<TableMetadata>}
 * @throws {IcebergTransactionConflictError} on concurrent-commit conflict.
 */
export async function icebergTransaction({ catalog, namespace, table, tableUrl, resolver }, callback) {
  const ctx = await loadTable({ catalog, namespace, table, tableUrl, resolver })
  const deleter = ctx.resolver?.deleter
  let workingMetadata = ctx.metadata

  /** @type {TableRequirement[]} */
  const allRequirements = []
  /** @type {TableUpdate[]} */
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

  try {
    return await commitStaged(catalog, { namespace, table }, ctx, {
      snapshot: lastSnapshot,
      requirements: allRequirements,
      updates: allUpdates,
      writtenFiles: allWrittenFiles,
    })
  } catch (err) {
    // The commit failed, so the data and manifest files we wrote during
    // staging are orphans. Best-effort cleanup so a contended workload
    // doesn't accumulate unreachable parquets on S3. Suppress per-file
    // failures (and the absence of a deleter) so the original commit error
    // surfaces unchanged.
    if (deleter && allWrittenFiles.length > 0) {
      await Promise.allSettled(allWrittenFiles.map(p => deleter(p)))
    }
    if (isCommitConflict(err)) {
      throw new IcebergTransactionConflictError(err)
    }
    throw err
  }
}

/**
 * Thrown when an `icebergTransaction` commit fails because another writer
 * advanced the table between the load and the commit. `icebergTransaction`
 * does not retry on conflict because the user-supplied callback may have
 * side effects, non-deterministic outputs, or external mutations that
 * cannot be safely re-invoked. Callers that need retry semantics should
 * wrap the whole `icebergTransaction(...)` call in their own loop and
 * ensure the callback is safe to re-run.
 *
 * For plain append workloads under contention, prefer `icebergAppend`:
 * it retries on 412/409 internally without re-uploading data or manifest
 * files (only the per-attempt manifest list and metadata.json are rewritten).
 */
export class IcebergTransactionConflictError extends Error {
  /**
   * @param {unknown} cause - The underlying error from the catalog commit (carries `.status`).
   */
  constructor(cause) {
    const status = /** @type {any} */ (cause)?.status
    const detail = /** @type {any} */ (cause)?.message
    super(
      `icebergTransaction commit conflicted with a concurrent writer${status ? ` (status ${status})` : ''}. ` +
      'The transaction was not applied and staged files were cleaned up. ' +
      'Wrap the icebergTransaction call in your own retry loop if the callback is safe to re-run; ' +
      'for append-only workloads prefer icebergAppend, which retries without re-uploading data.' +
      (detail ? ` (cause: ${detail})` : '')
    )
    this.name = 'IcebergTransactionConflictError'
    this.cause = cause
    if (typeof status === 'number') this.status = status
  }
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
    conditionalCommits: catalog.conditionalCommits,
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
 * @param {{metadata: TableMetadata, metadataFileName: string | undefined, version?: number, tableUrl: string, resolver: Resolver | undefined}} ctx
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
    currentVersion: ctx.version,
    staged,
    resolver: ctx.resolver,
    conditionalCommits: catalog.type === 'file' && catalog.conditionalCommits,
  })
}

/**
 * Stage and commit, retrying on concurrent-commit conflicts. The `stage`
 * callback is invoked once per attempt against the freshest loaded
 * metadata, so each retry rebuilds the snapshot/manifest-list against the
 * new parent (sequence number, parent snapshot id, ref pointer, etc., all
 * advance correctly). Data and manifest files written by failed file
 * catalog attempts may become orphans; cleanup is left to a separate
 * maintenance pass. REST catalog retries don't leak orphans since the
 * commit endpoint is atomic.
 *
 * Conflict detection: file catalogs see 412/409 on the conditional PUT
 * when `conditionalCommits` is true; REST catalogs see 409 from the
 * commit endpoint when the server-side `requirements` check fails. File
 * catalogs without `conditionalCommits` do not retry: the legacy
 * overwrite path stays one-shot.
 *
 * Between attempts, sleeps with full-jitter exponential back-off so that
 * concurrent writers don't all stampede the catalog. Policy comes from
 * the per-table `commit.retry.*` properties on `ctx.metadata` with the
 * library defaults tuned for parallel writers (50 attempts, 50ms→3s
 * back-off, 30 min total) as fallback. The wall-clock cap also bounds
 * in-flight attempts: once the budget is exhausted the loop stops even
 * if `maxAttempts` is not yet reached.
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {{namespace?: string | string[], table?: string}} options.target
 * @param {{metadata: TableMetadata, metadataFileName: string | undefined, version?: number, tableUrl: string, resolver: Resolver | undefined}} options.ctx - The initial loaded ctx; refreshed on retry.
 * @param {(workingCtx: {metadata: TableMetadata, metadataFileName: string | undefined, version?: number, tableUrl: string, resolver: Resolver | undefined}) => Promise<StagedUpdate> | StagedUpdate} options.stage
 * @returns {Promise<TableMetadata>}
 */
async function commitWithRetry({ catalog, target, ctx, stage }) {
  const retryEnabled = catalog.type === 'rest'
    || catalog.type === 'file' && catalog.conditionalCommits === true
  const policy = resolveRetryPolicy(ctx.metadata)
  const startedAt = Date.now()
  let workingCtx = ctx
  for (let attempt = 1; attempt <= policy.maxAttempts; attempt++) {
    const staged = await stage(workingCtx)
    try {
      return await commitStaged(catalog, target, workingCtx, staged)
    } catch (err) {
      if (!retryEnabled || !isCommitConflict(err)) throw err
      if (attempt === policy.maxAttempts) {
        throw new Error(
          `${catalog.type} catalog commit failed after ${policy.maxAttempts} attempts due to concurrent commits`
        )
      }
      const elapsed = Date.now() - startedAt
      if (elapsed >= policy.totalTimeoutMs) {
        throw new Error(
          `${catalog.type} catalog commit retry budget exhausted after ${attempt} attempts and ${elapsed}ms (limit ${policy.totalTimeoutMs}ms)`
        )
      }
      const remaining = policy.totalTimeoutMs - elapsed
      const sleepMs = Math.min(jitteredBackoff(attempt, policy), remaining)
      await sleep(sleepMs)
      workingCtx = await reloadCtx(catalog, target, workingCtx, err)
    }
  }
  throw new Error('unreachable')
}

/**
 * Refresh the working context between retry attempts. File catalogs reload
 * by probing the metadata directory; REST catalogs reload by calling the
 * catalog's load-table endpoint.
 *
 * @param {Catalog} catalog
 * @param {{namespace?: string | string[], table?: string}} target
 * @param {{metadata: TableMetadata, metadataFileName: string | undefined, version?: number, tableUrl: string, resolver: Resolver | undefined}} workingCtx
 * @param {unknown} lastErr
 * @returns {Promise<{metadata: TableMetadata, metadataFileName: string | undefined, version?: number, tableUrl: string, resolver: Resolver | undefined}>}
 */
async function reloadCtx(catalog, target, workingCtx, lastErr) {
  if (catalog.type === 'rest') {
    if (!target.namespace || !target.table) throw lastErr
    const { metadata } = await restCatalogLoadTable(catalog, {
      namespace: target.namespace, table: target.table,
    })
    return {
      metadata,
      metadataFileName: workingCtx.metadataFileName,
      version: workingCtx.version,
      tableUrl: workingCtx.tableUrl,
      resolver: workingCtx.resolver,
    }
  }
  if (!workingCtx.resolver) throw lastErr
  const fresh = await loadLatestFileCatalogMetadata({
    tableUrl: workingCtx.tableUrl,
    resolver: workingCtx.resolver,
    lister: catalog.lister,
  })
  return {
    metadata: fresh.metadata,
    metadataFileName: fresh.metadataFileName,
    version: fresh.version,
    tableUrl: workingCtx.tableUrl,
    resolver: workingCtx.resolver,
  }
}

/**
 * Resolve the retry policy from per-table `commit.retry.*` properties on
 * `metadata`, falling back to the library defaults. Missing or malformed
 * properties fall through to the default rather than throwing, so a bad
 * property value never breaks commits. Resolved once at the start of
 * `commitWithRetry`; property changes a writer makes mid-loop apply to
 * subsequent commits, not the one currently in flight.
 *
 * @param {TableMetadata} metadata
 * @returns {{maxAttempts: number, initialMs: number, maxMs: number, factor: number, totalTimeoutMs: number}}
 */
function resolveRetryPolicy(metadata) {
  const props = metadata.properties ?? {}
  const numRetries = parseTableProp(props['commit.retry.num-retries'])
  const maxAttempts = numRetries === undefined ? DEFAULT_RETRY.maxAttempts : numRetries + 1
  const initialMs = parseTableProp(props['commit.retry.min-wait-ms']) ?? DEFAULT_RETRY.initialMs
  const maxMs = parseTableProp(props['commit.retry.max-wait-ms']) ?? DEFAULT_RETRY.maxMs
  const totalTimeoutMs = parseTableProp(props['commit.retry.total-timeout-ms']) ?? DEFAULT_RETRY.totalTimeoutMs
  return { maxAttempts, initialMs, maxMs, factor: DEFAULT_RETRY.factor, totalTimeoutMs }
}

/**
 * Parse a `commit.retry.*` table property into a non-negative finite number.
 * Returns undefined for missing or malformed values so the caller falls
 * through to the default.
 *
 * @param {unknown} value
 * @returns {number | undefined}
 */
function parseTableProp(value) {
  if (value === undefined || value === null || value === '') return undefined
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0) return undefined
  return n
}

/**
 * Full-jitter back-off: random in [0, base) where base is the exponentially
 * growing ceiling capped at maxMs. (See "Exponential Backoff and Jitter",
 * AWS Architecture Blog.) `attempt` is 1-based.
 *
 * @param {number} attempt
 * @param {{initialMs: number, maxMs: number, factor: number}} policy
 * @returns {number}
 */
function jitteredBackoff(attempt, policy) {
  if (policy.initialMs === 0 || policy.maxMs === 0) return 0
  const base = Math.min(policy.maxMs, policy.initialMs * policy.factor ** (attempt - 1))
  return Math.floor(Math.random() * base)
}

/**
 * @param {number} ms
 * @returns {Promise<void>}
 */
function sleep(ms) {
  if (ms <= 0) return Promise.resolve()
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * @param {unknown} err
 * @returns {boolean}
 */
function isCommitConflict(err) {
  if (!err || typeof err !== 'object') return false
  const { status } = /** @type {any} */ (err)
  return status === 412 || status === 409
}
