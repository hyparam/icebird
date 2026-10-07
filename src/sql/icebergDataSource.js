import { asyncRow } from 'squirreling'
import { applicablePositionDeletes } from '../delete.js'
import { fetchDeleteMaps, urlResolver } from '../fetch.js'
import { fetchManifestEntries, icebergManifestList } from '../manifest.js'
import { icebergMetadata } from '../metadata.js'
import { readDataFile, readDataFileBatches, readDataFileColumn } from '../read.js'
import { fileMightMatch, manifestMightMatch, partitionMightMatch } from '../prune.js'
import { whereToParquetFilter } from './whereFilter.js'
import { pruneTopKFiles, scanTopKFiles } from './topK.js'

/**
 * @import {AsyncDataSource, ExprNode, PrepareScan, RelationSchema, ScanOptions, ScanResults, SqlPrimitive} from 'squirreling'
 * @import {ScanColumnResults} from 'squirreling/src/types.js'
 * @import {ParquetQueryFilter} from 'hyparquet'
 * @import {Lister, Manifest, ManifestEntry, Resolver, TableMetadata} from '../../src/types.js'
 */

/**
 * Icebird keeps the legacy scan surface alongside prepared batches so callers
 * written against older squirreling releases retain a statically callable
 * `scan()` method.
 *
 * @typedef {object} IcebergAsyncDataSource
 * @property {number} [numRows]
 * @property {string[]} columns
 * @property {RelationSchema} schema
 * @property {(options: ScanOptions) => ScanResults} scan
 * @property {PrepareScan} prepareScan
 * @property {NonNullable<AsyncDataSource['scanColumn']>} scanColumn
 */

/**
 * Creates a squirreling AsyncDataSource backed by an Iceberg table. Prepared
 * scans expose lazy native column batches; legacy scan hooks stream materialized
 * rows for compatibility.
 *
 * Metadata, the manifest list, delete manifests, schema, and delete maps are
 * resolved once at construction. Each scan fetches the data manifests it
 * needs and walks their data files in manifest order, yielding rows on
 * demand. Pushdowns:
 * - WHERE skips whole data manifests before they are fetched, using the
 *   manifest list's per-partition-field summaries (Java `ManifestEvaluator`).
 *   This pays off on tables whose manifests are clustered by partition (see
 *   `icebergRewriteManifests`).
 * - Column projection (`columns`) is pushed into the parquet read so only the
 *   requested columns are decoded. Equality-delete predicate columns and row
 *   lineage columns are read regardless when needed.
 * - WHERE prunes whole data files before they are opened, using each manifest
 *   entry's partition tuple and per-column `lower_bounds`/`upper_bounds`, and
 *   is passed to hyparquet for conservative row-group, bloom-filter, and page-
 *   index pruning for supported parts of the expression (comparisons, IN,
 *   AND/OR/NOT on identifier vs literal). Partial filters retain the full SQL
 *   predicate and LIMIT/OFFSET in the engine. Prepared scans leave exact
 *   matching to the engine; legacy scans match retained rows after recovering
 *   their physical positions. Unsupported nodes (LIKE,
 *   functions, arithmetic, identifier vs identifier) stay in the engine.
 * - For a single supported Top-K sort key and an exactly convertible WHERE,
 *   scan promising files and retain the best K surviving rows. Actual
 *   winners certify the cutoff for skipping worse files. Physical positions
 *   preserve stable ties; unknown/null-bearing files remain eligible. A tied
 *   cutoff retains a bounded buffer of ties, streaming eligible files again
 *   only on overflow. Partial filters keep the ordinary scan because they
 *   cannot certify matches.
 * - When WHERE is resolved at scan time (either absent or fully pushed) we
 *   cap the scan at `offset + limit` rows so the source terminates early.
 *   OFFSET is also pushed into the parquet seek, and the per-file read bounded
 *   by row position, only when there is no WHERE at all: a pushed-down WHERE is
 *   matched per row, so physical row positions no longer line up with result
 *   positions and a position-based bound would drop matching rows that sort
 *   later in the file. Deletes disable position pushdown for the same reason
 *   (record_count is pre-delete). In those cases the engine applies the final
 *   LIMIT/OFFSET slice over the (at most offset+limit) rows the source emits.
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL or path of the table.
 * @param {string} [options.metadataFileName] - Specific metadata file to load.
 * @param {TableMetadata} [options.metadata] - Pre-fetched table metadata.
 * @param {number | bigint} [options.snapshotId] - Optional snapshot id for time travel; defaults to the current snapshot.
 * @param {Resolver} [options.resolver] - I/O resolver (defaults to `urlResolver()`).
 * @param {Lister} [options.lister] - Directory lister, used to discover the latest metadata.
 * @returns {Promise<IcebergAsyncDataSource>}
 */
export async function icebergDataSource({ tableUrl, metadataFileName, metadata, snapshotId, resolver, lister }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  const fetchResolver = resolver ?? urlResolver()
  const tableMetadata = metadata ?? await icebergMetadata({ tableUrl, metadataFileName, resolver: fetchResolver, lister })

  // When a snapshot is pinned, use that snapshot's schema (snapshots can
  // reference older schemas after evolution). Fall back to the table's
  // current-schema-id when the snapshot doesn't carry one or none is pinned.
  const snapshot = snapshotId !== undefined
    ? tableMetadata.snapshots?.find(s => BigInt(s['snapshot-id']) === BigInt(snapshotId))
    : undefined
  const schemaId = snapshot?.['schema-id'] ?? tableMetadata['current-schema-id']
  const schema = tableMetadata.schemas.find(s => s['schema-id'] === schemaId)
  if (!schema) throw new Error('schema not found in metadata')
  // Narrowed for the hoisted helpers below.
  const scanSchema = schema
  const columns = schema.fields.map(f => f.name)
  /** @type {RelationSchema} */
  const relationSchema = {
    fields: schema.fields.map(field => ({
      id: field.id,
      name: field.name,
      dataType: { type: 'unknown' },
      nullable: !field.required,
    })),
  }
  const rowLineage = tableMetadata['format-version'] >= 3

  const { manifests } = await icebergManifestList({ metadata: tableMetadata, resolver: fetchResolver, snapshotId })
  const dataManifests = manifests.filter(m => (m.content ?? 0) === 0)
  const deleteEntries = liveEntries(await Promise.all(
    manifests.filter(m => m.content === 1).map(m => fetchManifestEntries(m, fetchResolver))
  ))
  const hasDeletes = deleteEntries.length > 0

  // Pre-fetch delete maps once; reused by every scan.
  const deleteMapsPromise = fetchDeleteMaps(deleteEntries, fetchResolver)

  /**
   * Live data entries of the manifests that might match `filter`, in
   * manifest-list order.
   *
   * @param {ParquetQueryFilter | undefined} filter
   * @returns {Promise<ManifestEntry[]>}
   */
  async function dataEntriesFor(filter) {
    const selected = filter
      ? dataManifests.filter(m => manifestMightMatch(filter, m, scanSchema, tableMetadata))
      : dataManifests
    return liveEntries(await Promise.all(selected.map(m => fetchManifestEntries(m, fetchResolver))))
  }
  /**
   * Live rows the data files of `selected` manifests hold, before deletes,
   * from the manifest list's counts; undefined when a count is missing.
   *
   * @param {Manifest[]} selected
   * @returns {number | undefined}
   */
  function manifestRows(selected) {
    let rows = 0
    for (const m of selected) {
      if (m.added_rows_count == null || m.existing_rows_count == null) return undefined
      rows += Number(m.added_rows_count) + Number(m.existing_rows_count)
    }
    return rows
  }

  // Exact row count from the manifest list, or by reading every data manifest
  // if a count is missing. When delete files exist the sum is pre-delete and
  // overstates the visible count, so leave numRows undefined rather than
  // reporting a wrong total.
  /** @type {number | undefined} */
  let numRows
  if (!hasDeletes) {
    numRows = manifestRows(dataManifests)
    if (numRows === undefined) {
      numRows = 0
      for (const entry of await dataEntriesFor(undefined)) {
        numRows += Number(entry.data_file.record_count)
      }
    }
  }

  /**
   * Scan pruning: skip manifests whose partition summaries, then data files
   * whose partition tuple or per-column bounds, prove no row can match the
   * filter. All pruners are inclusive projections (they never drop a file
   * with a matching row), so query results are unchanged.
   *
   * @param {ParquetQueryFilter | undefined} filter
   * @returns {Promise<ManifestEntry[]>}
   */
  async function pruneEntries(filter) {
    const dataEntries = await dataEntriesFor(filter)
    if (!filter) return dataEntries
    return dataEntries.filter(entry =>
      partitionMightMatch(filter, entry, scanSchema, tableMetadata) &&
        fileMightMatch(filter, entry, scanSchema))
  }

  /** @type {IcebergAsyncDataSource} */
  const thisSource = {
    numRows,
    columns,
    schema: relationSchema,
    prepareScan(request) {
      const requestedFields = request.columns.map(demand => {
        const field = relationSchema.fields.find(candidate => candidate.id === demand.field)
        if (!field) throw new Error(`Prepared scan requested unknown field id ${demand.field}`)
        return field
      })
      const exactFilter = whereToParquetFilter(request.filter)
      const filter = exactFilter ?? whereToParquetFilter(request.filter, { allowPartial: true })
      // Bounds come from the manifest list so preparing a scan fetches no
      // manifests: the rows of every manifest the filter might match.
      const maxRows = manifestRows(filter
        ? dataManifests.filter(m => manifestMightMatch(filter, m, schema, tableMetadata))
        : dataManifests)
      const topKPrunes = request.topK && !request.filter && !hasDeletes

      return {
        schema: { fields: requestedFields },
        residual: {
          filter: request.filter,
          limit: request.limit,
          offset: request.offset,
        },
        properties: {
          exactRows: request.filter || hasDeletes || topKPrunes ? undefined : maxRows,
          maxRows,
        },
        async *batches({ signal } = {}) {
          signal?.throwIfAborted()
          const dataEntries = await dataEntriesFor(filter)
          const candidates = topKPrunes
            ? pruneTopKFiles(dataEntries, schema, /** @type {NonNullable<typeof request.topK>} */ (request.topK))
            : dataEntries
          const scanEntries = filter
            ? candidates.filter(entry =>
              partitionMightMatch(filter, entry, schema, tableMetadata) &&
                fileMightMatch(filter, entry, schema))
            : candidates
          const { positionDeletesMap, equalityDeleteGroups } = await deleteMapsPromise
          // Delete maps are already needed by the reader. Once loaded, use
          // exact surviving counts without opening the data files. Equality
          // predicates still require inspecting rows, so keep their fallback.
          const liveEntries = hasDeletes && request.topK && !request.filter && !equalityDeleteGroups.length
            ? pruneTopKFiles(scanEntries, schema, request.topK, entry => {
              const { file_path, record_count } = entry.data_file
              const deleted = applicablePositionDeletes(entry, positionDeletesMap.get(file_path), tableMetadata)
              let count = record_count
              for (const pos of deleted) {
                if (pos >= 0n && pos < record_count) count--
              }
              return count
            })
            : scanEntries
          if (request.topK && exactFilter) {
            const topK = scanTopKFiles(liveEntries, schema, request.topK, requestedFields,
              (entry, fields) => readDataFileBatches({
                dataEntry: entry,
                schema,
                metadata: tableMetadata,
                resolver: fetchResolver,
                fields,
                positionDeletesMap,
                equalityDeleteGroups,
                filter: exactFilter,
                applyFilter: true,
                signal,
              }), signal)
            if (topK) {
              yield* topK
              return
            }
          }
          for (const entry of liveEntries) {
            signal?.throwIfAborted()
            yield* readDataFileBatches({
              dataEntry: entry,
              schema,
              metadata: tableMetadata,
              resolver: fetchResolver,
              fields: requestedFields,
              positionDeletesMap,
              equalityDeleteGroups,
              filter,
              signal,
            })
          }
        },
      }
    },
    scan({ columns: scanColumns, where, limit, offset, signal }) {
      const rowColumns = scanColumns ?? columns
      // Only an exact conversion discharges WHERE. A partial filter still
      // prunes files and rows, but leaves the full predicate and LIMIT/OFFSET
      // to the engine so unsupported terms cannot admit or lose matches.
      const exactFilter = whereToParquetFilter(where)
      const filter = exactFilter ?? whereToParquetFilter(where, { allowPartial: true })
      const appliedWhere = where !== undefined && exactFilter !== undefined
      // Treat a fully-pushed-down WHERE the same as "no WHERE" for the
      // purpose of capping how many rows the source emits (LIMIT).
      const whereResolved = !where || appliedWhere
      // Position-based pushdown — seeking past `offset` physical rows and the
      // `fileRowEnd` LIMIT bound below — translates a row *count* into a
      // physical row *position*, which is only correct when every physical row
      // is also a result row. That holds only when there is NO WHERE at all.
      // A pushed-down WHERE (appliedWhere) is matched per-row inside the
      // parquet read, so the first N physical rows may contain fewer than N
      // (or zero) matches; bounding by position would silently drop matching
      // rows that sort later in the file (e.g. WHERE node_type='File' LIMIT 5
      // when the leading rows are all 'Session'). It is likewise unsafe with
      // deletes (record_count is pre-delete). Pruning, which would break the
      // cumulative record_count to row position mapping, only happens with a
      // WHERE. In all those cases we keep emitting up to `offset + limit`
      // matched rows and let the engine apply the final LIMIT/OFFSET slice.
      const canPushOffset = !where && !hasDeletes
      const skip = canPushOffset ? offset ?? 0 : 0
      // LIMIT (early termination) is safe whenever WHERE is resolved: we yield
      // at most offset+limit rows and, when offset isn't pushed, let the engine
      // apply the slice. This still saves reading later files/row groups.
      let take = Infinity
      if (whereResolved && limit !== undefined) {
        take = canPushOffset ? limit : (offset ?? 0) + limit
      }
      const appliedLimitOffset = canPushOffset

      return {
        appliedWhere,
        appliedLimitOffset,
        async *rows() {
          if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
          if (take === 0) return
          const scanEntries = await pruneEntries(filter)
          if (scanEntries.length === 0) return

          const { positionDeletesMap, equalityDeleteGroups } = await deleteMapsPromise

          let remainingSkip = skip
          let remaining = take
          for (const entry of scanEntries) {
            if (remaining <= 0) break
            if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
            const recordCount = Number(entry.data_file.record_count)
            const fileRowStart = remainingSkip < recordCount ? remainingSkip : recordCount
            // Bound the per-file end at fileRowStart+remaining so readDataFile
            // skips later row groups in the file once we have enough rows.
            // Only safe when canPushOffset is true: `remaining` is in
            // post-delete coordinates, but fileRowStart+remaining is a
            // pre-delete index, so with deletes this could skip visible rows
            // we still need. Read the whole file in that case and rely on
            // the inner per-row break.
            const fileRowEnd = canPushOffset && remaining !== Infinity
              ? Math.min(recordCount, fileRowStart + remaining)
              : recordCount
            if (fileRowStart >= fileRowEnd) {
              remainingSkip -= recordCount
              continue
            }
            remainingSkip = 0

            let stop = false
            for await (const batch of readDataFile({
              dataEntry: entry,
              fileRowStart,
              fileRowEnd,
              schema,
              metadata: tableMetadata,
              resolver: fetchResolver,
              rowLineage,
              positionDeletesMap,
              equalityDeleteGroups,
              wantedColumns: scanColumns,
              filter,
              signal,
            })) {
              for (const row of batch) {
                if (signal?.aborted) { stop = true; break }
                yield asyncRow(row, rowColumns)
                remaining--
                if (remaining <= 0) { stop = true; break }
              }
              if (stop) break
            }
          }
        },
      }
    },
    /**
     * Streams a single column's values in row order as an async iterable of
     * native column chunks, without constructing intermediate row objects.
     * Contiguous numeric chunks retain their typed arrays. This is
     * squirreling's optional `AsyncDataSource.scanColumn` hook: its
     * `tryColumnScanAggregate` consumes it to compute a scalar aggregate
     * (`COUNT`/`MIN`/`MAX`/`SUM`/`AVG`, low-cardinality `COUNT(DISTINCT …)`) in
     * O(1)/O(cardinality) state. Engines can also consume prepareScan directly
     * to share one native scan across multiple aggregate columns.
     *
     * WHERE is pruned and pushed down exactly as in `scan`: whole data files
     * are dropped by partition tuple and per-column manifest bounds, and the
     * predicate is handed to hyparquet (row-group/page pruning plus per-row
     * matching) for supported parts of the predicate. Unsupported nodes
     * (LIKE, functions, arithmetic) leave `appliedWhere: false` so the consumer
     * must evaluate the remaining predicate with all required columns.
     * `appliedLimitOffset` mirrors `scan`: position-based reads require no WHERE,
     * deletes, or pruning. An exact WHERE can cap candidates at `offset+limit`;
     * a partial WHERE cannot cap them before the engine applies the residual.
     * `signal` aborts between chunks, mirroring `scan`.
     *
     * @param {object} options
     * @param {string} options.column - Name of the single column to stream.
     * @param {ExprNode} [options.where] - Row predicate; pruned/pushed when convertible.
     * @param {number} [options.limit] - Max number of values to yield.
     * @param {number} [options.offset] - Number of leading values to skip.
     * @param {AbortSignal} [options.signal] - Aborts the stream between chunks.
     * @returns {ScanColumnResults} Column-value chunks plus applied-hint flags.
     */
    scanColumn({ column, where, limit, offset, signal }) {
      const wantedColumns = [column]
      // Mirror scan(): convert WHERE, prune files by manifest bounds, and only
      // treat LIMIT/OFFSET as position-pushable when no WHERE is matched
      // per-row, no deletes shift positions, and no file was pruned.
      const exactFilter = whereToParquetFilter(where)
      const filter = exactFilter ?? whereToParquetFilter(where, { allowPartial: true })
      const appliedWhere = where !== undefined && exactFilter !== undefined
      const whereResolved = !where || appliedWhere
      const canPushOffset = !where && !hasDeletes
      const skip = canPushOffset ? offset ?? 0 : 0
      let take = Infinity
      if (whereResolved && limit !== undefined) {
        take = canPushOffset ? limit : (offset ?? 0) + limit
      }
      const appliedLimitOffset = canPushOffset
      return {
        appliedWhere,
        appliedLimitOffset,
        async *chunks() {
          if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
          if (take === 0) return
          const scanEntries = await pruneEntries(filter)
          if (scanEntries.length === 0) return

          const { positionDeletesMap, equalityDeleteGroups } = await deleteMapsPromise

          let remainingSkip = skip
          let remaining = take
          for (const entry of scanEntries) {
            if (remaining <= 0) break
            if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
            const recordCount = Number(entry.data_file.record_count)
            // Position-based file skip / per-file bound is only correct when
            // canPushOffset holds; a matched WHERE, deletes, or pruning break
            // the record_count-to-position mapping, so read whole files and
            // let the value-stream slice below (and the consumer) apply the
            // final LIMIT/OFFSET.
            let fileRowStart = 0
            if (canPushOffset && remainingSkip > 0) {
              if (remainingSkip >= recordCount) {
                remainingSkip -= recordCount
                continue
              }
              fileRowStart = remainingSkip
              remainingSkip = 0
            }
            const fileRowEnd = canPushOffset && remaining !== Infinity
              ? Math.min(recordCount, fileRowStart + remaining)
              : recordCount

            for await (const chunk of readDataFileColumn({
              dataEntry: entry,
              fileRowStart,
              fileRowEnd,
              schema,
              metadata: tableMetadata,
              resolver: fetchResolver,
              rowLineage,
              positionDeletesMap,
              equalityDeleteGroups,
              wantedColumns,
              column,
              limit: remaining,
              filter,
              signal,
            })) {
              if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
              // The native reader bounds the chunk before resolving payloads.
              if (chunk.length > 0) {
                yield chunk
                remaining -= chunk.length
              }
              if (remaining <= 0) break
            }
            if (remaining <= 0) break
          }
        },
      }
    },
  }
  return thisSource
}

/**
 * Flatten per-manifest entries, dropping logically deleted ones.
 *
 * @param {ManifestEntry[][]} perManifest
 * @returns {ManifestEntry[]}
 */
function liveEntries(perManifest) {
  /** @type {ManifestEntry[]} */
  const out = []
  for (const entries of perManifest) {
    for (const entry of entries) {
      if (entry.status !== 2) out.push(entry)
    }
  }
  return out
}
