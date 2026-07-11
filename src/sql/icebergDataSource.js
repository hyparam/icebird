import { asyncRow } from 'squirreling'
import { fetchDeleteMaps, urlResolver } from '../fetch.js'
import { icebergManifests, splitManifestEntries } from '../manifest.js'
import { icebergMetadata } from '../metadata.js'
import { readDataFile } from '../read.js'
import { fileMightMatch, partitionMightMatch } from '../prune.js'
import { whereToParquetFilter } from './whereFilter.js'

/**
 * @import {AsyncDataSource, ExprNode, SqlPrimitive} from 'squirreling'
 * @import {ScanColumnResults} from 'squirreling/src/types.js'
 * @import {Lister, Resolver, TableMetadata} from '../../src/types.js'
 */

/**
 * Creates a squirreling AsyncDataSource backed by an Iceberg table that streams
 * rows lazily from the underlying parquet data files (row group by row group,
 * row by row) instead of materializing everything up front.
 *
 * Metadata, manifests, schema, and delete maps are resolved once at
 * construction; each `scan()` walks the data files in record-count order and
 * yields rows on demand. Pushdowns:
 * - Column projection (`columns`) is pushed into the parquet read so only the
 *   requested columns are decoded. Equality-delete predicate columns and row
 *   lineage columns are read regardless when needed.
 * - WHERE prunes whole data files before they are opened, using each manifest
 *   entry's partition tuple and per-column `lower_bounds`/`upper_bounds`, and
 *   is pushed down to hyparquet (row-group pruning via statistics and bloom
 *   filters, plus per-row matching) when the expression can be fully converted
 *   to a parquet filter (comparisons, IN, AND/OR/NOT on identifier vs literal).
 *   Unsupported nodes (LIKE, functions, arithmetic, identifier vs identifier)
 *   leave WHERE for the engine to apply.
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
 * @returns {Promise<AsyncDataSource>}
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
  const columns = schema.fields.map(f => f.name)
  const rowLineage = tableMetadata['format-version'] >= 3

  const manifestList = await icebergManifests({ metadata: tableMetadata, resolver: fetchResolver, snapshotId })
  const { dataEntries, deleteEntries } = splitManifestEntries(manifestList)
  const hasDeletes = deleteEntries.length > 0

  // Pre-fetch delete maps once; reused by every scan.
  const deleteMapsPromise = fetchDeleteMaps(deleteEntries, fetchResolver)

  // Sum record_count across data manifest entries for an exact row count.
  // When delete files exist the sum is pre-delete and overstates the visible
  // count, so leave numRows undefined rather than reporting a wrong total.
  /** @type {number | undefined} */
  let numRows
  if (!hasDeletes) {
    numRows = 0
    for (const entry of dataEntries) {
      numRows += Number(entry.data_file.record_count)
    }
  }

  return {
    numRows,
    columns,
    scan({ columns: scanColumns, where, limit, offset, signal }) {
      const rowColumns = scanColumns ?? columns
      // Convert the WHERE AST to a hyparquet filter; undefined means the
      // expression has parts we can't push down (LIKE, functions, etc.) and
      // the engine must re-apply it.
      const filter = whereToParquetFilter(where)
      const appliedWhere = where !== undefined && filter !== undefined
      // Scan pruning: drop data files whose partition tuple OR per-column
      // manifest bounds prove no row can match the filter. Manifest entries are
      // already in memory, so this is a cheap synchronous pre-filter that
      // avoids opening the pruned files entirely. Both pruners are inclusive
      // projections (they never drop a file with a matching row), so query
      // results are unchanged.
      const scanEntries = filter
        ? dataEntries.filter(entry =>
          partitionMightMatch(filter, entry, schema, tableMetadata) &&
            fileMightMatch(filter, entry, schema))
        : dataEntries
      const pruned = scanEntries.length < dataEntries.length
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
      // deletes (record_count is pre-delete) or once pruning has dropped a
      // file (cumulative record_count no longer tracks row positions). In all
      // those cases we keep emitting up to `offset + limit` matched rows and
      // let the engine apply the final LIMIT/OFFSET slice.
      const canPushOffset = !where && !hasDeletes && !pruned
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
          if (take === 0 || scanEntries.length === 0) return

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
     * chunks (one chunk per parquet row group), so peak memory is bounded by a
     * single row group's worth of values rather than the whole table. This is
     * squirreling's optional `AsyncDataSource.scanColumn` hook: its
     * `tryColumnScanAggregate` consumes it to compute a scalar aggregate
     * (`COUNT`/`MIN`/`MAX`/`SUM`/`AVG`, low-cardinality `COUNT(DISTINCT …)`) in
     * O(1)/O(cardinality) state; without the hook the engine falls back to
     * buffering every scanned row.
     *
     * WHERE is pruned and pushed down exactly as in `scan`: whole data files
     * are dropped by partition tuple and per-column manifest bounds, and the
     * predicate is handed to hyparquet (row-group/page pruning plus per-row
     * matching) when it fully converts to a parquet filter. Unsupported nodes
     * (LIKE, functions, arithmetic) leave `appliedWhere: false` and the engine
     * re-applies the predicate over the emitted values. `appliedLimitOffset`
     * mirrors `scan`: OFFSET is pushed into the per-file seek and LIMIT bounds
     * the read only when WHERE is fully resolved AND there is no WHERE, no
     * deletes, and no pruning (position no longer tracks result rows
     * otherwise); in every other case the source emits at most `offset+limit`
     * values and the consumer applies the final slice. `signal` aborts between
     * chunks, mirroring `scan`.
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
      const filter = whereToParquetFilter(where)
      const appliedWhere = where !== undefined && filter !== undefined
      const scanEntries = filter
        ? dataEntries.filter(entry =>
          partitionMightMatch(filter, entry, schema, tableMetadata) &&
            fileMightMatch(filter, entry, schema))
        : dataEntries
      const pruned = scanEntries.length < dataEntries.length
      const whereResolved = !where || appliedWhere
      const canPushOffset = !where && !hasDeletes && !pruned
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
          if (take === 0 || scanEntries.length === 0) return

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
              wantedColumns,
              filter,
              signal,
            })) {
              if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
              // OFFSET is either already spent at fileRowStart (canPushOffset)
              // or deferred to the consumer via appliedLimitOffset: false, so
              // batches start at 0 here. `take` still caps the emitted count.
              let end = batch.length
              if (remaining !== Infinity && end > remaining) {
                end = remaining
                stop = true
              }
              /** @type {SqlPrimitive[]} */
              const chunk = []
              for (let i = 0; i < end; i++) chunk.push(batch[i][column])
              if (chunk.length > 0) {
                yield chunk
                remaining -= chunk.length
              }
              if (stop || remaining <= 0) break
            }
            if (stop || remaining <= 0) break
          }
        },
      }
    },
  }
}
