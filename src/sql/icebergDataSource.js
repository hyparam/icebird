import { asyncRow } from 'squirreling'
import { fetchDeleteMaps, urlResolver } from '../fetch.js'
import { icebergManifests, splitManifestEntries } from '../manifest.js'
import { icebergMetadata } from '../metadata.js'
import { readDataFile } from '../read.js'
import { partitionMightMatch } from '../prune.js'
import { whereToParquetFilter } from './whereFilter.js'

/**
 * @import {AsyncDataSource} from 'squirreling'
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
 * - WHERE is pushed down to hyparquet (row-group pruning via statistics and
 *   bloom filters, plus per-row matching) when the expression can be fully
 *   converted to a parquet filter (comparisons, IN, AND/OR/NOT on identifier
 *   vs literal). Unsupported nodes (LIKE, functions, arithmetic, identifier
 *   vs identifier) leave WHERE for the engine to apply.
 * - When WHERE is resolved at scan time (either absent or fully pushed) we
 *   cap the scan at `offset + limit` rows so the source terminates early.
 *   OFFSET is also pushed into the parquet seek when there are no deletes;
 *   with deletes the engine still applies OFFSET itself (record_count is
 *   pre-delete, so seeking by record_count would miscount visible rows in
 *   any file with applicable deletes).
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
      // Partition-level scan pruning: drop data files whose partition tuple
      // proves no row can match the filter. Manifest entries are already in
      // memory, so this is a cheap synchronous pre-filter that avoids opening
      // the pruned files entirely. Pruning never drops a file with a matching
      // row, so query results are unchanged.
      const scanEntries = filter
        ? dataEntries.filter(entry => partitionMightMatch(filter, entry, schema, tableMetadata))
        : dataEntries
      const pruned = scanEntries.length < dataEntries.length
      // Treat a fully-pushed-down WHERE the same as "no WHERE" for the
      // purpose of LIMIT/OFFSET pushdown.
      const whereResolved = !where || appliedWhere
      // OFFSET pushdown (seeking past rows in the parquet file) is only safe
      // when the WHERE is fully resolved at scan time AND the table has no
      // deletes: record_count is pre-delete, so seeking by it would skip the
      // wrong visible rows. It also assumes the cumulative record_count tracks
      // row positions, so disable it once pruning has removed any file.
      const canPushOffset = whereResolved && !hasDeletes && !pruned
      const skip = canPushOffset ? offset ?? 0 : 0
      // LIMIT pushdown (early termination) is safe whenever WHERE is
      // resolved: with deletes we yield offset+limit rows and let the engine
      // apply the slice, which still saves reading later files.
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
  }
}
