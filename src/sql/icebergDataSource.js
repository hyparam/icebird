import { asyncRow } from 'squirreling'
import { fetchDeleteMaps, urlResolver } from '../fetch.js'
import { icebergManifests, splitManifestEntries } from '../manifest.js'
import { icebergMetadata } from '../metadata.js'
import { readDataFile } from '../read.js'

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
 * - WHERE is not pushed down (the engine applies it after the scan), so when
 *   it is present we can't bound the scan by limit/offset.
 * - When there is no WHERE we always cap the scan at `offset + limit` rows so
 *   the source terminates early. OFFSET is also pushed into the parquet seek
 *   when there are no deletes; with deletes the engine still applies OFFSET
 *   itself (record_count is pre-delete, so seeking by record_count would
 *   miscount visible rows in any file with applicable deletes).
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

  const currentSchemaId = tableMetadata['current-schema-id']
  const schema = tableMetadata.schemas.find(s => s['schema-id'] === currentSchemaId)
  if (!schema) throw new Error('current schema not found in metadata')
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
      // OFFSET pushdown (seeking past rows in the parquet file) is only safe
      // when no WHERE is in play and the table has no deletes - record_count
      // is pre-delete, so seeking by it would skip the wrong visible rows.
      const canPushOffset = !where && !hasDeletes
      const skip = canPushOffset ? offset ?? 0 : 0
      // LIMIT pushdown (early termination) is safe whenever there is no
      // WHERE: with deletes we yield offset+limit rows and let the engine
      // apply the slice, which still saves reading later files.
      let take = Infinity
      if (!where && limit !== undefined) {
        take = canPushOffset ? limit : (offset ?? 0) + limit
      }
      const appliedLimitOffset = canPushOffset

      return {
        appliedWhere: false,
        appliedLimitOffset,
        async *rows() {
          if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
          if (take === 0 || dataEntries.length === 0) return

          const { positionDeletesMap, equalityDeleteGroups } = await deleteMapsPromise

          let remainingSkip = skip
          let remaining = take
          for (const entry of dataEntries) {
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
