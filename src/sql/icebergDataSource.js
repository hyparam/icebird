import { asyncRow } from 'squirreling'
import { fetchDeleteMaps, urlResolver } from '../fetch.js'
import { icebergManifests, splitManifestEntries } from '../manifest.js'
import { icebergMetadata } from '../metadata.js'
import { readDataFile } from '../read.js'

/**
 * @import {AsyncDataSource} from 'squirreling'
 * @import {Lister, Resolver, TableMetadata} from '../types.js'
 */

/**
 * Creates a squirreling AsyncDataSource backed by an Iceberg table that streams
 * rows lazily from the underlying parquet data files (row group by row group,
 * row by row) instead of materializing everything up front.
 *
 * Metadata, manifests, schema, and delete maps are resolved once at
 * construction; each `scan()` walks the data files in record-count order and
 * yields rows on demand. WHERE is not pushed down: the engine applies it
 * after the scan. LIMIT/OFFSET is pushed down only when there is no WHERE
 * and the table has no delete files (LIMIT/OFFSET are in post-delete
 * coordinates, but record_count is pre-delete, so naive pushdown using
 * record_count miscounts visible rows in any file with applicable deletes).
 *
 * @param {object} options
 * @param {string} options.tableUrl - Base URL or path of the table.
 * @param {string} [options.metadataFileName] - Specific metadata file to load.
 * @param {TableMetadata} [options.metadata] - Pre-fetched table metadata.
 * @param {Resolver} [options.resolver] - I/O resolver (defaults to `urlResolver()`).
 * @param {Lister} [options.lister] - Directory lister, used to discover the latest metadata.
 * @returns {Promise<AsyncDataSource>}
 */
export async function icebergDataSource({ tableUrl, metadataFileName, metadata, resolver, lister }) {
  if (!tableUrl) throw new Error('tableUrl is required')
  const fetchResolver = resolver ?? urlResolver()
  const tableMetadata = metadata ?? await icebergMetadata({ tableUrl, metadataFileName, resolver: fetchResolver, lister })

  const currentSchemaId = tableMetadata['current-schema-id']
  const schema = tableMetadata.schemas.find(s => s['schema-id'] === currentSchemaId)
  if (!schema) throw new Error('current schema not found in metadata')
  const columns = schema.fields.map(f => f.name)
  const rowLineage = tableMetadata['format-version'] >= 3

  const manifestList = await icebergManifests(tableMetadata, fetchResolver)
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
      const applyLimitOffset = !where && !hasDeletes
      const skip = applyLimitOffset ? offset ?? 0 : 0
      const take = applyLimitOffset && limit !== undefined ? limit : Infinity

      return {
        appliedWhere: false,
        appliedLimitOffset: applyLimitOffset,
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
            // Pushdown is only enabled when the table has no deletes, so
            // record_count equals the visible row count for this file.
            const fileRowStart = remainingSkip < recordCount ? remainingSkip : recordCount
            const fileRowEnd = recordCount
            if (fileRowStart >= fileRowEnd) {
              remainingSkip -= recordCount
              continue
            }
            remainingSkip = 0

            for await (const row of readDataFile({
              dataEntry: entry,
              fileRowStart,
              fileRowEnd,
              schema,
              metadata: tableMetadata,
              resolver: fetchResolver,
              rowLineage,
              positionDeletesMap,
              equalityDeleteGroups,
              signal,
            })) {
              if (signal?.aborted) break
              yield asyncRow(row, rowColumns)
              remaining--
              if (remaining <= 0) break
            }
          }
        },
      }
    },
  }
}
