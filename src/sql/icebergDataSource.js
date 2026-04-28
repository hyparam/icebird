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
 * after the scan. LIMIT/OFFSET is pushed down only when there is no WHERE.
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

  // Pre-fetch delete maps once; reused by every scan.
  const deleteMapsPromise = fetchDeleteMaps(deleteEntries, fetchResolver)

  // Sum record_count across data manifest entries. Overstates the count when
  // delete files are present, but it's only a hint for the engine.
  let numRows = 0
  for (const entry of dataEntries) {
    numRows += Number(entry.data_file.record_count)
  }

  return {
    numRows,
    columns,
    scan({ columns: scanColumns, where, limit, offset, signal }) {
      const rowColumns = scanColumns ?? columns
      const applyLimitOffset = !where
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
            // Skip the entire file if its row range is entirely before `skip`.
            // Note: post-delete row counts can be lower than recordCount, so
            // when deletes are present we have to actually read and discard.
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
