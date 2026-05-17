import { executeSql, extractTables, parseSql } from 'squirreling'
import { loadTable } from '../catalog/loadTable.js'
import { urlResolver } from '../fetch.js'
import { icebergDataSource } from './icebergDataSource.js'

/**
 * @import {AsyncDataSource, QueryResults} from 'squirreling'
 * @import {Catalog, Resolver} from '../../src/types.js'
 */

/**
 * Run a SQL query across Iceberg tables. Tables can be supplied two ways:
 *
 * 1. `tables` — a map from SQL identifier (e.g. `"analytics.orders"`) to
 *    either a tableUrl string or a pre-built `AsyncDataSource`. Pre-built
 *    sources let callers pin a specific snapshot, metadata file, or resolver
 *    via `icebergDataSource(...)` before handing it to `icebergQuery`. Wins
 *    over the catalog when both define a ref.
 * 2. `catalog` — a REST or file catalog. SQL refs are split on `.` (last
 *    segment is the table, earlier segments are the namespace) and resolved
 *    via the catalog. Unquoted identifiers can't contain dots, so multi-segment
 *    refs require quoting, e.g. `FROM "analytics.orders"`.
 *
 * At least one of the two must cover every FROM/JOIN ref in the query. File
 * catalogs identify tables by URL rather than name, so SQL queries against a
 * file catalog must use `tables` to map each ref to its tableUrl.
 *
 * Returns the squirreling `QueryResults` where `result.rows()` is an
 * AsyncGenerator that drives parquet reads on demand; rows are not
 * materialized up front.
 *
 * @param {object} options
 * @param {Catalog} [options.catalog]
 * @param {string} options.query
 * @param {Record<string, string | AsyncDataSource>} [options.tables] - Map from SQL identifier to a tableUrl or a pre-built AsyncDataSource.
 * @param {Resolver} [options.resolver]
 * @param {AbortSignal} [options.signal]
 * @returns {Promise<QueryResults>}
 */
export async function icebergQuery({ catalog, query, tables, resolver, signal }) {
  if (!query) throw new Error('query is required')
  if (!catalog && !tables) throw new Error('catalog or tables is required')
  const catalogResolver = catalog?.type === 'file' ? catalog.resolver : undefined
  const fetchResolver = resolver ?? catalogResolver ?? urlResolver()
  const ast = parseSql({ query })
  const refs = extractTables(ast)

  if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
  const loaded = await Promise.all(refs.map(async ref => {
    const entry = tables?.[ref]
    if (entry !== undefined) {
      const source = typeof entry === 'string'
        ? await icebergDataSource({ tableUrl: entry, resolver: fetchResolver })
        : entry
      return /** @type {const} */ ([ref, source])
    }
    if (!catalog) throw new Error(`no source for table "${ref}" — pass tables[${JSON.stringify(ref)}] or a catalog that resolves it`)
    const { namespace, table } = splitRef(ref)
    const { metadata, tableUrl: resolvedUrl } = await loadTable({ catalog, namespace, table, resolver: fetchResolver })
    const source = await icebergDataSource({ tableUrl: resolvedUrl, metadata, resolver: fetchResolver })
    return /** @type {const} */ ([ref, source])
  }))
  if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
  /** @type {Record<string, AsyncDataSource>} */
  const sources = Object.fromEntries(loaded)

  return executeSql({ tables: sources, query: ast, signal })
}

/**
 * Split a SQL table reference like 'analytics.orders' into its namespace
 * and table parts. The last dot-separated segment is the table; everything
 * before is the namespace. A bare table name (`FROM bunnies`) is treated
 * as a root-namespace reference, the catalog decides whether that's valid.
 *
 * @param {string} ref
 * @returns {{ namespace: string[], table: string }}
 */
function splitRef(ref) {
  const parts = ref.split('.')
  const table = parts[parts.length - 1]
  const namespace = parts.slice(0, -1)
  return { namespace, table }
}
