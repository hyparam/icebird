import { executeSql, extractTables, parseSql } from 'squirreling'
import { restCatalogLoadTable } from '../catalog/rest.js'
import { urlResolver } from '../fetch.js'
import { icebergDataSource } from './icebergDataSource.js'

/**
 * @import {AsyncDataSource, QueryResults} from 'squirreling'
 * @import {RestCatalogContext, Resolver} from '../types.js'
 */

/**
 * Run a SQL query across Iceberg tables resolved from a REST catalog.
 *
 * The query is parsed once; every FROM / JOIN table reference is split on
 * '.' to derive a namespace and table name (last segment is the table,
 * earlier segments are the namespace), then loaded via the catalog. Unquoted
 * identifiers can't contain dots, so multi-segment refs require quoting,
 * e.g. `FROM "analytics.orders"`.
 *
 * Returns the squirreling `QueryResults` where `result.rows()` is an
 * AsyncGenerator that drives parquet reads on demand; rows are not
 * materialized up front.
 *
 * @param {object} options
 * @param {RestCatalogContext} options.catalog
 * @param {string} options.query
 * @param {Resolver} [options.resolver]
 * @param {AbortSignal} [options.signal]
 * @returns {Promise<QueryResults>}
 */
export async function icebergQuery({ catalog, query, resolver, signal }) {
  if (!catalog) throw new Error('catalog is required')
  if (!query) throw new Error('query is required')
  const fetchResolver = resolver ?? urlResolver()
  const ast = parseSql({ query })
  const refs = extractTables(ast)

  if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
  const loaded = await Promise.all(refs.map(async ref => {
    const { namespace, table } = splitRef(ref)
    const { metadata } = await restCatalogLoadTable(catalog, { namespace, table })
    const source = await icebergDataSource({
      tableUrl: metadata.location,
      metadata,
      resolver: fetchResolver,
    })
    return /** @type {const} */ ([ref, source])
  }))
  if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
  /** @type {Record<string, AsyncDataSource>} */
  const tables = Object.fromEntries(loaded)

  return executeSql({ tables, query: ast, signal })
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
