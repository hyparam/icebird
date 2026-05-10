import { loadLatestFileCatalogMetadata, resolveMetadata } from '../metadata.js'
import { restCatalogLoadTable } from './rest.js'

/**
 * @import {Catalog, Resolver, TableMetadata} from '../../src/types.js'
 */

/**
 * Resolve `(metadata, tableUrl, resolver)` for the catalog branch in use.
 * REST: load via the catalog API and read tableUrl from `metadata.location`.
 * File: read `metadata.json` directly via the resolver. When the file catalog
 * has `conditionalCommits` enabled, the load goes through
 * `loadLatestFileCatalogMetadata` so a stale or forward-corrupted
 * `version-hint.text` cannot brick subsequent writes: the hint is treated as
 * a starting probe rather than authoritative.
 *
 * @param {object} options
 * @param {Catalog} options.catalog
 * @param {string | string[]} [options.namespace] - Required for REST catalogs.
 * @param {string} [options.table] - Required for REST catalogs.
 * @param {string} [options.tableUrl] - Required for file catalogs (table base URL).
 * @param {Resolver} [options.resolver] - Required for REST catalogs (data file I/O); optional for file catalogs (defaults to `catalog.resolver`).
 * @returns {Promise<{metadata: TableMetadata, metadataFileName: string | undefined, version?: number, tableUrl: string, resolver: Resolver | undefined}>}
 */
export async function loadTable({ catalog, namespace, table, tableUrl, resolver }) {
  if (catalog.type === 'rest') {
    if (!namespace || !table) throw new Error('namespace and table are required for rest catalogs')
    const { metadata } = await restCatalogLoadTable(catalog, { namespace, table })
    return { metadata, metadataFileName: undefined, tableUrl: metadata.location, resolver }
  }
  if (catalog.type === 'file') {
    if (!tableUrl) throw new Error('tableUrl is required for file catalogs')
    const eff = resolver ?? catalog.resolver
    if (catalog.conditionalCommits) {
      const { metadata, metadataFileName, version } =
        await loadLatestFileCatalogMetadata({ tableUrl, resolver: eff, lister: catalog.lister })
      return { metadata, metadataFileName, version, tableUrl, resolver: eff }
    }
    const { metadata, metadataFileName } =
      await resolveMetadata({ tableUrl, resolver: eff, lister: catalog.lister })
    return { metadata, metadataFileName, tableUrl, resolver: eff }
  }
  throw new Error(`unknown catalog type: ${/** @type {any} */ (catalog)?.type}`)
}
