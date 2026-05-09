/**
 * @import {FileCatalog, Lister, Resolver} from '../../src/types.js'
 */

/**
 * Construct a file-based catalog. The catalog is a thin discriminator over
 * a `Resolver` — file catalogs have no central registry; every table lives
 * at its own URL, and metadata commits go through `fileCatalogCommit` using
 * the supplied resolver.
 *
 * @param {object} options
 * @param {Resolver} options.resolver - Resolver used for both data file I/O and metadata commits. Must have `writer` for write operations.
 * @param {Lister} [options.lister] - Optional lister for metadata-version discovery on tables without `version-hint.text`. Defaults to `s3Lister()` inside the metadata module.
 * @param {boolean} [options.conditionalCommits] - When true, table-creation writes `v1.metadata.json` with `If-None-Match: *` so two concurrent creators see one success and one 412/409. `version-hint.text` is treated as a best-effort cache. Default false preserves backwards-compatible (overwrite) behavior. (This slice does not yet apply conditional creates to subsequent commits.)
 * @returns {FileCatalog}
 */
export function fileCatalog({ resolver, lister, conditionalCommits }) {
  if (!resolver) throw new Error('resolver is required')
  /** @type {FileCatalog} */
  const cat = { type: 'file', resolver }
  if (lister) cat.lister = lister
  if (conditionalCommits) cat.conditionalCommits = true
  return Object.freeze(cat)
}
