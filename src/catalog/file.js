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
 * @returns {FileCatalog}
 */
export function fileCatalog({ resolver, lister }) {
  if (!resolver) throw new Error('resolver is required')
  return Object.freeze({ type: 'file', resolver, lister })
}
