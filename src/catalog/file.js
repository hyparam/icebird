/**
 * @import {FileCatalog, Resolver} from '../../src/types.js'
 */

/**
 * Construct a file-based catalog. The catalog is a thin discriminator over
 * a `Resolver` — file catalogs have no central registry; every table lives
 * at its own URL, and metadata commits go through `fileCatalogCommit` using
 * the supplied resolver.
 *
 * @param {object} options
 * @param {Resolver} options.resolver - Resolver used for both data file I/O and metadata commits. Must have `writer` for write operations.
 * @returns {FileCatalog}
 */
export function fileCatalog({ resolver }) {
  if (!resolver) throw new Error('resolver is required')
  return Object.freeze({ type: 'file', resolver })
}
