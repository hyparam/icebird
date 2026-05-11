/**
 * @import {FileCatalog, Lister, Resolver} from '../../src/types.js'
 */

/**
 * Construct a file-based catalog. The catalog is a thin discriminator over
 * a `Resolver`: file catalogs have no central registry; every table lives
 * at its own URL, and metadata commits go through `fileCatalogCommit` using
 * the supplied resolver.
 *
 * @param {object} options
 * @param {Resolver} options.resolver - Resolver used for both data file I/O and metadata commits. Must have `writer` for write operations.
 * @param {Lister} [options.lister] - Optional lister for metadata-version discovery on tables without `version-hint.text`. Defaults to `s3Lister()` inside the metadata module.
 * @param {boolean} [options.conditionalCommits] - When true, every metadata commit (`v1.metadata.json` and each subsequent `vN+1.metadata.json`) is created with `If-None-Match: *`, so two concurrent writers see one success and one 412/409. The high-level write functions (`icebergAppend`, `icebergDelete`, `icebergSetRef`, `icebergExpireSnapshots`) retry on 412/409 by reloading the latest metadata via `loadLatestFileCatalogMetadata` and re-staging, with policy taken from the table's `commit.retry.*` properties. `icebergCreateTable` and `icebergTransaction` do not retry (initial create has nothing to rebase; transactions can't safely re-run their callback, and a conflicting transaction throws `IcebergTransactionConflictError` with its staged files cleaned up best-effort). `version-hint.text` is a best-effort cache. Default false preserves backwards-compatible (overwrite) behavior.
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
