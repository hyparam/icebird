# Icebird Changelog

## [0.8.11]
 - Fix pushed-down `where` combined with `limit`/`offset` silently dropping matching rows

## [0.8.10]
 - Support format-version 3 tables in `icebergRewrite`, preserving row lineage (`_row_id` and `_last_updated_sequence_number`) across rewrites

## [0.8.9]
 - Prune data files using manifest column bounds when pushed-down filters prove they cannot match
 - Sort appended data files by the table's default sort order and record the real sort order id
 - Add `icebergRewrite` to compact live rows into consolidated, sorted replacement data files

## [0.8.8]
 - Prune data files in the SQL data source whose partition values cannot match the pushed-down `where` filter, skipping their parquet footer and data reads

## [0.8.7]
 - Match identity partition fields by their transform rather than source field id, so partition values resolve correctly when a column has multiple transforms
 - Normalize a null identity partition value to `null` on projection

## [0.8.6]
 - Push down `where` filters into parquet reads so unmatched row groups and pages are skipped
 - Validate partition specs and transform specs when creating or updating tables
 - Fix `uuid` partitioning to follow the spec, and canonicalize uuid partition keys across string and byte values
 - Distinguish `-0.0` and `+0.0` in float partition keys
 - Fix identity partition fallback when a data file omits a partition value

## [0.8.5]
 - Parallelize row group reads for faster data file scans
 - Pass manifest lengths to the resolver so cached/range-aware resolvers can fetch manifests in a single request

## [0.8.4]
 - Write support for Iceberg list, map, and struct types
 - Validate field ids against the reserved range when adding columns to an existing table

## [0.8.3]
 - New `cachingResolver` wraps a `Resolver` to dedupe reads of the same path and share an in-memory buffer across range reads; writes and deletes invalidate the cached entry on success

## [0.8.2]
 - Use the schema from the pinned snapshot when reading via `snapshotId`, instead of the current schema

## [0.8.1]
 - `icebergDataSource` accepts `snapshotId` and forwards it to `icebergManifests`, so SQL sources can time-travel like `icebergRead`
 - `icebergQuery`'s `tables` map now accepts either a tableUrl string or a pre-built `AsyncDataSource`. Pre-built sources let callers pin a specific snapshot, metadata file, or resolver via `icebergDataSource(...)` before handing the source to `icebergQuery`

## [0.8.0]
 - REST catalog: read routing prefix from `overrides.prefix` per the Iceberg REST spec; fixes Cloudflare R2 Data Catalog
 - `icebergRead` accepts `snapshotId` for time travel; defaults to the current snapshot
 - Snapshot ids exceeding `Number.MAX_SAFE_INTEGER` (2^53-1) are now preserved as `BigInt` instead of being truncated. Affected fields are typed `number | bigint` and comparisons coerce to `BigInt`
 - Pass raw `s3://` paths to custom resolvers on the write path instead of pre-translating to AWS hostnames; lets custom resolvers (R2, MinIO, etc.) handle their own URL translation. The default `urlResolver` still maps `s3://` to AWS S3 internally
 - Manifest list `partitions` array now carries the required `element-id`
 - Parquet column metadata now carries the iceberg `field_id`
 - Emit lower/upper bounds for `date` and `time` columns
 - New `s3SignedResolver` for private S3-compatible buckets (AWS, R2, MinIO)
 - Export `collect` so `icebergQuery` results can be materialized
 - Breaking: `icebergManifests` now takes a single options object `{ metadata, resolver, snapshotId }`

## [0.7.0]
 - Breaking: Avro functions moved to `icebird/avro` subpackage and removed from the top-level export
 - Configure commit retries via `commit.retry.*` table properties instead of write-call overrides
 - Avro UUID type read and write support
 - Validate field ids against the reserved range when creating tables
 - List `metadata/` by default for file-catalog metadata discovery
 - Clearer error message when a transaction commit fails
 - Fix commit race conditions

## [0.6.0]
 - Conditional commits via `If-None-Match` for the file catalog
 - Retry concurrent commits under `conditionalCommits` with exponential backoff and jitter
 - New `loadLatestFileCatalogMetadata` discovery primitive

## [0.5.2]
 - Variant type read support
 - Fix deletion vector issues; replace existing position delete files when writing a deletion vector
 - Hash nanosecond timestamps at microsecond precision
 - Fix `rowStart` / `rowEnd` in the presence of deletes
 - Update `last-updated-ms` on snapshot writes
 - Fix partition write edge cases and partition fixed type conversion

## [0.5.1]
 - `icebergQuery` works against non-REST catalogs, including a default file catalog when no resolver is passed

## [0.5.0]
 - SQL query engine via `icebergQuery` and `icebergDataSource`, integrating with squirreling
 - `initial-default` applied for columns added after data was written
 - Thread metadata through write APIs
 - Emit date and time values per spec
 - Parse both `vN.metadata.json` and `NNNNN-<uuid>.metadata.json` filenames
 - `Lister` support in write APIs

## [0.4.1]
 - Transactions: stage multiple writes and commit atomically
 - Partitioned table writes

## [0.4.0]
 - Writer: `icebergCreateTable`, `icebergDropTable`, `icebergAppend`, `icebergDelete` with pluggable catalog type
 - Puffin file reader and writer, including deletion vector support
 - REST Catalog: create/drop/rename tables, register table, update table, create/delete namespaces, load credentials
 - Partition support including hidden partitions and the Void transform
 - Iceberg V3 types, row lineage, and V3 equality delete matching
 - Sort order, geospatial stats, decimal types
 - Parallelized data file reads
 - `Resolver` / `Lister` abstraction for pluggable storage backends
 - Async hyparquet writer
 - Gzip metadata JSON support

## [0.3.1]
 - `icebergCreate` to create new Iceberg tables

## [0.3.0]
 - Avro writer
 - Reorganize files in the repo

## [0.2.0]
 - Rename `IcebergMetadata` to `TableMetadata`
 - Column projection for missing field ids
 - Fix partition types

## [0.1.15]
 - V1 manifests default `sequence_number` to 0
 - Fix sequence number inheritance

## [0.1.14]
 - `icebergListVersions`

## [0.1.13]
 - Support AWS Athena tables

## [0.1.12]
 - Authentication via `requestInit`
 - Use `file_size_in_bytes` from metadata to skip HEAD request
 - Fetch error handling
 - Column rename support

## [0.1.11]
 - Avro logical types
 - Only fetch delete maps once

## [0.1.10]
 - Fix equality deletes
 - Inherit `sequence_number` on manifest entries

## [0.1.0]
 - Initial release
