# Icebird Changelog

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
