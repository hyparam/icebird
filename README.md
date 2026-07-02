# Icebird: JavaScript Iceberg Client

![Iceberg Icebird](icebird.jpg)

[![npm](https://img.shields.io/npm/v/icebird)](https://www.npmjs.com/package/icebird)
[![minzipped](https://img.shields.io/bundlephobia/minzip/icebird)](https://www.npmjs.com/package/icebird)
[![workflow status](https://github.com/hyparam/icebird/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/icebird/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-87-darkred)

Icebird is a JavaScript client for [Apache Iceberg](https://iceberg.apache.org/) tables. It reads and writes Iceberg v1/v2/v3 tables, runs SQL queries over them, and speaks to file-based or REST catalogs. It is built on top of [hyparquet](https://github.com/hyparam/hyparquet) and [hyparquet-writer](https://github.com/hyparam/hyparquet-writer) for the underlying parquet I/O.

> Part of **[HypStack](https://hypstack.ai/)**, an open-source stack for AI observability.

## Usage

To read an Iceberg table:

```javascript
const { icebergRead } = await import('icebird')

const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'
const data = await icebergRead({
  tableUrl,
  rowStart: 0,
  rowEnd: 10,
})
```

To read the Iceberg metadata (schema, etc):

```javascript
import { icebergMetadata } from 'icebird'

const metadata = await icebergMetadata({ tableUrl })

// subsequent reads will be faster if you provide the metadata:
const data = await icebergRead({
  tableUrl,
  metadata,
})
```

## Demo

Check out a minimal iceberg table viewer demo that shows how to integrate Icebird into a react web application using [HighTable](https://github.com/hyparam/hightable) to render the table data. You can view any publicly accessible Iceberg table:

 - **Live Demo**: [https://hyparam.github.io/demos/icebird/](https://hyparam.github.io/demos/icebird/)
 - **Demo Source Code**: [https://github.com/hyparam/demos/tree/master/icebird](https://github.com/hyparam/demos/tree/master/icebird)

## Time Travel

To fetch a previous version of the table, you can specify `metadataFileName`:

```javascript
import { icebergRead } from 'icebird'

const data = await icebergRead({
  tableUrl,
  metadataFileName: 'v1.metadata.json',
})
```

## Authentication

To add authentication or other custom `fetch` options, create a resolver and lister with `requestInit` and pass those into the public APIs:

```javascript
import { icebergMetadata, icebergRead, s3Lister, urlResolver } from 'icebird'

const requestInit = {
  headers: {
    Authorization: 'Bearer my_token',
  },
}

const resolver = urlResolver({ requestInit })
const lister = s3Lister({ requestInit })

const metadata = await icebergMetadata({
  tableUrl,
  resolver,
  lister,
})

const data = await icebergRead({
  tableUrl,
  metadata,
  resolver,
  lister,
})
```

For private S3-compatible buckets (AWS, Cloudflare R2, MinIO), use `s3SignedResolver` which signs SigV4 via Web Crypto so it works in browsers and Node:

```javascript
import { icebergRead, s3SignedResolver } from 'icebird'

const resolver = s3SignedResolver({
  accessKeyId, secretAccessKey, region: 'us-east-1',
  // For R2/MinIO, set endpoint and pathStyle:
  // endpoint: 'https://<acct>.r2.cloudflarestorage.com', pathStyle: true,
})
const data = await icebergRead({ tableUrl: 's3://my-bucket/warehouse/orders', resolver })
```

## REST Catalog

For tables behind an [Iceberg REST Catalog](https://iceberg.apache.org/rest-catalog-spec/), connect via `restCatalogConnect` and pass the loaded metadata into `icebergRead`. Multi-level namespaces are arrays.

```javascript
import { icebergRead, restCatalogConnect, restCatalogLoadTable } from 'icebird'

const ctx = await restCatalogConnect({ url: 'https://catalog.example.com' })
const { metadata } = await restCatalogLoadTable(ctx, { namespace: 'analytics', table: 'orders' })
const data = await icebergRead({ tableUrl: metadata.location, metadata })
```

For Amazon S3 Tables, use the optional [`icebird/s3tables`](#amazon-s3-tables) subpath (read-only in this release).

## SQL

Icebird ships a SQL engine on top of [squirreling](https://github.com/hyparam/squirreling). `icebergQuery` runs a SQL query across one or more iceberg tables. Rows are streamed lazily. Multi-segment namespaces in the SQL `FROM` clause must be dot-separated and quoted: `FROM "analytics.orders"` resolves to namespace `analytics`, table `orders`.

```javascript
import { collect, icebergQuery, restCatalogConnect } from 'icebird'

const catalog = await restCatalogConnect({ url: 'https://catalog.example.com' })
const result = await icebergQuery({
  catalog,
  query: 'SELECT "Breed Name", "Popularity Rank" FROM "java.bunnies" WHERE "Popularity Rank" <= 3 ORDER BY "Popularity Rank"',
})
const rows = await collect(result)
```

## Amazon S3 Tables

Read-only support for [Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-tables.html) lives in the optional `icebird/s3tables` subpath. The main `icebird` export has **no AWS dependency**, and requests are signed with icebird's own SigV4 implementation. The only optional dependency is `@aws-sdk/credential-providers`, used to resolve the default AWS credential chain — and even that is imported lazily, so passing explicit credentials needs no AWS SDK at all.

Install the peer dependency only if you rely on the default credential chain:

```bash
npm install icebird @aws-sdk/credential-providers
```

Connect with the default AWS credential chain (env vars, shared config, IAM role on Lambda/EC2):

```javascript
import { icebergRead } from 'icebird'
import { loadS3TablesTable, s3TablesCatalogConnectFromEnv } from 'icebird/s3tables'

const catalog = await s3TablesCatalogConnectFromEnv({
  region: 'us-east-1',
  tableBucketArn: 'arn:aws:s3tables:us-east-1:111122223333:bucket/my-bucket',
})
const { metadata, tableUrl, resolver } = await loadS3TablesTable({
  catalog, namespace: 'analytics', table: 'orders',
})
const rows = await icebergRead({ tableUrl, metadata, resolver })
```

Or pass explicit credentials:

```javascript
import { icebergRead, restCatalogLoadTable } from 'icebird'
import { loadS3TablesTable, s3TablesCatalogConnect, s3TablesResolver } from 'icebird/s3tables'

const creds = { region: 'us-east-1', accessKeyId, secretAccessKey }
const catalog = await s3TablesCatalogConnect({
  ...creds,
  tableBucketArn: 'arn:aws:s3tables:us-east-1:111122223333:bucket/my-bucket',
})

const resolver = await s3TablesResolver(creds)
const { metadata } = await restCatalogLoadTable(catalog, { namespace: 'analytics', table: 'orders' })
const rows = await icebergRead({ tableUrl: metadata.location, metadata, resolver })
```

**IAM (read-only):** grant `s3tables:GetTableBucket`, `s3tables:ListNamespaces`, `s3tables:GetNamespace`, `s3tables:ListTables`, `s3tables:GetTable`, `s3tables:GetTableMetadataLocation`, and `s3tables:GetTableData` on your table bucket and tables.

**Limitations:** S3 Tables namespaces are single-level only. `s3Lister` does not work on table-bucket warehouse paths (use the REST catalog to load metadata). Writes, Glue REST endpoint, and OAuth are not supported via this subpath yet.

## Writing

Icebird has experimental write support for Iceberg v2 (and v3 deletion vectors). All write functions take a `Catalog` and dispatch internally — the same call works against `fileCatalog({ resolver })` or a REST catalog context returned by `restCatalogConnect`.

```javascript
import {
  fileCatalog,
  icebergAppend,
  icebergCreateTable,
  icebergDelete,
  icebergExpireSnapshots,
  icebergRewrite,
  icebergSetRef,
} from 'icebird'

// `urlResolver()` ships with a `writer` (HTTP PUT) and `deleter` (HTTP DELETE);
// pass a custom `requestInit` to it for auth headers. For non-HTTP backends,
// supply your own `Resolver` with `writer` and (for drop) `deleter`.
const catalog = fileCatalog({ resolver })
const tableUrl = 's3://my-bucket/warehouse/orders'

const schema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
}

await icebergCreateTable({ catalog, tableUrl, schema })
await icebergAppend({ catalog, tableUrl, records: [{ id: 1n, name: 'alice' }] })

// position deletes — v3 writes deletion vectors; v2 writes parquet delete files
await icebergDelete({
  catalog, tableUrl,
  deletes: [{ file_path: 's3://.../data/abc.parquet', pos: 0 }],
})

// snapshot management
await icebergSetRef({ catalog, tableUrl, ref: 'main', snapshotId })
await icebergExpireSnapshots({ catalog, tableUrl, snapshotIds: [oldSnapshotId] })
```

If the table is created with a `sortOrder`, `icebergAppend` orders the rows in each written file by that order (tightening per-file column bounds for scan pruning). `icebergRewrite` compacts the current snapshot — reading every live row (deletes applied), sorting globally, and rewriting into consolidated, non-overlapping files via a `replace` snapshot (v2 tables):

```javascript
// compact small files into sorted, non-overlapping ones
await icebergRewrite({ catalog, tableUrl })
// optionally split large partitions and/or re-partition under another spec
await icebergRewrite({ catalog, tableUrl, targetFileRows: 1_000_000, partitionSpecId: 1 })
```

A rewrite is not retried on a concurrent commit (it would risk dropping rows another writer appended meanwhile); on conflict it throws and should be re-run against fresh metadata.

For a REST catalog, swap `fileCatalog(...)` for the connect context and pass `namespace`/`table` instead of `tableUrl`:

```javascript
const catalog = await restCatalogConnect({ url: 'https://catalog.example.com' })
await icebergAppend({ catalog, namespace: 'analytics', table: 'orders', records })
```

`icebergDropTable` on a file catalog requires a `lister` to enumerate files; pass `purgeRequested: true` to also delete `data/`.

## Supported Features

Icebird aims to support reading any Iceberg table, but currently only supports a subset of the features. The following features are supported:

| Feature | Supported | Notes |
| ------- | --------- | ----- |
| Read Iceberg v1 Tables | ✅ | |
| Read Iceberg v2 Tables | ✅ | |
| Read Iceberg v3 Tables | ✅ | |
| Write Iceberg v2 Tables | ✅ | |
| Write Iceberg v3 Tables | ✅ | |
| Parquet Storage | ✅ | |
| Avro Storage | ✅ | |
| ORC Storage | ❌ | |
| Puffin Storage | ⚠️ | Supports uncompressed `deletion-vector-v1` blobs only. |
| File-based Catalog (version-hint.text) | ✅ | |
| REST Catalog | ✅ | |
| Hive Catalog | ❌ | |
| Glue Catalog | ❌ | |
| Service-based Catalog | ❌ | |
| Position Deletes | ✅ | Supports Parquet position delete files and Puffin deletion vectors. |
| Equality Deletes | ✅ | |
| Binary Deletion Vectors | ✅ | Supports uncompressed Puffin `deletion-vector-v1` blobs. |
| Delete Partition Scope | ✅ | Applies sequence and partition scope before filtering rows. |
| Rename Columns | ✅ | |
| All Parquet Compression Codecs | ✅ | |
| All Parquet Types | ✅ | |
| Variant Types | ✅ | |
| Geometry Types | ✅ | |
| Geography Types | ✅ | |
| Row Lineage | ✅ | v3 `_row_id` and `_last_updated_sequence_number` inheritance. |
| Sorting | ✅ | Orders rows by the declared sort order on append; `icebergRewrite` compacts to sorted, non-overlapping files (v2). |
| Scan Pruning | ✅ | Skips data files via partition tuples and manifest column bounds, and parquet row groups via column statistics. |
| Encryption | ❌ | |

## References

 - https://iceberg.apache.org/spec/
 - https://avro.apache.org/docs/1.12.0/specification/
 - https://github.com/hyparam/hyparquet
 - https://github.com/hyparam/hyparquet-writer
 - https://github.com/apache/iceberg
 - https://github.com/apache/iceberg-python
 - https://github.com/apache/iceberg-rust
