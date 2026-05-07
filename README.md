# Icebird: JavaScript Iceberg Reader

![Iceberg Icebird](icebird.jpg)

[![npm](https://img.shields.io/npm/v/icebird)](https://www.npmjs.com/package/icebird)
[![minzipped](https://img.shields.io/bundlephobia/minzip/icebird)](https://www.npmjs.com/package/icebird)
[![workflow status](https://github.com/hyparam/icebird/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/icebird/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-87-darkred)

Icebird is a library for reading [Apache Iceberg](https://iceberg.apache.org/) tables in JavaScript. It is built on top of [hyparquet](https://github.com/hyparam/hyparquet) for reading the underlying parquet files.

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

## REST Catalog

For tables behind an [Iceberg REST Catalog](https://iceberg.apache.org/rest-catalog-spec/), connect via `restCatalogConnect` and pass the loaded metadata into `icebergRead`. Multi-level namespaces are arrays.

```javascript
import { icebergRead, restCatalogConnect, restCatalogLoadTable } from 'icebird'

const ctx = await restCatalogConnect({ url: 'https://catalog.example.com' })
const { metadata } = await restCatalogLoadTable(ctx, { namespace: 'analytics', table: 'orders' })
const data = await icebergRead({ tableUrl: metadata.location, metadata })
```

## SQL

Icebird ships a SQL engine on top of [squirreling](https://github.com/hyparam/squirreling). `icebergQuery` runs a SQL query across one or more iceberg tables. Rows are streamed lazily. Multi-segment namespaces in the SQL `FROM` clause must be dot-separated and quoted: `FROM "analytics.orders"` resolves to namespace `analytics`, table `orders`.

```javascript
import { icebergQuery, restCatalogConnect } from 'icebird'
import { collect } from 'squirreling'

const catalog = await restCatalogConnect({ url: 'https://catalog.example.com' })
const result = await icebergQuery({
  catalog,
  query: 'SELECT "Breed Name", "Popularity Rank" FROM "java.bunnies" WHERE "Popularity Rank" <= 3 ORDER BY "Popularity Rank"',
})
const rows = await collect(result)
```

## Writing

Icebird has experimental write support for Iceberg v2 (and v3 deletion vectors). All write functions take a `Catalog` and dispatch internally — the same call works against `fileCatalog({ resolver })` or a REST catalog context returned by `restCatalogConnect`.

```javascript
import {
  fileCatalog,
  icebergAppend,
  icebergCreateTable,
  icebergDelete,
  icebergExpireSnapshots,
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

// position deletes — `mode` defaults to 'puffin' on v3, 'parquet' on v2
await icebergDelete({
  catalog, tableUrl,
  deletes: [{ file_path: 's3://.../data/abc.parquet', pos: 0 }],
})

// snapshot management
await icebergSetRef({ catalog, tableUrl, ref: 'main', snapshotId })
await icebergExpireSnapshots({ catalog, tableUrl, snapshotIds: [oldSnapshotId] })
```

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
| Sorting | ❌ | |
| Encryption | ❌ | |

## References

 - https://iceberg.apache.org/spec/
 - https://avro.apache.org/docs/1.12.0/specification/
 - https://github.com/hyparam/hyparquet
 - https://github.com/hyparam/hyparquet-writer
 - https://github.com/apache/iceberg
 - https://github.com/apache/iceberg-python
 - https://github.com/apache/iceberg-rust
