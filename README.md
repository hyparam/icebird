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

You can add authentication to all http requests by passing a `requestInit` argument that will be passed to `fetch`:

```javascript
import { icebergRead } from 'icebird'

const data = await icebergRead({
  tableUrl,
  requestInit: {
    headers: {
      Authorization: 'Bearer my_token',
    },
  }
})
```

## Supported Features

Icebird aims to support reading any Iceberg table, but currently only supports a subset of the features. The following features are supported:

| Feature | Supported |
| ------- | --------- |
| Read Iceberg v1 Tables | ✅ |
| Read Iceberg v2 Tables | ✅ |
| Read Iceberg v3 Tables | ❌ |
| Parquet Storage | ✅ |
| Avro Storage | ✅ |
| ORC Storage | ❌ |
| Puffin Storage | ❌ |
| File-based Catalog (version-hint.text) | ✅ |
| REST Catalog | ❌ |
| Hive Catalog | ❌ |
| Glue Catalog | ❌ |
| Service-based Catalog | ❌ |
| Position Deletes | ✅ |
| Equality Deletes | ✅ |
| Binary Deletion Vectors | ❌ |
| Rename Columns | ✅ |
| Efficient Partitioned Read Queries | ❌ |
| All Parquet Compression Codecs | ✅ |
| All Parquet Types | ✅ |
| Variant Types | ❌ |
| Geometry Types | ❌ |
| Geography Types | ❌ |
| Sorting | ❌ |
| Encryption | ❌ |

## References

 - https://iceberg.apache.org/spec/
 - https://avro.apache.org/docs/1.12.0/specification/
 - https://github.com/hyparam/hyparquet
 - https://github.com/apache/iceberg
 - https://github.com/apache/iceberg-python
