# Icebird: JavaScript Iceberg Reader

![Iceberg Icebird](icebird.jpg)

[![npm](https://img.shields.io/npm/v/icebird)](https://www.npmjs.com/package/icebird)
[![minzipped](https://img.shields.io/bundlephobia/minzip/icebird)](https://www.npmjs.com/package/icebird)
[![workflow status](https://github.com/hyparam/icebird/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/icebird/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-91-darkred)

Icebird is a library for reading [Apache Iceberg](https://iceberg.apache.org/) tables in JavaScript. It is built on top of [hyparquet](https://github.com/hyparam/hyparquet) for reading the underlying parquet files.

## Usage

To read an Iceberg table:

```javascript
const { icebergRead } = await import('icebird')

const tableUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'
const data = await icebergRead({
  tableUrl,
  rowStart: 0,
  rowEnd: 10
})
```

To read the Iceberg metadata (schema, etc):

```javascript
import { icebergMetadata } from 'icebird'

const metadata = await icebergMetadata(tableUrl)

// subsequent reads will be faster if you provide the metadata:
const data = await icebergRead({
  tableUrl,
  metadata,
  rowStart: 0,
  rowEnd: 10
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
