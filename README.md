# Icebird: JavaScript Iceberg Reader

![Iceberg Icebird](icebird.jpg)

[![npm](https://img.shields.io/npm/v/icebird)](https://www.npmjs.com/package/icebird)
[![minzipped](https://img.shields.io/bundlephobia/minzip/icebird)](https://www.npmjs.com/package/icebird)
[![workflow status](https://github.com/hyparam/icebird/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/icebird/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-87-darkred)

Icebird is a library for reading [Apache Iceberg](https://iceberg.apache.org/) tables in JavaScript. It is built on top of [hyparquet](https://github.com/hyparam/hyparquet) for reading the underlying parquet files.

## Usage

```javascript
import { icebergRead } from 'icebird'

const data = await icebergRead({
  tableUrl,
  rowStart: 0,
  rowEnd: 10
})
```

## References

 - https://iceberg.apache.org/spec/
 - https://avro.apache.org/docs/1.12.0/specification/
 - https://github.com/hyparam/hyparquet
