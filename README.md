# Hyparquet Iceberg

[![workflow status](https://github.com/hyparam/hyparquet-iceberg/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/hyparquet-iceberg/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-87-darkred)

Hyparquet Iceberg is a library for reading Iceberg tables in JavaScript. It is built on top of [hyparquet](https://github.com/hyparam/hyparquet) for reading the underlying parquet files.

## Usage

```javascript
const data = await readIcebergData({ tableUrl, rowStart: 0, rowEnd: 10 })
```
