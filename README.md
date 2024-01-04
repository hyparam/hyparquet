# hyparquet

![hyparquet](hyparquet.jpg)

[![mit license](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![workflow status](https://github.com/hyparam/hyparquet/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/hyparquet/actions)
[![npm](https://img.shields.io/npm/v/hyparquet)](https://www.npmjs.com/package/hyparquet)

JavaScript parser for [Apache Parquet](https://parquet.apache.org) files.

Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.

Dependency free since 2023!

## Usage

```bash
npm install hyparquet
```

```js
import { parquetMetadata } from 'hyparquet'

const metadata = parquetMetdata(arrayBuffer)
```

## References

 - https://github.com/apache/parquet-format
 - https://github.com/dask/fastparquet
 - https://github.com/apache/thrift
 - https://github.com/google/snappy
 - https://github.com/zhipeng-jia/snappyjs
