# hyparquet

![hyparquet parakeet](hyparquet.jpg)

[![npm](https://img.shields.io/npm/v/hyparquet)](https://www.npmjs.com/package/hyparquet)
[![workflow status](https://github.com/hyparam/hyparquet/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/hyparquet/actions)
[![mit license](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![dependencies](https://img.shields.io/badge/Dependencies-0-blueviolet)](https://www.npmjs.com/package/hyparquet?activeTab=dependencies)
![coverage](https://img.shields.io/badge/Coverage-96-darkred)

Dependency free since 2023!

## What is hyparquet?

**Hyparquet** is a lightweight, dependency-free, pure JavaScript library for parsing [Apache Parquet](https://parquet.apache.org) files. Apache Parquet is a popular columnar storage format that is widely used in data engineering, data science, and machine learning applications for efficiently storing and processing large datasets.

Hyparquet aims to be the world's most compliant parquet parser. And it runs in the browser.

## Parquet Viewer

**Try hyparquet online**: Drag and drop your parquet file onto [hyperparam.app](https://hyperparam.app) to view it directly in your browser. This service is powered by hyparquet's in-browser capabilities.

[![hyperparam parquet viewer](./hyperparam.png)](https://hyperparam.app/)

## Features

1. **Browser-native**: Built to work seamlessly in the browser, opening up new possibilities for web-based data applications and visualizations.
2. **Performant**: Designed to efficiently process large datasets by only loading the required data, making it suitable for big data and machine learning applications.
3. **TypeScript**: Includes TypeScript definitions.
4. **Dependency-free**: Hyparquet has zero dependencies, making it lightweight and easy to use in any JavaScript project. Only 9.2kb min.gz!
5. **Highly Compliant:** Supports all parquet encodings, compression codecs, and can open more parquet files than any other library.

## Why hyparquet?

Existing JavaScript-based parquet readers (like [parquetjs](https://github.com/ironSource/parquetjs)) are no longer actively maintained, may not support streaming or in-browser processing efficiently, and often rely on dependencies that can inflate your bundle size.
Hyparquet is actively maintained and designed with modern web usage in mind.

## Demo

Check out a minimal parquet viewer demo that shows how to integrate hyparquet into a react web application using [HighTable](https://github.com/hyparam/hightable).

 - **Live Demo**: [https://hyparam.github.io/hyperparam-cli/apps/hyparquet-demo/](https://hyparam.github.io/hyperparam-cli/apps/hyparquet-demo/)
 - **Source Code**: [https://github.com/hyparam/hyperparam-cli/tree/master/apps/hyparquet-demo](https://github.com/hyparam/hyperparam-cli/tree/master/apps/hyparquet-demo)

## Quick Start

### Node.js Example

To read the contents of a parquet file in a node.js environment use `asyncBufferFromFile`:

```javascript
const { asyncBufferFromFile, parquetRead } = await import('hyparquet')

await parquetRead({
  file: await asyncBufferFromFile(filename),
  onComplete: data => console.log(data)
})
```

Note: Hyparquet is published as an ES module, so dynamic `import()` may be required on the command line.

### Browser Example

In the browser use `asyncBufferFromUrl` to wrap a url for reading asyncronously over the network.
It is recommended that you filter by row and column to limit fetch size:

```js
const { asyncBufferFromUrl, parquetRead } = await import('https://cdn.jsdelivr.net/npm/hyparquet/src/hyparquet.min.js')

const url = 'https://hyperparam-public.s3.amazonaws.com/bunnies.parquet'
await parquetRead({
  file: await asyncBufferFromUrl({url}),
  columns: ['Breed Name', 'Lifespan'],
  rowStart: 10,
  rowEnd: 20,
  onComplete: data => console.log(data)
})
```

## Advanced Usage

### Reading Metadata

You can read just the metadata, including schema and data statistics using the `parquetMetadata` function.
To load parquet data in the browser from a remote server using `fetch`:

```javascript
import { parquetMetadata } from 'hyparquet'

const res = await fetch(url)
const arrayBuffer = await res.arrayBuffer()
const metadata = parquetMetadata(arrayBuffer)
```

### AsyncBuffer

Hyparquet accepts argument `file` of type `AsyncBuffer` which is like a js `ArrayBuffer` but the `slice` method returns `Promise<ArrayBuffer>`.

```typescript
interface AsyncBuffer {
  byteLength: number
  slice(start: number, end?: number): Promise<ArrayBuffer>
}
```

You can define your own `AsyncBuffer` to create a virtual file that can be read asynchronously. In most cases, you should probably use `asyncBufferFromUrl` or `asyncBufferFromFile`.

### Authorization

Pass the `requestInit` option to `asyncBufferFromUrl` to provide authentication information to a remote web server. For example:

```js
await parquetRead({
  file: await asyncBufferFromUrl({url, requestInit: {headers: {Authorization: 'Bearer my_token'}}}),
  onComplete: data => console.log(data)
})
```

### Returned row format

By default, data returned in the `onComplete` function will be one array of columns per row.
If you would like each row to be an object with each key the name of the column, set the option `rowFormat` to `object`.

```javascript
import { parquetRead } from 'hyparquet'

await parquetRead({
  file,
  rowFormat: 'object',
  onComplete: data => console.log(data),
})
```

## Supported Parquet Files

The parquet format is known to be a sprawling format which includes options for a wide array of compression schemes, encoding types, and data structures.
Hyparquet supports all parquet encodings: plain, dictionary, rle, bit packed, delta, etc.

**Hyparquet is the most compliant parquet parser on earth** — hyparquet can open more files than pyarrow, rust, and duckdb.

## Compression

By default, hyparquet supports uncompressed and snappy-compressed parquet files.
To support the full range of parquet compression codecs (gzip, brotli, zstd, etc), use the [hyparquet-compressors](https://github.com/hyparam/hyparquet-compressors) package.

| Codec         | hyparquet | with hyparquet-compressors |
|---------------|-----------|----------------------------|
| Uncompressed  | ✅        | ✅                         |
| Snappy        | ✅        | ✅                         |
| GZip          | ❌        | ✅                         |
| LZO           | ❌        | ✅                         |
| Brotli        | ❌        | ✅                         |
| LZ4           | ❌        | ✅                         |
| ZSTD          | ❌        | ✅                         |
| LZ4_RAW       | ❌        | ✅                         |

### hysnappy

For faster snappy decompression, try [hysnappy](https://github.com/hyparam/hysnappy), which uses WASM for a 40% speed boost on large parquet files.

### hyparquet-compressors

You can include support for ALL parquet `compressors` plus hysnappy using the [hyparquet-compressors](https://github.com/hyparam/hyparquet-compressors) package.


```js
import { parquetRead } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

await parquetRead({ file, compressors, onComplete: console.log })
```

## References

 - https://github.com/apache/parquet-format
 - https://github.com/apache/parquet-testing
 - https://github.com/apache/thrift
 - https://github.com/apache/arrow
 - https://github.com/dask/fastparquet
 - https://github.com/duckdb/duckdb
 - https://github.com/google/snappy
 - https://github.com/hyparam/hightable
 - https://github.com/hyparam/hysnappy
 - https://github.com/hyparam/hyparquet-compressors
 - https://github.com/ironSource/parquetjs
 - https://github.com/zhipeng-jia/snappyjs

## Contributions

Contributions are welcome!
If you have suggestions, bug reports, or feature requests, please open an issue or submit a pull request.

Hyparquet development is supported by an open-source grant from Hugging Face :hugs:
