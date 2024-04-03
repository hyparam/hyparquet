# hyparquet

![hyparquet parakeet](hyparquet.jpg)

[![npm](https://img.shields.io/npm/v/hyparquet)](https://www.npmjs.com/package/hyparquet)
[![workflow status](https://github.com/hyparam/hyparquet/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/hyparquet/actions)
[![mit license](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![dependencies](https://img.shields.io/badge/Dependencies-0-blueviolet)](https://www.npmjs.com/package/hyparquet?activeTab=dependencies)

JavaScript parser for [Apache Parquet](https://parquet.apache.org) files.

Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.

Dependency free since 2023!

## Features

- Designed to work with huge ML datasets (things like [starcoder](https://huggingface.co/datasets/bigcode/starcoderdata))
- Can load metadata separately from data
- Data can be filtered by row and column ranges
- Only fetches the data needed
- Written in JavaScript, checked with TypeScript
- Fast data loading for large scale ML applications
- Bring data visualization closer to the user, in the browser

Why make a new parquet parser in javascript?
First, existing libraries like [parquetjs](https://github.com/ironSource/parquetjs) are officially "inactive".
Importantly, they do not support the kind of stream processing needed to make a really performant parser in the browser.
And finally, no dependencies means that hyparquet is lean, and easy to package and deploy.

## Demo

Online parquet file reader demo available at:

https://hyparam.github.io/hyparquet/

Demo source: [index.html](index.html)

## Installation

```bash
npm install hyparquet
```

## Usage

If you're in a node.js environment, you can load a parquet file with the following example:

```js
const { parquetMetadata } = await import('hyparquet')
const fs = await import('fs')

const buffer = fs.readFileSync('example.parquet')
const arrayBuffer = new Uint8Array(buffer).buffer
const metadata = parquetMetadata(arrayBuffer)
```

If you're in a browser environment, you'll probably get parquet file data from either a drag-and-dropped file from the user, or downloaded from the web.

To load parquet data in the browser from a remote server using `fetch`:

```js
import { parquetMetadata } from 'hyparquet'

const res = await fetch(url)
const arrayBuffer = await res.arrayBuffer()
const metadata = parquetMetadata(arrayBuffer)
```

To parse parquet files from a user drag-and-drop action, see example in [index.html](index.html).

## Reading Data

To read the entire contents of a parquet file in a browser environment:

```js
const { parquetRead } = await import("https://cdn.jsdelivr.net/npm/hyparquet/src/hyparquet.min.js")
const res = await fetch(url)
const arrayBuffer = await res.arrayBuffer()
await parquetRead({
  file: arrayBuffer,
  onComplete: data => console.log(data)
})
```

## Async

Hyparquet supports asynchronous fetching of parquet files, over a network.
You can provide an `AsyncBuffer` which is like a js `ArrayBuffer` but the `slice` method returns `Promise<ArrayBuffer>`.

## Supported Parquet Files

The parquet format is known to be a sprawling format which includes options for a wide array of compression schemes, encoding types, and data structures.

Hyparquet does not support 100% of all parquet files.
Supporting every possible compression codec available in parquet would blow up the size of the hyparquet library.
In practice, most parquet files use snappy compression.

You can extend support for parquet files with other compression codec using the `compressors` option.

```js
import { gunzipSync } from 'zlib'
parquetRead({ file, compressors: {
  // add gzip support:
  GZIP: (input, output) => output.set(gunzipSync(input)),
}})
```

Compression:
 - [X] Uncompressed
 - [X] Snappy
 - [ ] GZip
 - [ ] LZO
 - [ ] Brotli
 - [ ] LZ4
 - [ ] ZSTD
 - [ ] LZ4_RAW

Page Type:
 - [X] Data Page
 - [ ] Index Page
 - [X] Dictionary Page
 - [X] Data Page V2

Contributions are welcome!

## References

 - https://github.com/apache/parquet-format
 - https://github.com/apache/parquet-testing
 - https://github.com/apache/thrift
 - https://github.com/dask/fastparquet
 - https://github.com/google/snappy
 - https://github.com/zhipeng-jia/snappyjs
