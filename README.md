# hyparquet

![hyparquet](hyparquet.jpg)

[![npm](https://img.shields.io/npm/v/hyparquet)](https://www.npmjs.com/package/hyparquet)
[![workflow status](https://github.com/hyparam/hyparquet/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/hyparquet/actions)
[![mit license](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![dependencies](https://img.shields.io/badge/Dependencies-0-blueviolet)

JavaScript parser for [Apache Parquet](https://parquet.apache.org) files.

Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.

Dependency free since 2023!

## Features

- Designed to work with huge ML datasets (things like [starcoder](https://huggingface.co/datasets/bigcode/starcoderdata))
- Loads metadata separately from data
- Data can be filtered by row and column ranges
- Only fetches the data needed
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
const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
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

## References

 - https://github.com/apache/parquet-format
 - https://github.com/dask/fastparquet
 - https://github.com/apache/thrift
 - https://github.com/google/snappy
 - https://github.com/zhipeng-jia/snappyjs
