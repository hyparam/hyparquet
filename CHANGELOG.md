# Changelog

## [1.17.0]
 - Require explicit `$eq` operator for `parquetQuery` filters.

## [1.16.2]
 - Fix readRleBitPackedHybrid when length is zero

## [1.16.1]
 - Fix duckdb empty block

## [1.16.0]
 - New `parsers` option for custom date parsing.
 - Breaking change: parquetMetadataAsync moved initialFetchSize into an options object.

## [1.15.0]
 - Change packaging to have node-specific exports for `asyncBufferFromFile` (#80).

## [1.14.0]
 - Refactor to use `AsyncRowGroup` and `AsyncColumn` abstractions for better performance and flexibility (#83).

## [1.13.6]
 - Fix page continuation (#81 thanks @jpivarski).

## [1.13.5]
 - Fast parquetQuery filter (#78)

## [1.13.4]
 - parquetSchema more generic argument type.

## [1.13.3]
 - Convert logical type 'STRING'.
 - Side-effect-free in package.json.

## [1.13.2]
 - Fix duckdb delta encoding (#77 thanks @mike-iqmo).

## [1.13.1]
 - Throw exception for unsupported file_path.

## [1.13.0]
 - Query planner: pre-fetch byte ranges in parallel (#75).

## [1.12.1]
 - Fix conversion of unsigned types.
 - Allow passing a custom fetch function to utilities (#73).

## [1.12.0]
 - Add `onPage` callback to `parquetRead`.

## [1.11.1]
 - Fix handling of dictionary pages from parquet.net.

## [1.11.0]
 - Fix continued data pages
 - Skip decoding unnecessary pages.

## [1.10.4]
 - Add type definitions for thrift.

## [1.10.3]
 - Internal refactor split out `readPage` function.

## [1.10.2]
 - Export additional internal constants.

## [1.10.1]
 - Fix parsing of `crypto_metadata` in thrift.

## [1.10.0]
 - Map `src` files to TypeScript types via package exports (#70).
 - Use `defaultInitialFetchSize` for both metadata and `cachedAsyncBuffer`.

## [1.9.x]
 - Add `minSize` parameter to `cachedAsyncBuffer` for finer control.
 - Return typed arrays in `onChunk` callbacks.
 - Change `readColumn` to return an array of `DecodedArray` (#67).

## [1.8.x]
 - Support endpoints without range requests in `asyncBufferFromUrl` (#57 thanks @swlynch99).
 - Enhance error messages for common parsing issues.
 - Mongo-style `filter` option in `parquetQuery` (#56 thanks @park-brian).

## [1.7.0]
 - Enable `readColumn` to read all rows (#53 thanks @park-brian).
 - Validate url in `asyncBufferFromUrl`.

## [1.6.x]
 - Fix timestamp conversion in metadata parsing (#45 thanks @cbardasano).
 - Build TypeScript types before publishing to npm.

## [1.5.0]
 - Export `cachedAsyncBuffer` utility.

## [1.4.0]
 - Add `parquetQuery` with `orderBy` option.

## [1.3.0]
 - Promisify `parquetReadObjects` function.
 - Add support for parsing column and offset indexes (#29).

## [1.2.0]
 - Return columns in the requested order (#27 thanks @cstranstrum).
 - Add option to return each row as an object keyed by column names (#25 thanks @cstranstrum).

## [1.1.0]
 - Export `asyncBufferFromFile` and `asyncBufferFromUrl` utilities.

## [1.0.0]
 - Initial stable release.
