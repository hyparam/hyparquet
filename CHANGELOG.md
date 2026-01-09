# Changelog

## [1.24.0]
 - Variant support (#149)

## [1.23.3]
 - Fix `readColumn` truncation in struct columns (#148)

## [1.23.2]
 - Add option `useOffsetIndex` to control use of offset index for page filtering (#146)

## [1.23.1]
 - Add option `filterStrict` to control strictness of filter equality (#145)
 - Apply `filter` not just best-effort
 - Combine runs of smaller column chunks when prefetching

## [1.23.0]
 - Replace `columnName: string` with `pathInSchema: string[]` in `onPage` callback (#144)

## [1.22.1]
 - Fix `BYTE_STREAM_SPLIT` with data page v2 and compression

## [1.22.0]
 - Rename constants to plural, and remove LogicalTypeType

## [1.21.1]
 - Add bigint to `ParquetQueryValue` type

## [1.21.0]
 - Adds optional `filter` parameter for pushdown row group filtering (#141)

## [1.20.3]
 - Fix extra fetch on the boundary of row groups

## [1.20.2]
 - Support S3 presigned URLs in `asyncBufferFromUrl` (#137 thanks @EpsilonPrime)

## [1.20.1]
 - Update geospatial and variant metadata

## [1.20.0]
 - Mark geospatial columns in GeoParquet with geometry or geography data type (#133)
 - Add `geoparquet` option to opt out marking GeoParquet columns (#133)

## [1.19.0]
 - Parse geometry and geography data types to geojson geometry objects (#131)

## [1.18.1]
 - Fix geospatial metadata parsing
 - Custom `stringFromBytes` parser option (#129)

## [1.18.0]
 - Refine `onComplete` types for `rowFormat: 'array'` and `rowFormat: 'object'` (#120)
 - Only object format for `parquetReadObjects` and `parquetQuery` (#120)
 - Parquet `__index__` column overrides `parquetQuery` index annotation (#120)

## [1.17.8]
 - Export `readColumnIndex` and `readOffsetIndex` (#122)

## [1.17.7]
 - Fix early page termination for non-flat columns (#119)
 - Fix a bug in parquetQuery, when rowFormat is 'array' (#118 thanks @severo)

## [1.17.6]
 - Revert "Fix onComplete return type (#104)" (#117)

## [1.17.5]
 - Fix high-precision decimal parsing (#116)

## [1.17.4]
 - Fix onComplete return type (#104 thanks @supermar1010)

## [1.17.3]
 - Fix circular import (#111)

## [1.17.2]
 - Export `ParquetQueryFilter` type (#105)
 - Remove a circular dependency (#108)

## [1.17.1]
 - Fix zero row file (#98 thanks @kroche98)

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
