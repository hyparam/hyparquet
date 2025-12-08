export { readColumnIndex, readOffsetIndex } from './indexes.js'
export { parquetMetadataAsync, parquetSchema } from './metadata.js'
export { parquetRead, parquetReadObjects } from './read.js'
export { parquetQuery } from './query.js'
export { snappyUncompress } from './snappy.js'
export { asyncBufferFromUrl, byteLengthFromUrl, cachedAsyncBuffer, flatten, toJson } from './utils.js'

/**
 * Explicitly export types for use in downstream typescript projects through
 * `import { ParquetReadOptions } from 'hyparquet'` for example.
 *
 * @template {any} T
 * @typedef {import('../src/types.d.ts').Awaitable<T>} Awaitable<T>
 */
/**
 * @typedef {import('../src/types.d.ts').AsyncBuffer} AsyncBuffer
 * @typedef {import('../src/types.d.ts').AsyncRowGroup} AsyncRowGroup
 * @typedef {import('../src/types.d.ts').DataReader} DataReader
 * @typedef {import('../src/types.d.ts').FileMetaData} FileMetaData
 * @typedef {import('../src/types.d.ts').SchemaTree} SchemaTree
 * @typedef {import('../src/types.d.ts').SchemaElement} SchemaElement
 * @typedef {import('../src/types.d.ts').ParquetType} ParquetType
 * @typedef {import('../src/types.d.ts').FieldRepetitionType} FieldRepetitionType
 * @typedef {import('../src/types.d.ts').ConvertedType} ConvertedType
 * @typedef {import('../src/types.d.ts').TimeUnit} TimeUnit
 * @typedef {import('../src/types.d.ts').LogicalType} LogicalType
 * @typedef {import('../src/types.d.ts').RowGroup} RowGroup
 * @typedef {import('../src/types.d.ts').ColumnChunk} ColumnChunk
 * @typedef {import('../src/types.d.ts').ColumnMetaData} ColumnMetaData
 * @typedef {import('../src/types.d.ts').Encoding} Encoding
 * @typedef {import('../src/types.d.ts').CompressionCodec} CompressionCodec
 * @typedef {import('../src/types.d.ts').Compressors} Compressors
 * @typedef {import('../src/types.d.ts').KeyValue} KeyValue
 * @typedef {import('../src/types.d.ts').Statistics} Statistics
 * @typedef {import('../src/types.d.ts').GeospatialStatistics} GeospatialStatistics
 * @typedef {import('../src/types.d.ts').BoundingBox} BoundingBox
 * @typedef {import('../src/types.d.ts').PageType} PageType
 * @typedef {import('../src/types.d.ts').PageHeader} PageHeader
 * @typedef {import('../src/types.d.ts').DataPageHeader} DataPageHeader
 * @typedef {import('../src/types.d.ts').DictionaryPageHeader} DictionaryPageHeader
 * @typedef {import('../src/types.d.ts').DecodedArray} DecodedArray
 * @typedef {import('../src/types.d.ts').OffsetIndex} OffsetIndex
 * @typedef {import('../src/types.d.ts').ColumnIndex} ColumnIndex
 * @typedef {import('../src/types.d.ts').BoundaryOrder} BoundaryOrder
 * @typedef {import('../src/types.d.ts').ColumnData} ColumnData
 * @typedef {import('../src/types.d.ts').SubColumnData} SubColumnData
 * @typedef {import('../src/types.d.ts').ParquetReadOptions} ParquetReadOptions
 * @typedef {import('../src/types.d.ts').MetadataOptions} MetadataOptions
 * @typedef {import('../src/types.d.ts').ParquetParsers} ParquetParsers
 * @typedef {import('../src/types.d.ts').ParquetQueryFilter} ParquetQueryFilter
 */
