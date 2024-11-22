import type { AsyncBuffer, CompressionCodec, Compressors, ConvertedType, FileMetaData, LogicalType, ParquetType, SchemaTree } from './types.d.ts'

export type { AsyncBuffer, CompressionCodec, Compressors, ConvertedType, FileMetaData, LogicalType, ParquetType, SchemaTree }

/**
 * Read parquet data rows from a file-like object.
 * Reads the minimal number of row groups and columns to satisfy the request.
 *
 * Returns a void promise when complete, and to throw errors.
 * Data is returned in onComplete, not the return promise, because
 * if onComplete is undefined, we parse the data, and emit chunks, but skip
 * computing the row view directly. This saves on allocation if the caller
 * wants to cache the full chunks, and make their own view of the data from
 * the chunks.
 *
 * @param {object} options read options
 * @param {AsyncBuffer} options.file file-like object containing parquet data
 * @param {FileMetaData} [options.metadata] parquet file metadata
 * @param {string[]} [options.columns] columns to read, all columns if undefined
 * @param {string} [options.rowFormat] desired format of each row passed to the onComplete function
 * @param {number} [options.rowStart] first requested row index (inclusive)
 * @param {number} [options.rowEnd] last requested row index (exclusive)
 * @param {Function} [options.onChunk] called when a column chunk is parsed. chunks may include row data outside the requested range.
 * @param {Function} [options.onComplete] called when all requested rows and columns are parsed
 * @param {Compressors} [options.compressor] custom decompressors
 * @returns {Promise<void>} resolves when all requested rows and columns are parsed
 */
export function parquetRead(options: ParquetReadOptions): Promise<void>

/**
 * Read parquet data and return a Promise of object-oriented row data.
 *
 * @param {object} options read options
 * @param {AsyncBuffer} options.file file-like object containing parquet data
 * @param {FileMetaData} [options.metadata] parquet file metadata
 * @param {string[]} [options.columns] columns to read, all columns if undefined
 * @param {number} [options.rowStart] first requested row index (inclusive)
 * @param {number} [options.rowEnd] last requested row index (exclusive)
 * @param {Compressors} [options.compressor] custom decompressors
 * @returns {Promise<void>} resolves when all requested rows and columns are parsed
 */
export function parquetReadObjects(options: ParquetReadOptions): Promise<Array<Record<string, any>>>

/**
 * Wraps parquetRead with orderBy support.
 * This is a parquet-aware query engine that can read a subset of rows and columns.
 * Accepts an optional orderBy column name to sort the results.
 * Note that using orderBy may SIGNIFICANTLY increase the query time.
 *
 * @param {ParquetReadOptions & { orderBy?: string }} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export function parquetQuery(options: ParquetReadOptions & { orderBy?: string }): Promise<Array<Record<string, any>>>

/**
 * Read parquet metadata from an async buffer.
 *
 * An AsyncBuffer is like an ArrayBuffer, but the slices are loaded
 * asynchronously, possibly over the network.
 *
 * You must provide the byteLength of the buffer, typically from a HEAD request.
 *
 * In theory, you could use suffix-range requests to fetch the end of the file,
 * and save a round trip. But in practice, this doesn't work because chrome
 * deems suffix-range requests as a not-safe-listed header, and will require
 * a pre-flight. So the byteLength is required.
 *
 * To make this efficient, we initially request the last 512kb of the file,
 * which is likely to contain the metadata. If the metadata length exceeds the
 * initial fetch, 512kb, we request the rest of the metadata from the AsyncBuffer.
 *
 * This ensures that we either make one 512kb initial request for the metadata,
 * or a second request for up to the metadata size.
 *
 * @param {AsyncBuffer} asyncBuffer parquet file contents
 * @param {number} initialFetchSize initial fetch size in bytes (default 512kb)
 * @returns {Promise<FileMetaData>} parquet metadata object
 */
export function parquetMetadataAsync(asyncBuffer: AsyncBuffer, initialFetchSize?: number): Promise<FileMetaData>

/**
 * Read parquet metadata from a buffer
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {FileMetaData} parquet metadata object
 */
export function parquetMetadata(arrayBuffer: ArrayBuffer): FileMetaData

/**
 * Return a tree of schema elements from parquet metadata.
 *
 * @param {FileMetaData} metadata parquet metadata object
 * @returns {SchemaTree} tree of schema elements
 */
export function parquetSchema(metadata: FileMetaData): SchemaTree

/**
 * Decompress snappy data.
 * Accepts an output buffer to avoid allocating a new buffer for each call.
 *
 * @param {Uint8Array} input compressed data
 * @param {Uint8Array} output output buffer
 * @returns {boolean} true if successful
 */
export function snappyUncompress(input: Uint8Array, output: Uint8Array): boolean

/**
 * Replace bigints with numbers.
 * When parsing parquet files, bigints are used to represent 64-bit integers.
 * However, JSON does not support bigints, so it's helpful to convert to numbers.
 *
 * @param {any} obj object to convert
 * @returns {unknown} converted object
 */
export function toJson(obj: any): any

/**
 * Construct an AsyncBuffer for a URL.
 * If byteLength is not provided, will make a HEAD request to get the file size.
 * If requestInit is provided, it will be passed to fetch.
 */
export function asyncBufferFromUrl({url, byteLength, requestInit}: {url: string, byteLength?: number, requestInit?: RequestInit}): Promise<AsyncBuffer>

/**
 * Construct an AsyncBuffer for a local file using node fs package.
 */
export function asyncBufferFromFile(filename: string): Promise<AsyncBuffer>

/**
 * Get the byte length of a URL using a HEAD request.
 * If requestInit is provided, it will be passed to fetch.
 */
export function byteLengthFromUrl(url: string, requestInit?: RequestInit): Promise<number>

/**
 * Returns a cached layer on top of an AsyncBuffer.
 */
export function cachedAsyncBuffer(asyncBuffer: AsyncBuffer): AsyncBuffer

/**
 * Parquet query options for reading data
 */
export interface ParquetReadOptions {
  file: AsyncBuffer // file-like object containing parquet data
  metadata?: FileMetaData // parquet metadata, will be parsed if not provided
  columns?: string[] // columns to read, all columns if undefined
  rowFormat?: string // format of each row passed to the onComplete function
  rowStart?: number // inclusive
  rowEnd?: number // exclusive
  onChunk?: (chunk: ColumnData) => void // called when a column chunk is parsed. chunks may be outside the requested range.
  onComplete?: (rows: any[][]) => void // called when all requested rows and columns are parsed
  compressors?: Compressors // custom decompressors
  utf8?: boolean // decode byte arrays as utf8 strings (default true)
}

/**
 * A run of column data
 */
export interface ColumnData {
  columnName: string
  columnData: ArrayLike<any>
  rowStart: number
  rowEnd: number
}
