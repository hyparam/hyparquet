/**
 * Read parquet data rows from a file
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {any[][]} row data
 */
export function parquetRead(arrayBuffer: ArrayBuffer): any[][]


/**
 * Read parquet header, metadata, and schema information from a file
 *
 * @typedef {import("./hyparquet.js").FileMetaData} FileMetaData
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {FileMetaData} metadata object
 */
export function parquetMetadata(arrayBuffer: ArrayBuffer): any

/**
 * Decompress snappy data.
 * Accepts an output buffer to avoid allocating a new buffer for each call.
 *
 * @param {Uint8Array} inputArray compressed data
 * @param {Uint8Array} outputArray output buffer
 * @returns {boolean} true if successful
 */
export function snappyUncompress(inputArray: Uint8Array, outputArray: Uint8Array): boolean

/**
 * Replace bigints with numbers.
 * When parsing parquet files, bigints are used to represent 64-bit integers.
 * However, JSON does not support bigints, so it's helpful to convert to numbers.
 *
 * @param {any} obj object to convert
 * @returns {unknown} converted object
 */
export function toJson(obj: any): unknown
