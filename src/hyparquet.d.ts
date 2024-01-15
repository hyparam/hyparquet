export { AsyncBuffer, FileMetaData } from './types'

/**
 * Read parquet data rows from a file
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {any[][]} row data
 */
export function parquetRead(arrayBuffer: ArrayBuffer): any[][]

/**
 * Read parquet metadata from an async buffer.
 *
 * An AsyncBuffer is like an ArrayBuffer, but the slices are loaded
 * asynchronously, possibly over the network.
 *
 * To make this efficient, we initially request the last 512kb of the file,
 * which is likely to contain the metadata. If the metadata length exceeds the
 * initial fetch, 512kb, we request the rest of the metadata from the AsyncBuffer.
 *
 * This ensures that we either make one 512kb initial request for the metadata,
 * or two requests for exactly the metadata size.
 *
 * @param {AsyncBuffer} asyncBuffer parquet file contents
 * @param {number} initialFetchSize initial fetch size in bytes (default 512kb)
 * @returns {Promise<FileMetaData>} metadata object
 */
export async function parquetMetadataAsync(asyncBuffer: ArrayBuffer, initialFetchSize: number = 1 << 19 /* 512kb */): Promise<FileMetaData>

/**
 * Read parquet metadata from a buffer
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {FileMetaData} metadata object
 */
export function parquetMetadata(arrayBuffer: ArrayBuffer): FileMetaData

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
