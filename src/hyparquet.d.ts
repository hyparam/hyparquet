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
