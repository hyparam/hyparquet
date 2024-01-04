import { parquetMetadata } from './metadata.js'

/**
 * Read parquet data rows from a file
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {any[][]} row data
 */
export function parquetRead(arrayBuffer: ArrayBuffer): any[][] {
  const metadata = parquetMetadata(arrayBuffer)
  throw new Error('not implemented')
}
