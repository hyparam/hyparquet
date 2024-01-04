import { parquetMetadata } from './metadata.js'
export { parquetMetadata }

/**
 * Read parquet data rows from a file
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {any[][]} row data
 */
export function parquetRead(arrayBuffer) {
  const metadata = parquetMetadata(arrayBuffer)
  throw new Error('not implemented')
}
