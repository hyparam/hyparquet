import { parquetMetadata } from './metadata.js'

/**
 * Read parquet data rows from a file
 *
 * @param arrayBuffer parquet file contents
 * @returns array of rows
 */
export function parquetRead(arrayBuffer: ArrayBuffer): any[][] {
  const metadata = parquetMetadata(arrayBuffer)
  throw new Error('not implemented')
}
