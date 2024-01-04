import { parquetMetadata } from './metadata.js'
export { parquetMetadata }

import { snappyUncompress } from './snappy.js'
export { snappyUncompress }

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
