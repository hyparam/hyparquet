import { parquetMetadata, parquetMetadataAsync } from './metadata.js'
export { parquetMetadata, parquetMetadataAsync }

import { snappyUncompress } from './snappy.js'
export { snappyUncompress }

import { toJson } from './toJson.js'
export { toJson }

/**
 * Read parquet data rows from a buffer.
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {any[][]} row data
 */
export function parquetRead(arrayBuffer) {
  const metadata = parquetMetadata(arrayBuffer)
  throw new Error('not implemented')
}
