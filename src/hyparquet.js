import { parquetMetadata, parquetMetadataAsync, parquetSchema } from './metadata.js'
export { parquetMetadata, parquetMetadataAsync, parquetSchema }

import { parquetRead } from './read.js'
export { parquetRead }

import { snappyUncompress } from './snappy.js'
export { snappyUncompress }

import { asyncBufferFromFile, asyncBufferFromUrl, toJson } from './utils.js'
export { asyncBufferFromFile, asyncBufferFromUrl, toJson }

/**
 * @param {import('./hyparquet.js').ParquetReadOptions} options
 * @returns {Promise<Array<Record<string, any>>>}
 */
export function parquetReadObjects(options) {
  return new Promise((onComplete, reject) => {
    parquetRead({
      rowFormat: 'object',
      ...options,
      onComplete,
    }).catch(reject)
  })
}
