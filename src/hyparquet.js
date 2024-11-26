export { parquetMetadata, parquetMetadataAsync, parquetSchema } from './metadata.js'

import { parquetRead } from './read.js'
export { parquetRead }

export { parquetQuery } from './query.js'

export { snappyUncompress } from './snappy.js'

export { asyncBufferFromFile, asyncBufferFromUrl, byteLengthFromUrl, toJson } from './utils.js'

export { cachedAsyncBuffer } from './asyncBuffer.js'

/**
 * @param {import('../src/types.d.ts').ParquetReadOptions} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
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
