import { createReadStream, promises as fs } from 'fs'

export * from './hyparquet.js'

/**
 * @import {AsyncBuffer} from '../src/types.js'
 */
/**
 * Construct an AsyncBuffer for a local file using node fs package.
 *
 * @param {string} filename
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromFile(filename) {
  const { size } = await fs.stat(filename)
  return {
    byteLength: size,
    slice(start, end) {
      // read file slice
      const reader = createReadStream(filename, { start, end })
      return new Promise((resolve, reject) => {
        /** @type {any[]} */
        const chunks = []
        reader.on('data', chunk => chunks.push(chunk))
        reader.on('error', reject)
        reader.on('end', () => {
          const buffer = Buffer.concat(chunks)
          resolve(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength))
        })
      })
    },
  }
}
