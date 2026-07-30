/**
 * @import {AsyncBuffer} from '../src/types.js'
 */

import { createReadStream, promises as fs } from 'fs'

export * from './index.js'

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
      if (start === end) return Promise.resolve(new ArrayBuffer(0))
      // read file slice (createReadStream end is inclusive)
      const reader = createReadStream(filename, { start, end: end === undefined ? undefined : end - 1 })
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
