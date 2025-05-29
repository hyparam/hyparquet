import { createReadStream, promises as fs } from 'fs'

/**
 * Construct an AsyncBuffer for a local file using node fs package.
 *
 * @param {string} filename
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromFile(filename) {
  const stat = await fs.stat(filename)
  return {
    byteLength: stat.size,
    async slice(start, end) {
      // read file slice
      const readStream = createReadStream(filename, { start, end })
      return await readStreamToArrayBuffer(readStream)
    },
  }
}

/**
 * @import {AsyncBuffer} from '../src/types.js'
 * @param {import('stream').Readable} input
 * @returns {Promise<ArrayBuffer>}
 */
function readStreamToArrayBuffer(input) {
  return new Promise((resolve, reject) => {
    /** @type {Buffer[]} */
    const chunks = []
    input.on('data', chunk => chunks.push(chunk))
    input.on('end', () => {
      const buffer = Buffer.concat(chunks)
      resolve(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength))
    })
    input.on('error', reject)
  })
}
