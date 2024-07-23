import { createReadStream, createWriteStream, promises as fs } from 'fs'
import { compressors } from 'hyparquet-compressors'
import { pipeline } from 'stream/promises'
import { parquetRead } from './src/hyparquet.js'

const url = 'https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.en/train-00000-of-00041.parquet'
const filename = 'example.parquet'

// download test parquet file if needed
let stat = await fs.stat(filename).catch(() => undefined)
if (!stat) {
  console.log('downloading ' + url)
  const res = await fetch(url)
  if (!res.ok) throw new Error(res.statusText)
  // write to file async
  await pipeline(res.body, createWriteStream(filename))
  stat = await fs.stat(filename).catch(() => undefined)
  console.log('downloaded example.parquet', stat.size)
}

// asyncBuffer
const file = {
  byteLength: stat.size,
  async slice(start, end) {
    // read file slice
    const readStream = createReadStream(filename, { start, end })
    return await readStreamToArrayBuffer(readStream)
  },
}
const startTime = performance.now()
console.log('parsing example.parquet data...')

// read parquet file
await parquetRead({
  file,
  compressors,
})
const ms = performance.now() - startTime
console.log(`parsed ${stat.size.toLocaleString()} bytes in ${ms.toFixed(0)} ms`)

/**
 * Convert a web ReadableStream to ArrayBuffer.
 *
 * @param {ReadStream} input
 * @returns {Promise<ArrayBuffer>}
 */
function readStreamToArrayBuffer(input) {
  return new Promise((resolve, reject) => {
    const chunks = []
    input.on('data', chunk => chunks.push(chunk))
    input.on('end', () => {
      const buffer = Buffer.concat(chunks)
      resolve(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength))
    })
    input.on('error', reject)
  })
}
