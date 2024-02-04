import { createReadStream, createWriteStream, promises as fs } from 'fs'
import { parquetRead } from './src/hyparquet.js'

const url = 'https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.en/train-00000-of-00041.parquet'

// download test parquet file if needed
const stat = await fs.stat('benchmark.parquet').catch(() => undefined)
if (!stat) {
  console.log('downloading ' + url)
  const res = await fetch(url)
  if (!res.ok) throw new Error(res.statusText)
  // write to file async
  const writeStream = createWriteStream('benchmark.parquet')
  for await (const chunk of res.body) {
    writeStream.write(chunk)
  }
  // await res.body.pipeTo(writeStream)
  console.log('download benchmark.parquet')
}
// asyncBuffer
const file = {
  byteLength: stat.size,
  async slice(start, end) {
    // read file slice
    const readStream = createReadStream('benchmark.parquet', { start, end })
    const buffer = await readStreamToArrayBuffer(readStream)
    return new Uint8Array(buffer).buffer
  },
}
const startTime = performance.now()
console.log('parsing wikipedia data...')
// read parquet file
await parquetRead({ file })
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
    input.on('end', () => resolve(Buffer.concat(chunks).buffer))
    input.on('error', reject)
  })
}
