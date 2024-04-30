import { createReadStream, createWriteStream, promises as fs } from 'fs'
import { snappyUncompressor } from 'hysnappy'
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
  const writeStream = createWriteStream(filename)
  for await (const chunk of res.body) {
    writeStream.write(chunk)
  }
  writeStream.end()
  console.log('downloaded example.parquet')
  stat = await fs.stat(filename).catch(() => undefined)
}

// asyncBuffer
const file = {
  byteLength: stat.size,
  async slice(start, end) {
    // read file slice
    const readStream = createReadStream(filename, { start, end })
    const buffer = await readStreamToArrayBuffer(readStream)
    return new Uint8Array(buffer).buffer
  },
}
const startTime = performance.now()
console.log('parsing example.parquet data...')

// read parquet file
await parquetRead({
  file,
  compressors: { SNAPPY: snappyUncompressor() }, // hysnappy wasm
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
    input.on('end', () => resolve(Buffer.concat(chunks).buffer))
    input.on('error', reject)
  })
}
