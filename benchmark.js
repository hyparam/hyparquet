import { createWriteStream, promises as fs } from 'fs'
import { compressors } from 'hyparquet-compressors'
import { pipeline } from 'stream/promises'
import { parquetReadObjects } from './src/hyparquet.js'
import { asyncBufferFromFile } from './src/utils.js'

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
const file = await asyncBufferFromFile(filename)
const startTime = performance.now()
console.log('parsing example.parquet data...')

// read parquet file
await parquetReadObjects({
  file,
  compressors,
})
const ms = performance.now() - startTime
console.log(`parsed ${stat.size.toLocaleString()} bytes in ${ms.toFixed(0)} ms`)
