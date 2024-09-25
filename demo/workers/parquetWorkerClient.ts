import type { AsyncBuffer, FileMetaData } from '../../src/hyparquet.js'
import { asyncBufferFromUrl } from '../../src/utils.js'

// Serializable constructors for AsyncBuffers
interface AsyncBufferFromFile {
  file: File
  byteLength: number
}
interface AsyncBufferFromUrl {
  url: string
  byteLength: number
}
export type AsyncBufferFrom = AsyncBufferFromFile | AsyncBufferFromUrl

// Same as ParquetReadOptions, but AsyncBufferFrom instead of AsyncBuffer
interface ParquetReadWorkerOptions {
  asyncBuffer: AsyncBufferFrom
  metadata?: FileMetaData // parquet metadata, will be parsed if not provided
  columns?: number[] // columns to read, all columns if undefined
  rowStart?: number // inclusive
  rowEnd?: number // exclusive
  orderBy?: string // column to sort by
}

let worker: Worker | undefined

/**
 * Presents almost the same interface as parquetRead, but runs in a worker.
 * This is useful for reading large parquet files without blocking the main thread.
 * Instead of taking an AsyncBuffer, it takes a FileContent, because it needs
 * to be serialized to the worker.
 */
export function parquetQueryWorker({
  metadata, asyncBuffer, rowStart, rowEnd, orderBy }: ParquetReadWorkerOptions
): Promise<Record<string, any>[]> {
  return new Promise((resolve, reject) => {
    // Create a worker
    if (!worker) {
      worker = new Worker(new URL('demo/workers/worker.min.js', import.meta.url))
    }
    worker.onmessage = ({ data }) => {
      // Convert postmessage data to callbacks
      if (data.error) {
        reject(data.error)
      } else if (data.result) {
        resolve(data.result)
      } else {
        reject(new Error('Unexpected message from worker'))
      }
    }
    worker.postMessage({ metadata, asyncBuffer, rowStart, rowEnd, orderBy })
  })
}

/**
 * Convert AsyncBufferFrom to AsyncBuffer.
 */
export async function asyncBufferFrom(from: AsyncBufferFrom): Promise<AsyncBuffer> {
  if ('url' in from) {
    return asyncBufferFromUrl(from.url)
  } else {
    return from.file.arrayBuffer()
  }
}
