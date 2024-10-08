import type { AsyncBuffer, Awaitable } from "../src/types.js"

/**
 * Returns a caches layer on top of an AsyncBuffer.
 * This is useful for caching slices of a file that are read multiple times,
 * possibly over a network.
 *
 * TODO: require data to be loaded with preload(), reads outside of preload rejected.
 *
 * @param {AsyncBuffer} file file-like object to cache
 * @returns {AsyncBuffer} cached file-like object
 */
export function cachedAsyncBuffer(file: AsyncBuffer): AsyncBuffer {
  // indexed by 'start,end'
  const cache = new Map<string, Awaitable<ArrayBuffer>>()
  return {
    byteLength: file.byteLength,
    slice(start: number, end?: number): Awaitable<ArrayBuffer> {
      // ensure both "100-200" and "100-" are both cached the same
      const key = cacheKey(start, end, file.byteLength)
      const cached = cache.get(key)
      if (cached) return cached
      // cache miss, read from file
      const promise = file.slice(start, end)
      cache.set(key, promise)
      return promise
    },
  }
}


/**
 * Returns canonical cache key for a byte range.
 * Cache key is a string of the form 'start,end'.
 * Attempts to normalize int-range and suffix-range requests to the same key.
 */
function cacheKey(start: number, end: number | undefined, fileSize: number | undefined): string {
  if (start < 0) {
    if (end !== undefined) throw new Error(`invalid suffix range [${start}, ${end}]`)
    if (fileSize === undefined) return `${start},`
    return `${fileSize + start},${fileSize}`
  } else if (end !== undefined) {
    if (start > end) throw new Error(`invalid empty range [${start}, ${end}]`)
    return `${start},${end}`
  } else if (fileSize === undefined) {
    return `${start},`
  } else {
    return `${start},${fileSize}`
  }
}
