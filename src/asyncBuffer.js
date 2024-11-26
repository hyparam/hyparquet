
/**
 * Returns a cached layer on top of an AsyncBuffer. For caching slices of a file
 * that are read multiple times, possibly over a network.
 *
 * @typedef {import('../src/types.d.ts').AsyncBuffer} AsyncBuffer
 * @param {AsyncBuffer} file file-like object to cache
 * @returns {AsyncBuffer} cached file-like object
 */
export function cachedAsyncBuffer({ byteLength, slice }) {
  const cache = new Map()
  return {
    byteLength,
    /**
     * @param {number} start
     * @param {number} [end]
     * @returns {import('../src/types.d.ts').Awaitable<ArrayBuffer>}
     */
    slice(start, end) {
      const key = cacheKey(start, end, byteLength)
      const cached = cache.get(key)
      if (cached) return cached
      // cache miss, read from file
      const promise = slice(start, end)
      cache.set(key, promise)
      return promise
    },
  }
}


/**
 * Returns canonical cache key for a byte range 'start,end'.
 * Normalize int-range and suffix-range requests to the same key.
 *
 * @param {number} start start byte of range
 * @param {number} [end] end byte of range, or undefined for suffix range
 * @param {number} [size] size of file, or undefined for suffix range
 * @returns {string}
 */
function cacheKey(start, end, size) {
  if (start < 0) {
    if (end !== undefined) throw new Error(`invalid suffix range [${start}, ${end}]`)
    if (size === undefined) return `${start},`
    return `${size + start},${size}`
  } else if (end !== undefined) {
    if (start > end) throw new Error(`invalid empty range [${start}, ${end}]`)
    return `${start},${end}`
  } else if (size === undefined) {
    return `${start},`
  } else {
    return `${start},${size}`
  }
}
