import { defaultInitialFetchSize } from './metadata.js'

/**
 * Replace bigint, date, etc with legal JSON types.
 *
 * @param {any} obj object to convert
 * @returns {unknown} converted object
 */
export function toJson(obj) {
  if (obj === undefined) return null
  if (typeof obj === 'bigint') return Number(obj)
  if (Array.isArray(obj)) return obj.map(toJson)
  if (obj instanceof Uint8Array) return Array.from(obj)
  if (obj instanceof Date) return obj.toISOString()
  if (obj instanceof Object) {
    /** @type {Record<string, unknown>} */
    const newObj = {}
    for (const key of Object.keys(obj)) {
      if (obj[key] === undefined) continue
      newObj[key] = toJson(obj[key])
    }
    return newObj
  }
  return obj
}

/**
 * Concatenate two arrays fast.
 *
 * @param {any[]} aaa first array
 * @param {DecodedArray} bbb second array
 */
export function concat(aaa, bbb) {
  const chunk = 10000
  for (let i = 0; i < bbb.length; i += chunk) {
    aaa.push(...bbb.slice(i, i + chunk))
  }
}

/**
 * Deep equality comparison
 *
 * @param {any} a First object to compare
 * @param {any} b Second object to compare
 * @returns {boolean} true if objects are equal
 */
export function equals(a, b) {
  if (a === b) return true
  if (a instanceof Uint8Array && b instanceof Uint8Array) return equals(Array.from(a), Array.from(b))
  if (!a || !b || typeof a !== typeof b) return false
  return Array.isArray(a) && Array.isArray(b)
    ? a.length === b.length && a.every((v, i) => equals(v, b[i]))
    : typeof a === 'object' && Object.keys(a).length === Object.keys(b).length && Object.keys(a).every(k => equals(a[k], b[k]))
}

/**
 * Get the byte length using fetch with a ranged GET request.
 * Aborts the request if server returns 200 instead of 206.
 *
 * @param {string} url
 * @param {RequestInit} [requestInit] fetch options
 * @param {typeof globalThis.fetch} [fetchFn] fetch function to use
 * @returns {Promise<number>}
 */
async function byteLengthFromUrlUsingFetch(url, requestInit = {}, fetchFn = globalThis.fetch) {
  const controller = new AbortController()
  const headers = new Headers(requestInit.headers)
  headers.set('Range', 'bytes=0-0')

  const res = await fetchFn(url, {
    ...requestInit,
    headers,
    signal: controller.signal,
  })

  if (!res.ok) throw new Error(`fetch with range failed ${res.status}`)

  // Server supports Range requests (206 Partial Content)
  if (res.status === 206) {
    const contentRange = res.headers.get('Content-Range')
    if (!contentRange) throw new Error('missing content-range header')

    // Parse "bytes 0-0/9446073" to get total length
    const match = contentRange.match(/bytes \d+-\d+\/(\d+)/)
    if (!match) throw new Error(`invalid content-range header: ${contentRange}`)

    return parseInt(match[1])
  }

  // Server ignored Range and returned 200 - get Content-Length and abort request
  if (res.status === 200) {
    const contentLength = res.headers.get('Content-Length')

    // Abort the request to stop any ongoing download
    controller.abort()

    if (contentLength) return parseInt(contentLength)
  }

  throw new Error('server does not support range requests and missing content-length')
}

/**
 * Get the byte length of a URL using a HEAD request.
 * If HEAD fails with 403 (e.g., with signed S3 URLs), falls back to a ranged GET request.
 * If HEAD succeeds but Content-Length is missing, falls back to GET with range.
 * If requestInit is provided, it will be passed to fetch.
 *
 * @param {string} url
 * @param {RequestInit} [requestInit] fetch options
 * @param {typeof globalThis.fetch} [customFetch] fetch function to use
 * @returns {Promise<number>}
 */
export async function byteLengthFromUrl(url, requestInit, customFetch) {
  const fetch = customFetch ?? globalThis.fetch
  const res = await fetch(url, { ...requestInit, method: 'HEAD' })

  // If HEAD request is forbidden (common with signed S3 URLs), try GET with range
  if (res.status === 403) {
    return byteLengthFromUrlUsingFetch(url, requestInit, fetch)
  }

  if (!res.ok) throw new Error(`fetch head failed ${res.status}`)
  const length = res.headers.get('Content-Length')
  // If Content-Length is missing from HEAD, fallback to GET with range
  if (!length) {
    return byteLengthFromUrlUsingFetch(url, requestInit, fetch)
  }
  return parseInt(length)
}

/**
 * Construct an AsyncBuffer for a URL.
 * If byteLength is not provided, will make a HEAD request to get the file size.
 * If fetch is provided, it will be used instead of the global fetch.
 * If requestInit is provided, it will be passed to fetch.
 *
 * @param {object} options
 * @param {string} options.url
 * @param {number} [options.byteLength]
 * @param {typeof globalThis.fetch} [options.fetch] fetch function to use
 * @param {RequestInit} [options.requestInit]
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromUrl({ url, byteLength, requestInit, fetch: customFetch }) {
  if (!url) throw new Error('missing url')
  const fetch = customFetch ?? globalThis.fetch
  // byte length from HEAD request
  byteLength ??= await byteLengthFromUrl(url, requestInit, fetch)

  /**
   * A promise for the whole buffer, if range requests are not supported.
   * @type {Promise<ArrayBuffer>|undefined}
   */
  let buffer = undefined
  const init = requestInit || {}

  return {
    byteLength,
    async slice(start, end) {
      if (buffer) {
        return buffer.then(buffer => buffer.slice(start, end))
      }

      const headers = new Headers(init.headers)
      const endStr = end === undefined ? '' : end - 1
      headers.set('Range', `bytes=${start}-${endStr}`)

      const res = await fetch(url, { ...init, headers })
      if (!res.ok || !res.body) throw new Error(`fetch failed ${res.status}`)

      if (res.status === 200) {
        // Endpoint does not support range requests and returned the whole object
        buffer = res.arrayBuffer()
        return buffer.then(buffer => buffer.slice(start, end))
      } else if (res.status === 206) {
        // The endpoint supports range requests and sent us the requested range
        return res.arrayBuffer()
      } else {
        throw new Error(`fetch received unexpected status code ${res.status}`)
      }
    },
  }
}

/**
 * Returns a cached layer on top of an AsyncBuffer. For caching slices of a file
 * that are read multiple times, possibly over a network.
 *
 * @param {AsyncBuffer} file file-like object to cache
 * @param {{ minSize?: number }} [options]
 * @returns {AsyncBuffer} cached file-like object
 */
export function cachedAsyncBuffer({ byteLength, slice }, { minSize = defaultInitialFetchSize } = {}) {
  if (byteLength < minSize) {
    // Cache whole file if it's small
    const buffer = slice(0, byteLength)
    return {
      byteLength,
      async slice(start, end) {
        return (await buffer).slice(start, end)
      },
    }
  }
  const cache = new Map()
  return {
    byteLength,
    /**
     * @param {number} start
     * @param {number} [end]
     * @returns {Awaitable<ArrayBuffer>}
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
 * @import {AsyncBuffer, Awaitable, DecodedArray} from '../src/types.d.ts'
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

/**
 * Flatten a list of lists into a single list.
 *
 * @param {DecodedArray[]} [chunks]
 * @returns {DecodedArray}
 */
export function flatten(chunks) {
  if (!chunks) return []
  if (chunks.length === 1) return chunks[0]
  /** @type {any[]} */
  const output = []
  for (const chunk of chunks) {
    concat(output, chunk)
  }
  return output
}
