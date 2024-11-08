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
 * @typedef {import('./types.js').DecodedArray} DecodedArray
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
 * Get the byte length of a URL using a HEAD request.
 *
 * @param {string} url
 * @param {RequestInit} [requestInit] fetch options
 * @returns {Promise<number>}
 */
export async function byteLengthFromUrl(url, requestInit) {
  return await fetch(url, { ...requestInit, method: 'HEAD' })
    .then(res => {
      if (!res.ok) throw new Error(`fetch head failed ${res.status}`)
      const length = res.headers.get('Content-Length')
      if (!length) throw new Error('missing content length')
      return parseInt(length)
    })
}

/**
 * Construct an AsyncBuffer for a URL.
 *
 * @typedef {import('./types.js').AsyncBuffer} AsyncBuffer
 * @param {object} options
 * @param {string} options.url
 * @param {number} [options.byteLength]
 * @param {RequestInit} [options.requestInit]
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromUrl({ url, byteLength, requestInit }) {
  // byte length from HEAD request
  byteLength ||= await byteLengthFromUrl(url, requestInit)
  const init = requestInit || {}
  return {
    byteLength,
    async slice(start, end) {
      // fetch byte range from url
      const headers = new Headers(init.headers)
      const endStr = end === undefined ? '' : end - 1
      headers.set('Range', `bytes=${start}-${endStr}`)
      const res = await fetch(url, { ...init, headers })
      if (!res.ok || !res.body) throw new Error(`fetch failed ${res.status}`)
      return res.arrayBuffer()
    },
  }
}

/**
 * Construct an AsyncBuffer for a local file using node fs package.
 *
 * @param {string} filename
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromFile(filename) {
  const fsPackage = 'fs' // webpack no include
  const fs = await import(fsPackage)
  const stat = await fs.promises.stat(filename)
  return {
    byteLength: stat.size,
    async slice(start, end) {
      // read file slice
      const readStream = fs.createReadStream(filename, { start, end })
      return await readStreamToArrayBuffer(readStream)
    },
  }
}

/**
 * Convert a node ReadStream to ArrayBuffer.
 *
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
