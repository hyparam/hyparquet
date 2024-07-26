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
 * Construct an AsyncBuffer for a URL.
 *
 * @typedef {import('./types.js').AsyncBuffer} AsyncBuffer
 * @param {string} url
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromUrl(url) {
  // byte length from HEAD request
  const byteLength = await fetch(url, { method: 'HEAD' })
    .then(res => {
      if (!res.ok) throw new Error(`fetch head failed ${res.status}`)
      const length = res.headers.get('Content-Length')
      if (!length) throw new Error('missing content length')
      return parseInt(length)
    })
  return {
    byteLength,
    async slice(start, end) {
      // fetch byte range from url
      const headers = new Headers()
      const endStr = end === undefined ? '' : end - 1
      headers.set('Range', `bytes=${start}-${endStr}`)
      const res = await fetch(url, { headers })
      if (!res.ok || !res.body) throw new Error(`fetch failed ${res.status}`)
      return res.arrayBuffer()
    },
  }
}
