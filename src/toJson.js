/**
 * Replace bigints with numbers.
 * When parsing parquet files, bigints are used to represent 64-bit integers.
 * However, JSON does not support bigints, so it's helpful to convert to numbers.
 *
 * @param {any} obj object to convert
 * @returns {unknown} converted object
 */
export function toJson(obj) {
  if (typeof obj === 'bigint') {
    return Number(obj)
  } else if (Array.isArray(obj)) {
    return obj.map(toJson)
  } else if (obj instanceof Object) {
    /** @type {Record<string, unknown>} */
    const newObj = {}
    for (const key of Object.keys(obj)) {
      newObj[key] = toJson(obj[key])
    }
    return newObj
  } else {
    return obj
  }
}
