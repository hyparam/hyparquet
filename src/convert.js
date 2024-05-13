const dayMillis = 86400000 // 1 day in milliseconds

/**
 * Convert known types from primitive to rich.
 *
 * @typedef {import('./types.js').DecodedArray} DecodedArray
 * @param {DecodedArray} data series of primitive types
 * @param {import('./types.js').SchemaElement} schemaElement schema element for the data
 * @returns {DecodedArray} series of rich types
 */
export function convert(data, schemaElement) {
  if (!Array.isArray(data)) return data
  const ctype = schemaElement.converted_type
  if (ctype === 'UTF8') {
    const decoder = new TextDecoder()
    return data.map(v => v && decoder.decode(v))
  }
  if (ctype === 'DECIMAL') {
    const scaleFactor = schemaElement.scale ? Math.pow(10, schemaElement.scale) : 1
    if (typeof data[0] === 'number') {
      return scaleFactor === 1 ? data : data.map(v => v * scaleFactor)
    } else if (typeof data[0] === 'bigint') {
      return scaleFactor === 1 ? data : data.map(v => v * BigInt(scaleFactor))
    } else {
      return data.map(v => parseDecimal(v) * scaleFactor)
    }
  }
  if (ctype === 'DATE') {
    return data.map(v => new Date(v * dayMillis))
  }
  if (ctype === undefined && schemaElement.type === 'INT96') {
    return data.map(parseInt96Date)
  }
  if (ctype === 'TIME_MILLIS') {
    return data.map(v => new Date(v))
  }
  if (ctype === 'JSON') {
    return data.map(v => JSON.parse(v))
  }
  if (ctype === 'BSON') {
    throw new Error('parquet bson not supported')
  }
  if (ctype === 'INTERVAL') {
    throw new Error('parquet interval not supported')
  }
  return data
}

/**
 * @param {Uint8Array} bytes
 * @returns {number}
 */
function parseDecimal(bytes) {
  // TODO: handle signed
  let value = 0
  for (const byte of bytes) {
    value = value << 8 | byte
  }
  return value
}

/**
 * @param {bigint} value
 * @returns {Date}
 */
function parseInt96Date(value) {
  const days = Number((value >> 64n) - 2440588n)
  const nano = Number((value & 0xffffffffffffffffn) / 1000000n)
  const millis = days * dayMillis + nano
  return new Date(millis)
}
