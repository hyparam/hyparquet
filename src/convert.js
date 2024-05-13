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
  const ctype = schemaElement.converted_type
  if (ctype === 'UTF8') {
    const decoder = new TextDecoder()
    return data.map(v => v && decoder.decode(v))
  }
  if (ctype === 'DECIMAL') {
    const scale = schemaElement.scale || 0
    const factor = Math.pow(10, -scale)
    if (typeof data[0] === 'number') {
      if (factor === 1) return data
      return Array.from(data).map(v => v * factor)
    } else if (typeof data[0] === 'bigint') {
      if (factor === 1) return data
      return Array.from(data).map(v => Number(v) * factor)
    } else {
      return Array.from(data).map(v => parseDecimal(v) * factor)
    }
  }
  if (ctype === 'DATE') {
    return Array.from(data).map(v => new Date(v * dayMillis))
  }
  if (ctype === undefined && schemaElement.type === 'INT96') {
    return Array.from(data).map(parseInt96Date)
  }
  if (ctype === 'TIME_MILLIS') {
    return Array.from(data).map(v => new Date(v))
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
