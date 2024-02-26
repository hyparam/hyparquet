/**
 * @typedef {import('./types.js').SchemaElement} SchemaElement
 */

const dayMillis = 86400000000000 // 1 day in milliseconds

/**
 * Convert known types from primitive to rich.
 *
 * @param {any[]} data series of primitive types
 * @param {SchemaElement} schemaElement schema element for the data
 * @returns {any[]} series of rich types
 */
export function convert(data, schemaElement) {
  const ctype = schemaElement.converted_type
  if (ctype === 'UTF8') {
    const decoder = new TextDecoder()
    return data.map(v => decoder.decode(v))
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
 * Parse decimal from byte array.
 *
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
