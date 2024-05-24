const dayMillis = 86400000 // 1 day in milliseconds

/**
 * Convert known types from primitive to rich.
 *
 * @typedef {import('./types.js').DecodedArray} DecodedArray
 * @param {DecodedArray} data series of primitive types
 * @param {import('./types.js').SchemaElement} schemaElement schema element for the data
 * @param {boolean | undefined} utf8 decode bytes as utf8?
 * @returns {DecodedArray} series of rich types
 */
export function convert(data, schemaElement, utf8 = true) {
  const ctype = schemaElement.converted_type
  if (ctype === 'DECIMAL') {
    const scale = schemaElement.scale || 0
    const factor = Math.pow(10, -scale)
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      if (data[0] instanceof Uint8Array) {
        arr[i] = parseDecimal(data[i]) * factor
      } else {
        arr[i] = Number(data[i]) * factor
      }
    }
    return arr
  }
  if (ctype === undefined && schemaElement.type === 'INT96') {
    return Array.from(data).map(parseInt96Date)
  }
  if (ctype === 'DATE') {
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = new Date(data[i] * dayMillis)
    }
    return arr
  }
  if (ctype === 'TIMESTAMP_MILLIS') {
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = new Date(Number(data[i]))
    }
    return arr
  }
  if (ctype === 'TIMESTAMP_MICROS') {
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = new Date(Number(data[i] / 1000n))
    }
    return arr
  }
  if (ctype === 'JSON') {
    const decoder = new TextDecoder()
    return data.map(v => JSON.parse(decoder.decode(v)))
  }
  if (ctype === 'BSON') {
    throw new Error('parquet bson not supported')
  }
  if (ctype === 'INTERVAL') {
    throw new Error('parquet interval not supported')
  }
  if (ctype === 'UTF8' || utf8 && schemaElement.type === 'BYTE_ARRAY') {
    const decoder = new TextDecoder()
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = data[i] && decoder.decode(data[i])
    }
    return arr
  }
  if (ctype === 'UINT_64') {
    const arr = new BigUint64Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = BigInt(data[i])
    }
    return arr
  }
  const logicalType = schemaElement.logical_type?.type
  if (logicalType === 'FLOAT16') {
    return Array.from(data).map(parseFloat16)
  }
  if (logicalType === 'TIMESTAMP') {
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = new Date(Number(data[i]))
    }
    return arr
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

/**
 * @param {Uint8Array | undefined} bytes
 * @returns {number | undefined}
 */
export function parseFloat16(bytes) {
  if (!bytes) return undefined
  const int16 = bytes[1] << 8 | bytes[0]
  const sign = int16 >> 15 ? -1 : 1
  const exp = int16 >> 10 & 0x1f
  const frac = int16 & 0x3ff
  if (exp === 0) return sign * Math.pow(2, -14) * (frac / 1024) // subnormals
  if (exp === 0x1f) return frac ? NaN : sign * Infinity
  return sign * Math.pow(2, exp - 15) * (1 + frac / 1024)
}

/**
 * Map data to dictionary values in place.
 *
 * @param {DecodedArray | undefined} dictionary
 * @param {DecodedArray} dataPage
 * @returns {DecodedArray}
 */
export function dereferenceDictionary(dictionary, dataPage) {
  let output = dataPage
  if (dictionary) {
    if (dataPage instanceof Uint8Array && !(dictionary instanceof Uint8Array)) {
      // upgrade dataPage to match dictionary type
      // @ts-expect-error not my fault typescript doesn't understand constructors
      output = new dictionary.constructor(dataPage.length)
    }
    for (let i = 0; i < dataPage.length; i++) {
      output[i] = dictionary[dataPage[i]]
    }
  }
  return output
}
