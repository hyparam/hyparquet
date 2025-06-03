const dayMillis = 86400000 // 1 day in milliseconds

/**
 * Convert known types from primitive to rich, and dereference dictionary.
 *
 * @param {DecodedArray} data series of primitive types
 * @param {DecodedArray | undefined} dictionary
 * @param {Encoding} encoding
 * @param {ColumnDecoder} columnDecoder
 * @returns {DecodedArray} series of rich types
 */
export function convertWithDictionary(data, dictionary, encoding, columnDecoder) {
  if (dictionary && encoding.endsWith('_DICTIONARY')) {
    let output = data
    if (data instanceof Uint8Array && !(dictionary instanceof Uint8Array)) {
      // @ts-expect-error upgrade data to match dictionary type with fancy constructor
      output = new dictionary.constructor(data.length)
    }
    for (let i = 0; i < data.length; i++) {
      output[i] = dictionary[data[i]]
    }
    return output
  } else {
    return convert(data, columnDecoder)
  }
}

/**
 * Convert known types from primitive to rich.
 *
 * @param {DecodedArray} data series of primitive types
 * @param {Pick<ColumnDecoder, "element" | "utf8">} columnDecoder
 * @returns {DecodedArray} series of rich types
 */
export function convert(data, columnDecoder) {
  const { element, utf8 = true } = columnDecoder
  const { type, converted_type: ctype, logical_type: ltype } = element
  if (ctype === 'DECIMAL') {
    const scale = element.scale || 0
    const factor = 10 ** -scale
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
  if (!ctype && type === 'INT96') {
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
  if (ctype === 'UTF8' || ltype?.type === 'STRING' || utf8 && type === 'BYTE_ARRAY') {
    const decoder = new TextDecoder()
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = data[i] && decoder.decode(data[i])
    }
    return arr
  }
  if (ctype === 'UINT_64' || ltype?.type === 'INTEGER' && ltype.bitWidth === 64 && !ltype.isSigned) {
    if (data instanceof BigInt64Array) {
      return new BigUint64Array(data.buffer, data.byteOffset, data.length)
    }
    const arr = new BigUint64Array(data.length)
    for (let i = 0; i < arr.length; i++) arr[i] = BigInt(data[i])
    return arr
  }
  if (ctype === 'UINT_32' || ltype?.type === 'INTEGER' && ltype.bitWidth === 32 && !ltype.isSigned) {
    if (data instanceof Int32Array) {
      return new Uint32Array(data.buffer, data.byteOffset, data.length)
    }
    const arr = new Uint32Array(data.length)
    for (let i = 0; i < arr.length; i++) arr[i] = data[i]
    return arr
  }
  if (ltype?.type === 'FLOAT16') {
    return Array.from(data).map(parseFloat16)
  }
  if (ltype?.type === 'TIMESTAMP') {
    const { unit } = ltype
    let factor = 1n
    if (unit === 'MICROS') factor = 1000n
    if (unit === 'NANOS') factor = 1000000n
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = new Date(Number(data[i] / factor))
    }
    return arr
  }
  return data
}

/**
 * @param {Uint8Array} bytes
 * @returns {number}
 */
export function parseDecimal(bytes) {
  let value = 0
  for (const byte of bytes) {
    value = value * 256 + byte
  }

  // handle signed
  const bits = bytes.length * 8
  if (value >= 2 ** (bits - 1)) {
    value -= 2 ** bits
  }

  return value
}

/**
 * @import {ColumnDecoder, DecodedArray, Encoding, SchemaElement} from '../src/types.d.ts'
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
  if (exp === 0) return sign * 2 ** -14 * (frac / 1024) // subnormals
  if (exp === 0x1f) return frac ? NaN : sign * Infinity
  return sign * 2 ** (exp - 15) * (1 + frac / 1024)
}
