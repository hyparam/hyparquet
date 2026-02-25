import { wkbToGeojson } from './wkb.js'

/**
 * @import {ColumnDecoder, DecodedArray, Encoding, ParquetParsers} from '../src/types.js'
 */

const decoder = new TextDecoder()

/**
 * Default type parsers when no custom ones are given
 * @type ParquetParsers
 */
export const DEFAULT_PARSERS = {
  timestampFromMilliseconds(millis) {
    return new Date(Number(millis))
  },
  timestampFromMicroseconds(micros) {
    return new Date(Number(micros / 1000n))
  },
  timestampFromNanoseconds(nanos) {
    return new Date(Number(nanos / 1000000n))
  },
  dateFromDays(days) {
    return new Date(days * 86400000)
  },
  stringFromBytes(bytes) {
    return bytes && decoder.decode(bytes)
  },
  geometryFromBytes(bytes) {
    return bytes && wkbToGeojson({ view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength), offset: 0 })
  },
  geographyFromBytes(bytes) {
    return bytes && wkbToGeojson({ view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength), offset: 0 })
  },
}

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
 * @param {ColumnDecoder} columnDecoder
 * @returns {DecodedArray} series of rich types
 */
export function convert(data, columnDecoder) {
  const { element, parsers, utf8 = true, schemaPath } = columnDecoder
  const { type, converted_type: ctype, logical_type: ltype } = element
  const nullable = element.repetition_type !== 'REQUIRED'

  // Skip utf8 conversion for plain BYTE_ARRAY inside VARIANT
  const isVariant = schemaPath?.some(s => s.element.logical_type?.type === 'VARIANT')
  if (isVariant && type === 'BYTE_ARRAY' && ctype !== 'UTF8' && ltype?.type !== 'STRING') {
    return data
  }
  if (ctype === 'DECIMAL') {
    const scale = element.scale || 0
    const factor = 10 ** -scale
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      if (data[i] instanceof Uint8Array) {
        arr[i] = parseDecimal(data[i]) * factor
      } else {
        arr[i] = Number(data[i]) * factor
      }
    }
    return arr
  }
  if (!ctype && type === 'INT96') {
    return Array.from(data).map(v => parsers.timestampFromNanoseconds(parseInt96Nanos(v)))
  }
  if (ctype === 'DATE') {
    return Array.from(data).map(v => parsers.dateFromDays(v))
  }
  if (ctype === 'TIMESTAMP_MILLIS') {
    return Array.from(data).map(v => parsers.timestampFromMilliseconds(v))
  }
  if (ctype === 'TIMESTAMP_MICROS') {
    return Array.from(data).map(v => parsers.timestampFromMicroseconds(v))
  }
  if (ctype === 'JSON') {
    return data.map(v => JSON.parse(decoder.decode(v)))
  }
  if (ctype === 'BSON') {
    throw new Error('parquet bson not supported')
  }
  if (ctype === 'INTERVAL') {
    throw new Error('parquet interval not supported')
  }
  if (ltype?.type === 'GEOMETRY') {
    return data.map(v => parsers.geometryFromBytes(v))
  }
  if (ltype?.type === 'GEOGRAPHY') {
    return data.map(v => parsers.geographyFromBytes(v))
  }
  if (ctype === 'UTF8' || ltype?.type === 'STRING' || utf8 && type === 'BYTE_ARRAY') {
    return data.map(v => parsers.stringFromBytes(v))
  }
  if (ctype === 'UINT_64' || ltype?.type === 'INTEGER' && ltype.bitWidth === 64 && !ltype.isSigned) {
    if (data instanceof BigInt64Array) return new BigUint64Array(data.buffer, data.byteOffset, data.length)
    const arr = nullable ? new Array(data.length) : new BigUint64Array(data.length)
    for (let i = 0; i < arr.length; i++) arr[i] = data[i]
    return arr
  }
  if (ctype === 'UINT_32' || ltype?.type === 'INTEGER' && ltype.bitWidth === 32 && !ltype.isSigned) {
    if (data instanceof Int32Array) return new Uint32Array(data.buffer, data.byteOffset, data.length)
    const arr = nullable ? new Array(data.length) : new Uint32Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = data[i] < 0 ? 4294967296 + data[i] : data[i]
    }
    return arr
  }
  if (ltype?.type === 'FLOAT16') {
    return Array.from(data).map(parseFloat16)
  }
  if (ltype?.type === 'TIMESTAMP') {
    const { unit } = ltype
    /** @type {ParquetParsers[keyof ParquetParsers]} */
    let parser = parsers.timestampFromMilliseconds
    if (unit === 'MICROS') parser = parsers.timestampFromMicroseconds
    if (unit === 'NANOS') parser = parsers.timestampFromNanoseconds
    const arr = new Array(data.length)
    for (let i = 0; i < arr.length; i++) {
      arr[i] = parser(data[i])
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
  if (!bytes.length) return 0

  let value = 0n
  for (const byte of bytes) {
    value = value * 256n + BigInt(byte)
  }

  // handle signed
  const bits = bytes.length * 8
  if (value >= 2n ** BigInt(bits - 1)) {
    value -= 2n ** BigInt(bits)
  }

  return Number(value)
}

/**
 * Converts INT96 date format (hi 32bit days, lo 64bit nanos) to nanos since epoch
 * @param {bigint} value
 * @returns {bigint}
 */
function parseInt96Nanos(value) {
  const days = (value >> 64n) - 2440588n
  const nano = value & 0xffffffffffffffffn
  return days * 86400000000000n + nano
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
