import { Encoding, ParquetType } from './constants.js'
import { readVarInt } from './thrift.js'

/**
 * Return type with bytes read.
 * This is useful to advance an offset through a buffer.
 *
 * @typedef {import("./types.d.ts").Decoded<T>} Decoded
 * @template T
 */

/**
 * Read `count` boolean values.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} count - number of values to read
 * @returns {Decoded<boolean[]>} array of boolean values
 */
function readPlainBoolean(dataView, offset, count) {
  const value = []
  for (let i = 0; i < count; i++) {
    const byteOffset = offset + Math.floor(i / 8)
    const bitOffset = i % 8
    const byte = dataView.getUint8(byteOffset)
    value.push((byte & (1 << bitOffset)) !== 0)
  }
  return { value, byteLength: Math.ceil(count / 8) }
}

/**
 * Read `count` int32 values.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} count - number of values to read
 * @returns {Decoded<number[]>} array of int32 values
 */
function readPlainInt32(dataView, offset, count) {
  const value = []
  for (let i = 0; i < count; i++) {
    value.push(dataView.getInt32(offset + i * 4, true))
  }
  return { value, byteLength: count * 4 }
}

/**
 * Read `count` int64 values.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} count - number of values to read
 * @returns {Decoded<bigint[]>} array of int64 values
 */
function readPlainInt64(dataView, offset, count) {
  const value = []
  for (let i = 0; i < count; i++) {
    value.push(dataView.getBigInt64(offset + i * 8, true))
  }
  return { value, byteLength: count * 8 }
}

/**
 * Read `count` int96 values.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} count - number of values to read
 * @returns {Decoded<bigint[]>} array of int96 values
 */
function readPlainInt96(dataView, offset, count) {
  const value = []
  for (let i = 0; i < count; i++) {
    const low = dataView.getBigInt64(offset + i * 12, true)
    const high = dataView.getInt32(offset + i * 12 + 8, true)
    value.push((BigInt(high) << BigInt(32)) | low)
  }
  return { value, byteLength: count * 12 }
}

/**
 * Read `count` float values.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} count - number of values to read
 * @returns {Decoded<number[]>} array of float values
 */
function readPlainFloat(dataView, offset, count) {
  const value = []
  for (let i = 0; i < count; i++) {
    value.push(dataView.getFloat32(offset + i * 4, true))
  }
  return { value, byteLength: count * 4 }
}

/**
 * Read `count` double values.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} count - number of values to read
 * @returns {Decoded<number[]>} array of double values
 */
function readPlainDouble(dataView, offset, count) {
  const value = []
  for (let i = 0; i < count; i++) {
    value.push(dataView.getFloat64(offset + i * 8, true))
  }
  return { value, byteLength: count * 8 }
}

/**
 * Read `count` byte array values.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} count - number of values to read
 * @returns {Decoded<Uint8Array[]>} array of byte arrays
 */
function readPlainByteArray(dataView, offset, count) {
  const value = []
  let byteLength = 0 // byte length of all data read
  for (let i = 0; i < count; i++) {
    const length = dataView.getInt32(offset + byteLength, true)
    byteLength += 4
    const bytes = new Uint8Array(dataView.buffer, dataView.byteOffset + offset + byteLength, length)
    value.push(bytes)
    byteLength += length
  }
  return { value, byteLength }
}

/**
 * Read a fixed length byte array.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} fixedLength - length of each fixed length byte array
 * @returns {Decoded<Uint8Array>} array of fixed length byte arrays
 */
function readPlainByteArrayFixed(dataView, offset, fixedLength) {
  return {
    value: new Uint8Array(dataView.buffer, dataView.byteOffset + offset, fixedLength),
    byteLength: fixedLength,
  }
}

/**
 * Read `count` values of the given type from the dataView.
 *
 * @typedef {import("./types.d.ts").DecodedArray} DecodedArray
 * @param {DataView} dataView - buffer to read data from
 * @param {number} type - parquet type of the data
 * @param {number} count - number of values to read
 * @param {number} offset - offset to start reading from the DataView
 * @param {boolean} utf8 - whether to decode byte arrays as UTF-8
 * @returns {Decoded<DecodedArray>} array of values
 */
export function readPlain(dataView, type, count, offset, utf8) {
  if (count === 0) return { value: [], byteLength: 0 }
  if (type === ParquetType.BOOLEAN) {
    return readPlainBoolean(dataView, offset, count)
  } else if (type === ParquetType.INT32) {
    return readPlainInt32(dataView, offset, count)
  } else if (type === ParquetType.INT64) {
    return readPlainInt64(dataView, offset, count)
  } else if (type === ParquetType.INT96) {
    return readPlainInt96(dataView, offset, count)
  } else if (type === ParquetType.FLOAT) {
    return readPlainFloat(dataView, offset, count)
  } else if (type === ParquetType.DOUBLE) {
    return readPlainDouble(dataView, offset, count)
  } else if (type === ParquetType.BYTE_ARRAY) {
    const byteArray = readPlainByteArray(dataView, offset, count)
    if (utf8) {
      const decoder = new TextDecoder()
      return {
        value: byteArray.value.map(bytes => decoder.decode(bytes)),
        byteLength: byteArray.byteLength,
      }
    }
    return byteArray
  } else if (type === ParquetType.FIXED_LEN_BYTE_ARRAY) {
    return readPlainByteArrayFixed(dataView, offset, count)
  } else {
    throw new Error(`parquet unhandled type: ${type}`)
  }
}

/**
 * Convert the value specified to a bit width.
 *
 * @param {number} value - value to convert to bitwidth
 * @returns {number} bit width of the value
 */
export function widthFromMaxInt(value) {
  return Math.ceil(Math.log2(value + 1))
}

/**
 * Read data from the file-object using the given encoding.
 * The data could be definition levels, repetition levels, or actual values.
 *
 * @typedef {import("./types.d.ts").Encoding} Encoding
 * @param {DataView} dataView - buffer to read data from
 * @param {Encoding} encoding - encoding type
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} count - number of values to read
 * @param {number} bitWidth - width of each bit-packed group
 * @returns {Decoded<any>} array of values
 */
export function readData(dataView, encoding, offset, count, bitWidth) {
  const value = []
  let byteLength = 0
  if (encoding === Encoding.RLE) {
    let seen = 0
    while (seen < count) {
      const rle = readRleBitPackedHybrid(dataView, offset + byteLength, bitWidth, 0, count)
      if (!rle.value.length) break // EOF
      value.push(...rle.value)
      seen += rle.value.length
      byteLength += rle.byteLength
    }
  } else {
    throw new Error(`parquet encoding not supported ${encoding}`)
  }
  return { value, byteLength }
}

/**
 * Read values from a run-length encoded/bit-packed hybrid encoding.
 *
 * If length is zero, then read as int32 at the start of the encoded data.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} width - width of each bit-packed group
 * @param {number} length - length of the encoded data
 * @param {number} numValues - number of values to read
 * @returns {Decoded<number[]>} array of rle/bit-packed values
 */
export function readRleBitPackedHybrid(dataView, offset, width, length, numValues) {
  let byteLength = 0
  if (!length) {
    length = dataView.getInt32(offset, true)
    if (length < 0) throw new Error(`parquet invalid rle/bitpack length ${length}`)
    byteLength += 4
  }
  const value = []
  const startByteLength = byteLength
  while (byteLength - startByteLength < length) {
    const [header, newOffset] = readVarInt(dataView, offset + byteLength)
    byteLength = newOffset - offset
    if ((header & 1) === 0) {
      // rle
      const rle = readRle(dataView, offset + byteLength, header, width)
      value.push(...rle.value)
      byteLength += rle.byteLength
    } else {
      // bit-packed
      const bitPacked = readBitPacked(
        dataView, offset + byteLength, header, width, numValues - value.length
      )
      value.push(...bitPacked.value)
      byteLength += bitPacked.byteLength
    }
  }

  return { value, byteLength }
}

/**
 * Read a run-length encoded value.
 *
 * The count is determined from the header and the width is used to grab the
 * value that's repeated. Yields the value repeated count times.
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} header - header information
 * @param {number} bitWidth - width of each bit-packed group
 * @returns {Decoded<number[]>} array of rle values
 */
function readRle(dataView, offset, header, bitWidth) {
  const count = header >>> 1
  const width = (bitWidth + 7) >> 3
  let byteLength = 0
  let readValue
  if (width === 1) {
    readValue = dataView.getUint8(offset)
    byteLength += 1
  } else if (width === 2) {
    readValue = dataView.getUint16(offset, true)
    byteLength += 2
  } else if (width === 4) {
    readValue = dataView.getUint32(offset, true)
    byteLength += 4
  } else {
    throw new Error(`parquet invalid rle width ${width}`)
  }

  // repeat value count times
  const value = []
  for (let i = 0; i < count; i++) {
    value.push(readValue)
  }
  return { value, byteLength }
}

/**
 * Read a bit-packed run of the rle/bitpack hybrid.
 * Supports width > 8 (crossing bytes).
 *
 * @param {DataView} dataView - buffer to read data from
 * @param {number} offset - offset to start reading from the DataView
 * @param {number} header - header information
 * @param {number} bitWidth - width of each bit-packed group
 * @param {number} remaining - number of values remaining to be read
 * @returns {Decoded<number[]>} array of bit-packed values
 */
function readBitPacked(dataView, offset, header, bitWidth, remaining) {
  // extract number of values to read from header
  let count = (header >> 1) << 3
  const mask = maskForBits(bitWidth)

  // Sometimes it tries to read outside of available memory, but it will be masked out anyway
  let data = 0
  if (offset < dataView.byteLength) {
    data = dataView.getUint8(offset)
  } else if (mask) {
    throw new Error(`parquet bitpack offset ${offset} out of range`)
  }
  let byteLength = 1
  let left = 8
  let right = 0
  /** @type {number[]} */
  const value = []

  // read values
  while (count) {
    // if we have crossed a byte boundary, shift the data
    if (right > 8) {
      right -= 8
      left -= 8
      data >>= 8
    } else if (left - right < bitWidth) {
      // if we don't have bitWidth number of bits to read, read next byte
      data |= dataView.getUint8(offset + byteLength) << left
      byteLength++
      left += 8
    } else {
      // otherwise, read bitWidth number of bits
      // don't write more than remaining number of rows
      // even if there are still bits to read
      if (remaining > 0) {
        // emit value by shifting off to the right and masking
        value.push((data >> right) & mask)
        remaining--
      }
      count--
      right += bitWidth
    }
  }

  // return values and number of bytes read
  return { value, byteLength }
}

/**
* Generate a mask for the given number of bits.
*
* @param {number} bits - number of bits for the mask
* @returns {number} a mask for the given number of bits
*/
function maskForBits(bits) {
  return (1 << bits) - 1
}
