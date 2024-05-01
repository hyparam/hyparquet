import { readVarInt } from './thrift.js'
import { splice } from './utils.js'

/**
 * Read `count` boolean values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {boolean[]} array of boolean values
 */
function readPlainBoolean(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const byteOffset = reader.offset + Math.floor(i / 8)
    const bitOffset = i % 8
    const byte = reader.view.getUint8(byteOffset)
    values[i] = (byte & (1 << bitOffset)) !== 0
  }
  reader.offset += Math.ceil(count / 8)
  return values
}

/**
 * Read `count` int32 values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {number[]} array of int32 values
 */
function readPlainInt32(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = reader.view.getInt32(reader.offset + i * 4, true)
  }
  reader.offset += count * 4
  return values
}

/**
 * Read `count` int64 values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {bigint[]} array of int64 values
 */
function readPlainInt64(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = reader.view.getBigInt64(reader.offset + i * 8, true)
  }
  reader.offset += count * 8
  return values
}

/**
 * Read `count` int96 values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {bigint[]} array of int96 values
 */
function readPlainInt96(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const low = reader.view.getBigInt64(reader.offset + i * 12, true)
    const high = reader.view.getInt32(reader.offset + i * 12 + 8, true)
    values[i] = (BigInt(high) << BigInt(32)) | low
  }
  reader.offset += count * 12
  return values
}

/**
 * Read `count` float values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {number[]} array of float values
 */
function readPlainFloat(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = reader.view.getFloat32(reader.offset + i * 4, true)
  }
  reader.offset += count * 4
  return values
}

/**
 * Read `count` double values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {number[]} array of double values
 */
function readPlainDouble(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = reader.view.getFloat64(reader.offset + i * 8, true)
  }
  reader.offset += count * 8
  return values
}

/**
 * Read `count` byte array values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {Uint8Array[]} array of byte arrays
 */
function readPlainByteArray(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const length = reader.view.getInt32(reader.offset, true)
    reader.offset += 4
    values[i] = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
    reader.offset += length
  }
  return values
}

/**
 * Read a fixed length byte array.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} fixedLength - length of each fixed length byte array
 * @returns {Uint8Array} array of fixed length byte arrays
 */
function readPlainByteArrayFixed(reader, fixedLength) {
  reader.offset += fixedLength
  return new Uint8Array(
    reader.view.buffer,
    reader.view.byteOffset + reader.offset - fixedLength,
    fixedLength
  )
}

/**
 * Read `count` values of the given type from the reader.view.
 *
 * @typedef {import("./types.d.ts").DecodedArray} DecodedArray
 * @typedef {import("./types.d.ts").ParquetType} ParquetType
 * @param {DataReader} reader - buffer to read data from
 * @param {ParquetType} type - parquet type of the data
 * @param {number} count - number of values to read
 * @param {boolean} utf8 - whether to decode byte arrays as UTF-8
 * @returns {DecodedArray} array of values
 */
export function readPlain(reader, type, count, utf8) {
  if (count === 0) return []
  if (type === 'BOOLEAN') {
    return readPlainBoolean(reader, count)
  } else if (type === 'INT32') {
    return readPlainInt32(reader, count)
  } else if (type === 'INT64') {
    return readPlainInt64(reader, count)
  } else if (type === 'INT96') {
    return readPlainInt96(reader, count)
  } else if (type === 'FLOAT') {
    return readPlainFloat(reader, count)
  } else if (type === 'DOUBLE') {
    return readPlainDouble(reader, count)
  } else if (type === 'BYTE_ARRAY') {
    const byteArray = readPlainByteArray(reader, count)
    if (utf8) {
      const decoder = new TextDecoder()
      return byteArray.map(bytes => decoder.decode(bytes))
    }
    return byteArray
  } else if (type === 'FIXED_LEN_BYTE_ARRAY') {
    return readPlainByteArrayFixed(reader, count)
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
 * Read values from a run-length encoded/bit-packed hybrid encoding.
 *
 * If length is zero, then read as int32 at the start of the encoded data.
 *
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader - buffer to read data from
 * @param {number} width - width of each bit-packed group
 * @param {number} length - length of the encoded data
 * @param {number[]} values - output array
 */
export function readRleBitPackedHybrid(reader, width, length, values) {
  if (!length) {
    length = reader.view.getInt32(reader.offset, true)
    reader.offset += 4
    if (length < 0) throw new Error(`parquet invalid rle/bitpack length ${length}`)
  }
  let seen = 0
  const startOffset = reader.offset
  while (reader.offset - startOffset < length && seen < values.length) {
    const [header, newOffset] = readVarInt(reader.view, reader.offset)
    reader.offset = newOffset
    if ((header & 1) === 0) {
      // rle
      const count = header >>> 1
      readRle(reader, count, width, values, seen)
      seen += count
    } else {
      // bit-packed
      const bitPacked = readBitPacked(reader, header, width, values.length - seen)
      splice(values, bitPacked, seen)
      seen += bitPacked.length
    }
  }
}

/**
 * Read a run-length encoded value.
 *
 * The count is determined from the header and the width is used to grab the
 * value that's repeated. Yields the value repeated count times.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @param {number} bitWidth - width of each bit-packed group
 * @param {number[]} values - output array
 * @param {number} seen - number of values seen so far
 */
function readRle(reader, count, bitWidth, values, seen) {
  const width = (bitWidth + 7) >> 3
  let value
  if (width === 1) {
    value = reader.view.getUint8(reader.offset)
  } else if (width === 2) {
    value = reader.view.getUint16(reader.offset, true)
  } else if (width === 4) {
    value = reader.view.getUint32(reader.offset, true)
  } else {
    throw new Error(`parquet invalid rle width ${width}`)
  }
  reader.offset += width

  // repeat value count times
  for (let i = 0; i < count; i++) {
    values[seen + i] = value
  }
}

/**
 * Read a bit-packed run of the rle/bitpack hybrid.
 * Supports width > 8 (crossing bytes).
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} header - header information
 * @param {number} bitWidth - width of each bit-packed group
 * @param {number} remaining - number of values remaining to be read
 * @returns {number[]} array of bit-packed values
 */
function readBitPacked(reader, header, bitWidth, remaining) {
  // extract number of values to read from header
  let count = (header >> 1) << 3
  // mask for bitWidth number of bits
  const mask = (1 << bitWidth) - 1

  // Sometimes it tries to read outside of available memory, but it will be masked out anyway
  let data = 0
  if (reader.offset < reader.view.byteLength) {
    data = reader.view.getUint8(reader.offset)
    reader.offset++
  } else if (mask) {
    throw new Error(`parquet bitpack offset ${reader.offset} out of range`)
  }
  let left = 8
  let right = 0
  /** @type {number[]} */
  const values = []

  // read values
  while (count) {
    // if we have crossed a byte boundary, shift the data
    if (right > 8) {
      right -= 8
      left -= 8
      data >>= 8
    } else if (left - right < bitWidth) {
      // if we don't have bitWidth number of bits to read, read next byte
      data |= reader.view.getUint8(reader.offset) << left
      reader.offset++
      left += 8
    } else {
      if (remaining > 0) {
        // emit value by shifting off to the right and masking
        values.push((data >> right) & mask)
        remaining--
      }
      count--
      right += bitWidth
    }
  }

  return values
}
