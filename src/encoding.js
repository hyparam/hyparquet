import { readVarInt } from './thrift.js'

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
 * @typedef {import("./types.d.ts").DecodedArray} DecodedArray
 * @param {DataReader} reader - buffer to read data from
 * @param {number} width - width of each bit-packed group
 * @param {number} length - length of the encoded data
 * @param {DecodedArray} values - output array
 */
export function readRleBitPackedHybrid(reader, width, length, values) {
  if (!length) {
    length = reader.view.getUint32(reader.offset, true)
    reader.offset += 4
  }
  let seen = 0
  while (seen < values.length) {
    const header = readVarInt(reader)
    if (header & 1) {
      // bit-packed
      seen = readBitPacked(reader, header, width, values, seen)
    } else {
      // rle
      const count = header >>> 1
      readRle(reader, count, width, values, seen)
      seen += count
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
 * @param {DecodedArray} values - output array
 * @param {number} seen - number of values seen so far
 */
function readRle(reader, count, bitWidth, values, seen) {
  const width = bitWidth + 7 >> 3
  let value = 0
  if (width === 1) {
    value = reader.view.getUint8(reader.offset)
  } else if (width === 2) {
    value = reader.view.getUint16(reader.offset, true)
  } else if (width === 4) {
    value = reader.view.getUint32(reader.offset, true)
  } else if (width) {
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
 * @param {DecodedArray} values - output array
 * @param {number} seen - number of values seen so far
 * @returns {number} number of values seen
 */
function readBitPacked(reader, header, bitWidth, values, seen) {
  let count = header >> 1 << 3 // values to read
  const mask = (1 << bitWidth) - 1

  let data = 0
  if (reader.offset < reader.view.byteLength) {
    data = reader.view.getUint8(reader.offset++)
  } else if (mask) {
    // sometimes out-of-bounds reads are masked out
    throw new Error(`parquet bitpack offset ${reader.offset} out of range`)
  }
  let left = 8
  let right = 0

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
      if (seen < values.length) {
        // emit value
        values[seen++] = data >> right & mask
      }
      count--
      right += bitWidth
    }
  }

  return seen
}

/**
 * @typedef {import("./types.d.ts").ParquetType} ParquetType
 * @param {DataReader} reader
 * @param {number} count
 * @param {ParquetType} type
 * @param {number | undefined} typeLength
 * @returns {DecodedArray}
 */
export function byteStreamSplit(reader, count, type, typeLength) {
  const width = byteWidth(type, typeLength)
  const bytes = new Uint8Array(count * width)
  for (let b = 0; b < width; b++) {
    for (let i = 0; i < count; i++) {
      bytes[i * width + b] = reader.view.getUint8(reader.offset++)
    }
  }
  // interpret bytes as typed array
  if (type === 'FLOAT') return new Float32Array(bytes.buffer)
  else if (type === 'DOUBLE') return new Float64Array(bytes.buffer)
  else if (type === 'INT32') return new Int32Array(bytes.buffer)
  else if (type === 'INT64') return new BigInt64Array(bytes.buffer)
  else if (type === 'FIXED_LEN_BYTE_ARRAY') {
    // split into arrays of typeLength
    const split = new Array(count)
    for (let i = 0; i < count; i++) {
      split[i] = bytes.subarray(i * width, (i + 1) * width)
    }
    return split
  }
  throw new Error(`parquet byte_stream_split unsupported type: ${type}`)
}

/**
 * @param {ParquetType} type
 * @param {number | undefined} typeLength
 * @returns {number}
 */
function byteWidth(type, typeLength) {
  switch (type) {
  case 'INT32':
  case 'FLOAT':
    return 4
  case 'INT64':
  case 'DOUBLE':
    return 8
  case 'FIXED_LEN_BYTE_ARRAY':
    if (!typeLength) throw new Error('parquet byteWidth missing type_length')
    return typeLength
  default:
    throw new Error(`parquet unsupported type: ${type}`)
  }
}
