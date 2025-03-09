import { readVarInt } from './thrift.js'

/**
 * Minimum bits needed to store value.
 *
 * @param {number} value
 * @returns {number}
 */
export function bitWidth(value) {
  return 32 - Math.clz32(value)
}

/**
 * Read values from a run-length encoded/bit-packed hybrid encoding.
 *
 * If length is zero, then read int32 length at the start.
 *
 * @param {DataReader} reader
 * @param {number} width - width of each bit-packed group
 * @param {number} length - length of the encoded data
 * @param {DecodedArray} output
 */
export function readRleBitPackedHybrid(reader, width, length, output) {
  if (!length) {
    // length = reader.view.getUint32(reader.offset, true)
    reader.offset += 4
  }
  let seen = 0
  while (seen < output.length) {
    const header = readVarInt(reader)
    if (header & 1) {
      // bit-packed
      seen = readBitPacked(reader, header, width, output, seen)
    } else {
      // rle
      const count = header >>> 1
      readRle(reader, count, width, output, seen)
      seen += count
    }
  }
  // assert(reader.offset - startOffset === length)
}

/**
 * Run-length encoding: read value with bitWidth and repeat it count times.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @param {number} bitWidth
 * @param {DecodedArray} output
 * @param {number} seen
 */
function readRle(reader, count, bitWidth, output, seen) {
  const width = bitWidth + 7 >> 3
  let value = 0
  for (let i = 0; i < width; i++) {
    value |= reader.view.getUint8(reader.offset++) << (i << 3)
  }
  // assert(value < 1 << bitWidth)

  // repeat value count times
  for (let i = 0; i < count; i++) {
    output[seen + i] = value
  }
}

/**
 * Read a bit-packed run of the rle/bitpack hybrid.
 * Supports width > 8 (crossing bytes).
 *
 * @param {DataReader} reader
 * @param {number} header - bit-pack header
 * @param {number} bitWidth
 * @param {DecodedArray} output
 * @param {number} seen
 * @returns {number} total output values so far
 */
function readBitPacked(reader, header, bitWidth, output, seen) {
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
      data >>>= 8
    } else if (left - right < bitWidth) {
      // if we don't have bitWidth number of bits to read, read next byte
      data |= reader.view.getUint8(reader.offset) << left
      reader.offset++
      left += 8
    } else {
      if (seen < output.length) {
        // emit value
        output[seen++] = data >> right & mask
      }
      count--
      right += bitWidth
    }
  }

  return seen
}

/**
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
 * @import {DataReader, DecodedArray, ParquetType} from '../src/types.d.ts'
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
