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
      seen = readBitPacked(reader, header, width, values, seen)
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
 * @param {number[]} values - output array
 * @param {number} seen - number of values seen so far
 * @returns {number} number of values seen
 */
function readBitPacked(reader, header, bitWidth, values, seen) {
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
        // emit value by shifting off to the right and masking
        values[seen++] = (data >> right) & mask
      }
      count--
      right += bitWidth
    }
  }

  return seen
}
