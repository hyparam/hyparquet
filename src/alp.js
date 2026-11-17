/**
 * ALP (Adaptive Lossless floating-Point) encoding decoder.
 * Supports FLOAT and DOUBLE types.
 *
 * ALP encodes floating-point values by converting them to integers using
 * decimal scaling, then applying frame of reference (FOR) encoding and
 * bit-packing. Values that cannot be losslessly converted are stored as exceptions.
 *
 * See https://github.com/apache/parquet-format/blob/master/AlpEncoding.md
 */

// Correctly-rounded powers of ten (all exactly representable as doubles)
const POW10 = [
  1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
  1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
]
const POW10_NEG = [
  1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9,
  1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18,
]
// float32 versions for FLOAT decoding (exponent range [0, 10])
const POW10F = POW10.slice(0, 11).map(Math.fround)
const POW10F_NEG = POW10_NEG.slice(0, 11).map(Math.fround)

/**
 * Decode ALP encoded data.
 *
 * @import {DataReader, DecodedArray, ParquetType} from '../src/types.d.ts'
 * @param {DataReader} reader - data reader positioned at start of ALP data
 * @param {number} count - number of values to decode
 * @param {ParquetType} type - FLOAT or DOUBLE
 * @returns {DecodedArray} decoded float or double array
 */
export function alpDecode(reader, count, type) {
  if (type === 'FLOAT') {
    return alpDecodeFloat(reader, count)
  } else if (type === 'DOUBLE') {
    return alpDecodeDouble(reader, count)
  } else {
    throw new Error(`ALP encoding unsupported type: ${type}`)
  }
}

/**
 * Read the ALP page header and offset array, returning vector layout info.
 *
 * @param {DataReader} reader
 * @returns {{ vectorSize: number, numElements: number, numVectors: number, offsetArrayStart: number }}
 */
function readAlpHeader(reader) {
  const { view } = reader
  const compressionMode = view.getUint8(reader.offset++)
  if (compressionMode !== 0) {
    throw new Error(`ALP unsupported compression mode: ${compressionMode}`)
  }
  const integerEncoding = view.getUint8(reader.offset++)
  if (integerEncoding !== 0) {
    throw new Error(`ALP unsupported integer encoding: ${integerEncoding}`)
  }
  const logVectorSize = view.getUint8(reader.offset++)
  if (logVectorSize < 3 || logVectorSize > 15) {
    throw new Error(`ALP invalid log_vector_size: ${logVectorSize}`)
  }
  const vectorSize = 1 << logVectorSize
  const numElements = view.getInt32(reader.offset, true)
  reader.offset += 4
  const numVectors = Math.ceil(numElements / vectorSize)
  const offsetArrayStart = reader.offset
  reader.offset += numVectors * 4
  return { vectorSize, numElements, numVectors, offsetArrayStart }
}

/**
 * Decode ALP encoded float data.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Float32Array}
 */
export function alpDecodeFloat(reader, count) {
  const { view } = reader
  const { vectorSize, numElements, numVectors, offsetArrayStart } = readAlpHeader(reader)
  if (numElements !== count) {
    throw new Error(`ALP num_elements ${numElements} does not match expected ${count}`)
  }

  const output = new Float32Array(count)
  const outputBytes = new Uint8Array(output.buffer)

  for (let v = 0; v < numVectors; v++) {
    reader.offset = offsetArrayStart + view.getUint32(offsetArrayStart + v * 4, true)
    const outputOffset = v * vectorSize
    const n = Math.min(vectorSize, numElements - outputOffset)

    // AlpInfo (4 bytes)
    const exponent = view.getUint8(reader.offset++)
    const factor = view.getUint8(reader.offset++)
    const numExceptions = view.getUint16(reader.offset, true)
    reader.offset += 2

    // ForInfo (5 bytes)
    const frameOfReference = view.getInt32(reader.offset, true)
    reader.offset += 4
    const bitWidth = view.getUint8(reader.offset++)

    // PackedValues
    const deltas = unpackBits(reader, n, bitWidth)

    // Reverse FOR (wrapping int32) and decimal decode in float32 arithmetic
    const mulFactor = POW10F[factor]
    const mulExponent = POW10F_NEG[exponent]
    for (let i = 0; i < n; i++) {
      const encoded = deltas[i] + frameOfReference | 0
      output[outputOffset + i] = Math.fround(Math.fround(encoded) * mulFactor) * mulExponent
    }

    patchExceptions(reader, numExceptions, 4, outputBytes, outputOffset)
  }

  return output
}

/**
 * Decode ALP encoded double data.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Float64Array}
 */
export function alpDecodeDouble(reader, count) {
  const { view } = reader
  const { vectorSize, numElements, numVectors, offsetArrayStart } = readAlpHeader(reader)
  if (numElements !== count) {
    throw new Error(`ALP num_elements ${numElements} does not match expected ${count}`)
  }

  const output = new Float64Array(count)
  const outputBytes = new Uint8Array(output.buffer)

  for (let v = 0; v < numVectors; v++) {
    reader.offset = offsetArrayStart + view.getUint32(offsetArrayStart + v * 4, true)
    const outputOffset = v * vectorSize
    const n = Math.min(vectorSize, numElements - outputOffset)

    // AlpInfo (4 bytes)
    const exponent = view.getUint8(reader.offset++)
    const factor = view.getUint8(reader.offset++)
    const numExceptions = view.getUint16(reader.offset, true)
    reader.offset += 2

    // ForInfo (9 bytes)
    const frameOfReference = view.getBigInt64(reader.offset, true)
    reader.offset += 8
    const bitWidth = view.getUint8(reader.offset++)

    const mulFactor = POW10[factor]
    const mulExponent = POW10_NEG[exponent]
    const forNumber = Number(frameOfReference)
    if (bitWidth <= 32 && Number.isSafeInteger(forNumber)) {
      // Fast path: deltas and frame of reference are exact as doubles,
      // so delta + FOR rounds identically to an int64 -> double cast.
      const deltas = unpackBits(reader, n, bitWidth)
      for (let i = 0; i < n; i++) {
        output[outputOffset + i] = (deltas[i] + forNumber) * mulFactor * mulExponent
      }
    } else {
      // Wide values: use BigInt for exact wrapping int64 arithmetic
      const deltas = unpackBitsBig(reader, n, bitWidth)
      for (let i = 0; i < n; i++) {
        const encoded = BigInt.asIntN(64, deltas[i] + frameOfReference)
        output[outputOffset + i] = Number(encoded) * mulFactor * mulExponent
      }
    }

    patchExceptions(reader, numExceptions, 8, outputBytes, outputOffset)
  }

  return output
}

/**
 * Read exception positions and values, and copy the raw IEEE-754 bytes
 * into the output (preserving NaN payloads bit-for-bit).
 *
 * @param {DataReader} reader
 * @param {number} numExceptions
 * @param {number} byteSize - 4 for float, 8 for double
 * @param {Uint8Array} outputBytes - byte view of the output array
 * @param {number} outputOffset - element index of the current vector
 */
function patchExceptions(reader, numExceptions, byteSize, outputBytes, outputOffset) {
  if (!numExceptions) return
  const { view } = reader
  const positionsStart = reader.offset
  const valuesStart = positionsStart + numExceptions * 2
  const source = new Uint8Array(view.buffer, view.byteOffset + valuesStart, numExceptions * byteSize)
  for (let e = 0; e < numExceptions; e++) {
    const pos = view.getUint16(positionsStart + e * 2, true)
    const dest = (outputOffset + pos) * byteSize
    outputBytes.set(source.subarray(e * byteSize, (e + 1) * byteSize), dest)
  }
  reader.offset = valuesStart + numExceptions * byteSize
}

/**
 * Unpack LSB-first bit-packed unsigned values with bitWidth <= 32.
 * Consumes ceil(count * bitWidth / 8) bytes from the reader.
 *
 * @param {DataReader} reader
 * @param {number} count - number of values to unpack
 * @param {number} bitWidth - bits per value
 * @returns {Uint32Array}
 */
function unpackBits(reader, count, bitWidth) {
  const output = new Uint32Array(count)
  if (!bitWidth) return output
  const { view } = reader
  const start = reader.offset
  const modulus = 2 ** bitWidth
  let bitPos = 0
  for (let i = 0; i < count; i++) {
    let byteIndex = start + (bitPos >>> 3)
    const shift = bitPos & 7
    // accumulate as a double to avoid 32-bit overflow (at most 40 bits needed)
    let value = view.getUint8(byteIndex++) >>> shift
    let bits = 8 - shift
    while (bits < bitWidth) {
      value += view.getUint8(byteIndex++) * 2 ** bits
      bits += 8
    }
    output[i] = value % modulus
    bitPos += bitWidth
  }
  reader.offset = start + Math.ceil(count * bitWidth / 8)
  return output
}

/**
 * Unpack LSB-first bit-packed unsigned values with bitWidth <= 64 as BigInts.
 * Consumes ceil(count * bitWidth / 8) bytes from the reader.
 *
 * @param {DataReader} reader
 * @param {number} count - number of values to unpack
 * @param {number} bitWidth - bits per value
 * @returns {BigUint64Array}
 */
function unpackBitsBig(reader, count, bitWidth) {
  const output = new BigUint64Array(count)
  if (!bitWidth) return output
  const { view } = reader
  const start = reader.offset
  const mask = (1n << BigInt(bitWidth)) - 1n
  let buffer = 0n
  let bitsInBuffer = 0
  let byteIndex = start
  for (let i = 0; i < count; i++) {
    while (bitsInBuffer < bitWidth) {
      buffer |= BigInt(view.getUint8(byteIndex++)) << BigInt(bitsInBuffer)
      bitsInBuffer += 8
    }
    output[i] = buffer & mask
    buffer >>= BigInt(bitWidth)
    bitsInBuffer -= bitWidth
  }
  reader.offset = start + Math.ceil(count * bitWidth / 8)
  return output
}
