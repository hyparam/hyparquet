/**
 * ALP (Adaptive Lossless floating-Point) encoding decoder.
 * Supports FLOAT and DOUBLE types.
 *
 * ALP encodes floating-point values by converting them to integers using
 * decimal scaling, then applying frame of reference (FOR) encoding and
 * bit-packing. Values that cannot be losslessly converted are stored as exceptions.
 */

// Precomputed powers of 10 for float (max exponent 10)
const POWERS_OF_10_FLOAT = new Float64Array([
  1, 10, 100, 1000, 10000, 100000, 1000000, 10000000,
  100000000, 1000000000, 10000000000,
])

// Precomputed powers of 10 for double (max exponent 18)
const POWERS_OF_10_DOUBLE = new Float64Array([
  1, 10, 100, 1000, 10000, 100000, 1000000, 10000000,
  100000000, 1000000000, 10000000000, 100000000000,
  1000000000000, 10000000000000, 100000000000000,
  1000000000000000, 10000000000000000, 100000000000000000,
  1000000000000000000,
])

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
 * Decode ALP encoded float data.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Float32Array}
 */
export function alpDecodeFloat(reader, count) {
  const { view } = reader

  // Read header (8 bytes)
  const version = view.getUint8(reader.offset++)
  if (version !== 1) {
    throw new Error(`ALP unsupported version: ${version}`)
  }
  const compressionMode = view.getUint8(reader.offset++)
  if (compressionMode !== 0) {
    throw new Error(`ALP unsupported compression mode: ${compressionMode}`)
  }
  const integerEncoding = view.getUint8(reader.offset++)
  if (integerEncoding !== 0) {
    throw new Error(`ALP unsupported integer encoding: ${integerEncoding}`)
  }
  const logVectorSize = view.getUint8(reader.offset++)
  const vectorSize = 1 << logVectorSize
  const numElements = view.getInt32(reader.offset, true)
  reader.offset += 4

  const numVectors = Math.ceil(numElements / vectorSize)

  // Read AlpInfo array (4 bytes per vector)
  const alpInfos = new Array(numVectors)
  for (let v = 0; v < numVectors; v++) {
    const exponent = view.getUint8(reader.offset++)
    const factor = view.getUint8(reader.offset++)
    const numExceptions = view.getUint16(reader.offset, true)
    reader.offset += 2
    alpInfos[v] = { exponent, factor, numExceptions }
  }

  // Read ForInfo array (5 bytes per vector for float)
  const forInfos = new Array(numVectors)
  for (let v = 0; v < numVectors; v++) {
    const frameOfReference = view.getInt32(reader.offset, true)
    reader.offset += 4
    const bitWidth = view.getUint8(reader.offset++)
    forInfos[v] = { frameOfReference, bitWidth }
  }

  // Decode data vectors
  const output = new Float32Array(count)
  let outputOffset = 0

  for (let v = 0; v < numVectors; v++) {
    const { exponent, factor, numExceptions } = alpInfos[v]
    const { frameOfReference, bitWidth } = forInfos[v]

    // Number of elements in this vector
    const isLastVector = v === numVectors - 1
    const elementsInVector = isLastVector
      ? numElements - (numVectors - 1) * vectorSize
      : vectorSize

    // Read bit-packed deltas
    const encoded = new Int32Array(elementsInVector)
    if (bitWidth > 0) {
      unpackBitsFloat(reader, elementsInVector, bitWidth, encoded)
    }
    // If bitWidth is 0, all deltas are 0 (encoded stays all zeros)

    // Apply FOR decoding and decimal decoding
    const multiplier = POWERS_OF_10_FLOAT[factor] / POWERS_OF_10_FLOAT[exponent]
    for (let i = 0; i < elementsInVector && outputOffset + i < count; i++) {
      const encodedValue = encoded[i] + frameOfReference
      output[outputOffset + i] = encodedValue * multiplier
    }

    // Read and patch exceptions
    if (numExceptions > 0) {
      // Read exception positions (uint16[])
      const positions = new Uint16Array(numExceptions)
      for (let e = 0; e < numExceptions; e++) {
        positions[e] = view.getUint16(reader.offset, true)
        reader.offset += 2
      }

      // Read exception values (float32[])
      for (let e = 0; e < numExceptions; e++) {
        const pos = positions[e]
        if (outputOffset + pos < count) {
          output[outputOffset + pos] = view.getFloat32(reader.offset, true)
        }
        reader.offset += 4
      }
    }

    outputOffset += elementsInVector
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

  // Read header (8 bytes)
  const version = view.getUint8(reader.offset++)
  if (version !== 1) {
    throw new Error(`ALP unsupported version: ${version}`)
  }
  const compressionMode = view.getUint8(reader.offset++)
  if (compressionMode !== 0) {
    throw new Error(`ALP unsupported compression mode: ${compressionMode}`)
  }
  const integerEncoding = view.getUint8(reader.offset++)
  if (integerEncoding !== 0) {
    throw new Error(`ALP unsupported integer encoding: ${integerEncoding}`)
  }
  const logVectorSize = view.getUint8(reader.offset++)
  const vectorSize = 1 << logVectorSize
  const numElements = view.getInt32(reader.offset, true)
  reader.offset += 4

  const numVectors = Math.ceil(numElements / vectorSize)

  // Read AlpInfo array (4 bytes per vector)
  const alpInfos = new Array(numVectors)
  for (let v = 0; v < numVectors; v++) {
    const exponent = view.getUint8(reader.offset++)
    const factor = view.getUint8(reader.offset++)
    const numExceptions = view.getUint16(reader.offset, true)
    reader.offset += 2
    alpInfos[v] = { exponent, factor, numExceptions }
  }

  // Read ForInfo array (9 bytes per vector for double)
  const forInfos = new Array(numVectors)
  for (let v = 0; v < numVectors; v++) {
    const frameOfReference = view.getBigInt64(reader.offset, true)
    reader.offset += 8
    const bitWidth = view.getUint8(reader.offset++)
    forInfos[v] = { frameOfReference, bitWidth }
  }

  // Decode data vectors
  const output = new Float64Array(count)
  let outputOffset = 0

  for (let v = 0; v < numVectors; v++) {
    const { exponent, factor, numExceptions } = alpInfos[v]
    const { frameOfReference, bitWidth } = forInfos[v]

    // Number of elements in this vector
    const isLastVector = v === numVectors - 1
    const elementsInVector = isLastVector
      ? numElements - (numVectors - 1) * vectorSize
      : vectorSize

    // Read bit-packed deltas
    const encoded = new BigInt64Array(elementsInVector)
    if (bitWidth > 0) {
      unpackBitsDouble(reader, elementsInVector, bitWidth, encoded)
    }
    // If bitWidth is 0, all deltas are 0 (encoded stays all zeros)

    // Apply FOR decoding and decimal decoding
    const multiplier = POWERS_OF_10_DOUBLE[factor] / POWERS_OF_10_DOUBLE[exponent]
    for (let i = 0; i < elementsInVector && outputOffset + i < count; i++) {
      const encodedValue = Number(encoded[i] + frameOfReference)
      output[outputOffset + i] = encodedValue * multiplier
    }

    // Read and patch exceptions
    if (numExceptions > 0) {
      // Read exception positions (uint16[])
      const positions = new Uint16Array(numExceptions)
      for (let e = 0; e < numExceptions; e++) {
        positions[e] = view.getUint16(reader.offset, true)
        reader.offset += 2
      }

      // Read exception values (float64[])
      for (let e = 0; e < numExceptions; e++) {
        const pos = positions[e]
        if (outputOffset + pos < count) {
          output[outputOffset + pos] = view.getFloat64(reader.offset, true)
        }
        reader.offset += 8
      }
    }

    outputOffset += elementsInVector
  }

  return output
}

/**
 * Unpack bit-packed values into Int32Array.
 * Reads values packed at the specified bit width.
 *
 * @param {DataReader} reader
 * @param {number} count - number of values to unpack
 * @param {number} bitWidth - bits per value
 * @param {Int32Array} output - output array
 */
function unpackBitsFloat(reader, count, bitWidth, output) {
  const { view } = reader
  const mask = (1 << bitWidth) - 1

  let buffer = 0
  let bitsInBuffer = 0

  for (let i = 0; i < count; i++) {
    // Load more bytes into buffer as needed
    while (bitsInBuffer < bitWidth) {
      buffer |= view.getUint8(reader.offset++) << bitsInBuffer
      bitsInBuffer += 8
    }

    // Extract value
    output[i] = buffer & mask
    buffer >>>= bitWidth
    bitsInBuffer -= bitWidth
  }
}

/**
 * Unpack bit-packed values into BigInt64Array.
 * Reads values packed at the specified bit width.
 *
 * @param {DataReader} reader
 * @param {number} count - number of values to unpack
 * @param {number} bitWidth - bits per value
 * @param {BigInt64Array} output - output array
 */
function unpackBitsDouble(reader, count, bitWidth, output) {
  const { view } = reader
  const mask = (1n << BigInt(bitWidth)) - 1n

  let buffer = 0n
  let bitsInBuffer = 0

  for (let i = 0; i < count; i++) {
    // Load more bytes into buffer as needed
    while (bitsInBuffer < bitWidth) {
      buffer |= BigInt(view.getUint8(reader.offset++)) << BigInt(bitsInBuffer)
      bitsInBuffer += 8
    }

    // Extract value
    output[i] = buffer & mask
    buffer >>= BigInt(bitWidth)
    bitsInBuffer -= bitWidth
  }
}
