import { deserializeTCompactProtocol } from './thrift.js'

/**
 *  * @import { AsyncBuffer, ColumnChunk, ColumnMetaData } from "./types.js";
 */

/**
* @param {AsyncBuffer} file
* @param {ColumnChunk} columnChunk
* @returns {Promise<Uint32Array<ArrayBuffer>[] | null>}
*/
export async function getBloomFilterBitSet(file, columnChunk) {
  if (!columnChunk?.meta_data) {
    return null
  }

  const bloomBytes = await readBloomFilterBytes(file, columnChunk.meta_data)

  if (!bloomBytes) {
    return null
  }

  const view = new DataView(bloomBytes)
  const reader = { view, offset: 0 }
  const bloomFilterHeader = deserializeTCompactProtocol(reader)

  const headerLength = reader.offset // Number of bytes consumed
  const bitSetLength = bloomFilterHeader.field_1
  const numBlocks = bitSetLength / 32

  if (!Number.isInteger(numBlocks)) {
    throw new Error(`The bloom filter bit set length (${bitSetLength}) is not a multiple of 32.`)
  }

  const bloomBitSet = bloomBytes.slice(headerLength, headerLength + bitSetLength)
  const blocks = []
  for (let i = 0; i < numBlocks; i++) {
    blocks.push(getBloomFilterBitSetBlock(bloomBitSet))
  }

  return blocks
}

/**
 *
 * @param {ArrayBuffer} bloomBitSet
 * @returns {Uint32Array<ArrayBuffer>}
 */
function getBloomFilterBitSetBlock(bloomBitSet) {
  const block = new Uint32Array(8)
  const bleh = new DataView(bloomBitSet)
  for (let i = 0; i < 8; i++) {
    // TODO: is the data always endian?
    block[i] = bleh.getUint32(i * 4, true)
  }
  return block
}

/**
 * Read bloom filter data from file buffer
 * @param {AsyncBuffer} file
 * @param {ColumnMetaData} columnMetadata
 * @returns {Promise<ArrayBuffer | null>}
 */
async function readBloomFilterBytes(file, { bloom_filter_offset, bloom_filter_length }) {
  if (!bloom_filter_offset || !bloom_filter_length) {
    return null // No bloom filter for this column
  }
  const start = Number(bloom_filter_offset)
  const bloomFilterData = await file.slice(
    start,
    start + bloom_filter_length
  )
  return bloomFilterData
}

/**
 * Apply salt-based masking to a value for bloom filter operations within a single block
 * @param {number} x
 * @returns {Uint32Array} result
 */
function mask(x) {
  const parquet_bloom_salt = new Uint32Array([
    0x47b6137b, 0x44974d91, 0x8824ad5b,
    0xa2b7289d, 0x705495c7, 0x2df1424b,
    0x9efc4947, 0x5c6bfb31,
  ])

  const result = new Uint32Array(8)
  for (let i = 0; i < 8; i++) {
    // Split block hashing
    result[i] = x * parquet_bloom_salt[i] >> 27
  }
  return result
}

/**
 * Check if a specific bit is set in a number
 * @param {number} x
 * @param {number} i
 * @returns {number} 1 if bit set else 0
 */
function checkBit(x, i) {
  // Bit shift into the least significant digit and check if set
  return x >> i & 1
}

/**
 * Insert a value into the bloom filter block
 * @param {Uint32Array} block
 * x must be internally represented in memory as a uint32. In the bloom filter
 * implementation, these 32 bits represent the least significant bits from the
 * xxhash 64 operation (not yet implemented).
 * @param {number} x
 * @returns {void}
 */
export function insertIntoBlock(block, x) {
  const masked = mask(x)
  for (let i = 0; i < 8; i++) {
    // Set bit in block
    block[i] |= 1 << masked[i]
  }
}

/**
 * Check if a value might be present in the bloom filter block
 * @param {Uint32Array} block
 * @param {number} x
 * @returns {boolean}
 */
export function checkInBlock(block, x) {
  const masked = mask(x >>> 0)
  for (let i = 0; i < 8; i++) {
    if (!checkBit(block[i], masked[i])) {
      return false
    }
  }
  return true
}
