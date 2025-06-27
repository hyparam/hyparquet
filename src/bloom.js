/**
 * Read bloom filter data from file buffer
 * @import { AsyncBuffer, ByteRange, ColumnMetaData } from "./types.js";
 * @param {AsyncBuffer} file
 * @param {ColumnMetaData} columnMetadata
 * @returns {Promise<ArrayBuffer | null>}
 */
export async function readBloomFilterBytes(file, { bloom_filter_offset, bloom_filter_length }) {
  if (!bloom_filter_offset || !bloom_filter_length) {
    return null // No bloom filter for this column
  }
  const bloomFilterData = await file.slice(
    Number(bloom_filter_offset),
    Number(bloom_filter_offset) + bloom_filter_length
  )
  return bloomFilterData
}

export class ParquetBloomBlock {
  /**
   * Initialize a new bloom block or use an existing one
   * @param {Uint32Array | null} block
   */
  constructor(block) {
    if (!block) {
      this.block = new Uint32Array(8)
    } else {
      this.block = block
    }
  }

  /**
  * @param {number} x
  * @returns {Uint32Array} result
  */
  mask(x) {
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
   * @param {number} x
   * @param {number} i
   * @returns {number} 1 if bit set else 0
   */
  checkBit(x, i) {
    // Bit shift into the least significant digit and check if set
    return x >> i & 1
  }

  /**
   * @param {number} x
   * @returns {void}
   */
  blockInsert(x) {
    const masked = this.mask(x)
    for (let i = 0; i < 8; i++) {
      // Set bit in block
      this.block[i] |= 1 << masked[i]
    }
  }

  /**
   * @param {number} x
   * @returns {boolean}
  */
  blockCheck(x) {
    const masked = this.mask(x)
    for (let i = 0; i < 8; i++) {
      if (!this.checkBit(this.block[i], masked[i])) {
        return false
      }
    }
    return true
  }
}
