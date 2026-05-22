// Split Block Bloom Filter (https://github.com/apache/parquet-format/blob/master/BloomFilter.md)
// A bloom filter is a sequence of 32-byte blocks. Each block holds 8 little-endian uint32 words.
// Insertion sets one bit per word, chosen by salting the low 32 bits of an xxhash64.
// Membership requires all 8 bits to be set; misses are exact, hits are probabilistic.

import { deserializeTCompactProtocol } from './thrift.js'

/**
 * @import {BloomFilter, DataReader} from '../src/types.js'
 */

const SALT = new Uint32Array([
  0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
  0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
])

/**
 * Map the high 32 bits of a hash to a block index in [0, numBlocks).
 *
 * @param {bigint} hash
 * @param {number} numBlocks
 * @returns {number}
 */
function blockIndex(hash, numBlocks) {
  return Number((hash >> 32n) * BigInt(numBlocks) >> 32n)
}

/**
 * Per-block mask: 8 uint32 words, each with a single bit set at position `(low32 * SALT[i]) >> 27`.
 *
 * @param {bigint} hash
 * @returns {Uint32Array}
 */
function blockMask(hash) {
  const m = new Uint32Array(8)
  const low = Number(hash & 0xffffffffn) | 0
  for (let i = 0; i < 8; i++) {
    m[i] = 1 << (Math.imul(low, SALT[i]) >>> 27)
  }
  return m
}

/**
 * Insert a hash into a Split Block Bloom Filter.
 *
 * @param {Uint32Array} blocks bloom filter words (8 * numBlocks long)
 * @param {bigint} hash 64-bit xxhash of the parquet-plain-encoded value
 */
export function sbbfInsert(blocks, hash) {
  const offset = blockIndex(hash, blocks.length >> 3) << 3
  const m = blockMask(hash)
  for (let i = 0; i < 8; i++) {
    blocks[offset + i] |= m[i]
  }
}

/**
 * Test whether a hash might be present in a Split Block Bloom Filter.
 * False positives are possible; false negatives are not.
 *
 * @param {Uint32Array} blocks bloom filter words (8 * numBlocks long)
 * @param {bigint} hash 64-bit xxhash of the parquet-plain-encoded value
 * @returns {boolean}
 */
export function sbbfContains(blocks, hash) {
  const offset = blockIndex(hash, blocks.length >> 3) << 3
  const m = blockMask(hash)
  for (let i = 0; i < 8; i++) {
    if ((blocks[offset + i] & m[i]) === 0) return false
  }
  return true
}

/**
 * Parse a Split Block Bloom Filter from a reader positioned at the BloomFilterHeader.
 * Returns undefined when the header advertises an unsupported algorithm, hash, or
 * compression — callers should treat that as "cannot use this bloom filter."
 *
 * @param {DataReader} reader
 * @returns {BloomFilter | undefined}
 */
export function readBloomFilter(reader) {
  const header = deserializeTCompactProtocol(reader)
  const numBytes = header.field_1
  if (typeof numBytes !== 'number' || numBytes <= 0 || numBytes % 32 !== 0) return undefined
  // BloomFilterAlgorithm / Hash / Compression are unions with a single supported variant each.
  if (!header.field_2?.field_1) return undefined // algorithm must be BLOCK
  if (!header.field_3?.field_1) return undefined // hash must be XXHASH
  if (!header.field_4?.field_1) return undefined // compression must be UNCOMPRESSED

  const { view, offset } = reader
  if (offset + numBytes > view.byteLength) {
    throw new Error(`parquet bloom filter truncated: need ${numBytes} bytes, have ${view.byteLength - offset}`)
  }
  // Reader offset is not 4-aligned in general, and we want endian-portable reads.
  const blocks = new Uint32Array(numBytes >> 2)
  for (let i = 0; i < blocks.length; i++) {
    blocks[i] = view.getUint32(offset + i * 4, true)
  }
  reader.offset = offset + numBytes
  return { numBytes, blocks }
}
