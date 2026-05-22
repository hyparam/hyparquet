// Split Block Bloom Filter (https://github.com/apache/parquet-format/blob/master/BloomFilter.md)
// A bloom filter is a sequence of 32-byte blocks. Each block holds 8 little-endian uint32 words.
// Insertion sets one bit per word, chosen by salting the low 32 bits of an xxhash64.
// Membership requires all 8 bits to be set; misses are exact, hits are probabilistic.

import { deserializeTCompactProtocol } from './thrift.js'
import { xxhash64 } from './xxhash.js'

/**
 * @import {BloomFilter, DataReader, ParquetQueryFilter, SchemaElement} from '../src/types.js'
 */

const textEncoder = new TextEncoder()

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

/**
 * Hash a JS filter value as its parquet PLAIN-encoded bytes, suitable for a
 * bloom filter lookup. Returns undefined when the column's parser is lossy or
 * ambiguous (DATE, TIMESTAMP_*, DECIMAL, JSON, BSON, INT96, FLOAT16, UUID,
 * GEOMETRY, GEOGRAPHY, INTERVAL) or when the JS value type doesn't match the
 * column. Callers must treat undefined as "bloom filter cannot help."
 *
 * @param {any} value
 * @param {SchemaElement} element
 * @returns {bigint | undefined}
 */
export function hashParquetValue(value, element) {
  if (value === null || value === undefined) return undefined
  const { type, converted_type, logical_type } = element

  if (type === 'BOOLEAN') {
    if (typeof value !== 'boolean') return undefined
    return xxhash64(new Uint8Array([value ? 1 : 0]))
  }

  if (type === 'FLOAT') {
    if (typeof value !== 'number') return undefined
    const buf = new ArrayBuffer(4)
    new DataView(buf).setFloat32(0, value, true)
    return xxhash64(new Uint8Array(buf))
  }

  if (type === 'DOUBLE') {
    if (typeof value !== 'number') return undefined
    const buf = new ArrayBuffer(8)
    new DataView(buf).setFloat64(0, value, true)
    return xxhash64(new Uint8Array(buf))
  }

  if (type === 'INT32') {
    if (converted_type === 'DATE' || converted_type === 'DECIMAL' || converted_type === 'TIME_MILLIS') return undefined
    if (logical_type?.type === 'DATE' || logical_type?.type === 'TIME' || logical_type?.type === 'DECIMAL') return undefined
    if (typeof value !== 'number' || !Number.isInteger(value)) return undefined
    const buf = new ArrayBuffer(4)
    new DataView(buf).setInt32(0, value | 0, true)
    return xxhash64(new Uint8Array(buf))
  }

  if (type === 'INT64') {
    if (converted_type === 'TIMESTAMP_MILLIS' || converted_type === 'TIMESTAMP_MICROS') return undefined
    if (converted_type === 'TIME_MICROS' || converted_type === 'DECIMAL') return undefined
    if (logical_type?.type === 'TIMESTAMP' || logical_type?.type === 'TIME' || logical_type?.type === 'DECIMAL') return undefined
    let bigValue
    if (typeof value === 'bigint') bigValue = value
    else if (typeof value === 'number' && Number.isSafeInteger(value)) bigValue = BigInt(value)
    else return undefined
    const buf = new ArrayBuffer(8)
    new DataView(buf).setBigUint64(0, BigInt.asUintN(64, bigValue), true)
    return xxhash64(new Uint8Array(buf))
  }

  if (type === 'BYTE_ARRAY') {
    if (converted_type === 'JSON' || converted_type === 'BSON' || converted_type === 'DECIMAL') return undefined
    if (logical_type?.type === 'JSON' || logical_type?.type === 'BSON' || logical_type?.type === 'VARIANT') return undefined
    if (logical_type?.type === 'GEOMETRY' || logical_type?.type === 'GEOGRAPHY') return undefined
    if (typeof value === 'string') return xxhash64(textEncoder.encode(value))
    if (value instanceof Uint8Array) return xxhash64(value)
    return undefined
  }

  if (type === 'FIXED_LEN_BYTE_ARRAY') {
    if (converted_type === 'DECIMAL' || converted_type === 'INTERVAL') return undefined
    if (logical_type?.type === 'DECIMAL' || logical_type?.type === 'UUID' || logical_type?.type === 'FLOAT16') return undefined
    if (logical_type?.type === 'GEOMETRY' || logical_type?.type === 'GEOGRAPHY') return undefined
    if (value instanceof Uint8Array) return xxhash64(value)
    return undefined
  }

  // INT96 deprecated, or type missing on group columns
  return undefined
}

/**
 * Top-level column names that appear in $eq or $in predicates within a filter.
 * These are the only columns where a bloom filter can prove a value's absence
 * and let us skip a row group; any other operator can't be helped by a bloom.
 *
 * @param {ParquetQueryFilter | undefined} filter
 * @returns {Set<string>}
 */
export function bloomEligibleColumns(filter) {
  /** @type {Set<string>} */
  const out = new Set()
  walkBloomEligible(filter, out)
  return out
}

/**
 * @param {ParquetQueryFilter | undefined} filter
 * @param {Set<string>} out
 */
function walkBloomEligible(filter, out) {
  if (!filter) return
  if ('$and' in filter && Array.isArray(filter.$and)) {
    for (const sub of filter.$and) walkBloomEligible(sub, out)
    return
  }
  if ('$or' in filter && Array.isArray(filter.$or)) {
    for (const sub of filter.$or) walkBloomEligible(sub, out)
    return
  }
  // $nor would need to prove presence, not absence — bloom can't help.
  if ('$nor' in filter) return
  for (const [field, condition] of Object.entries(filter)) {
    if (field.startsWith('$')) continue
    if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
      if ('$eq' in condition || '$in' in condition) out.add(field)
    } else {
      // primitive / null / array condition is an implicit $eq
      out.add(field)
    }
  }
}
