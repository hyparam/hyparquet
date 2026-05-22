import { describe, expect, it } from 'vitest'
import { hashParquetValue, readBloomFilter, sbbfContains, sbbfInsert } from '../src/bloom.js'
import { xxhash64 } from '../src/xxhash.js'

/**
 * @import {DataReader, SchemaElement} from '../src/types.js'
 */

/**
 * Encode an unsigned zigzag varint (i32 values, non-negative for our tests).
 *
 * @param {number} value
 * @returns {number[]}
 */
function zigzagVarint(value) {
  let zz = value < 0 ? -value * 2 - 1 : value * 2
  const out = []
  while (zz > 0x7f) {
    out.push(zz & 0x7f | 0x80)
    zz = Math.floor(zz / 128)
  }
  out.push(zz & 0x7f)
  return out
}

/**
 * Build the BloomFilterHeader thrift bytes. Each union field id defaults to 1
 * (the only supported variant); pass a different id to simulate an unsupported
 * algorithm / hash / compression.
 *
 * @param {number} numBytes
 * @param {{ algorithm?: number, hash?: number, compression?: number }} [opts]
 * @returns {number[]}
 */
function encodeHeader(numBytes, opts = {}) {
  const a = opts.algorithm ?? 1
  const h = opts.hash ?? 1
  const c = opts.compression ?? 1
  return [
    0x15, ...zigzagVarint(numBytes), // field 1 (I32): numBytes
    0x1c, a << 4 | 12, 0x00, 0x00, // field 2 (STRUCT): algorithm union
    0x1c, h << 4 | 12, 0x00, 0x00, // field 3 (STRUCT): hash union
    0x1c, c << 4 | 12, 0x00, 0x00, // field 4 (STRUCT): compression union
    0x00, // STOP
  ]
}

/**
 * @param {Uint32Array} blocks
 * @returns {number[]}
 */
function blocksToBytes(blocks) {
  const bytes = []
  for (let i = 0; i < blocks.length; i++) {
    bytes.push(blocks[i] & 0xff, blocks[i] >>> 8 & 0xff, blocks[i] >>> 16 & 0xff, blocks[i] >>> 24 & 0xff)
  }
  return bytes
}

/**
 * @param {number[]} bytes
 * @returns {DataReader}
 */
function reader(bytes) {
  return { view: new DataView(new Uint8Array(bytes).buffer), offset: 0 }
}

describe('Split Block Bloom Filter', () => {
  it('rejects every hash in an empty filter', () => {
    const blocks = new Uint32Array(8 * 4) // 4 blocks
    for (const h of [0n, 1n, 0xdeadbeefn, 0xffffffffffffffffn]) {
      expect(sbbfContains(blocks, h)).toBe(false)
    }
  })

  it('insert with hash=0 sets the low bit of every word in block 0', () => {
    const blocks = new Uint32Array(8 * 2)
    sbbfInsert(blocks, 0n)
    expect(Array.from(blocks.subarray(0, 8))).toEqual([1, 1, 1, 1, 1, 1, 1, 1])
    expect(Array.from(blocks.subarray(8, 16))).toEqual([0, 0, 0, 0, 0, 0, 0, 0])
    expect(sbbfContains(blocks, 0n)).toBe(true)
  })

  it('routes hashes by the high 32 bits', () => {
    const blocks = new Uint32Array(8 * 2)
    // high 32 bits = 0x80000000 → block index = (0x80000000 * 2) >> 32 = 1
    sbbfInsert(blocks, 0x80000000_00000000n)
    expect(Array.from(blocks.subarray(0, 8))).toEqual([0, 0, 0, 0, 0, 0, 0, 0])
    // every word in block 1 should be nonzero (one bit set per word)
    for (let i = 8; i < 16; i++) expect(blocks[i]).not.toBe(0)
  })

  it('has no false negatives across many random inserts', () => {
    // 64 blocks (2 KiB) is plenty for 500 items
    const blocks = new Uint32Array(8 * 64)
    /** @type {bigint[]} */
    const hashes = []
    for (let i = 0; i < 500; i++) {
      const bytes = new Uint8Array(8)
      new DataView(bytes.buffer).setBigUint64(0, BigInt(i) * 2654435761n, true)
      hashes.push(xxhash64(bytes))
    }
    for (const h of hashes) sbbfInsert(blocks, h)
    for (const h of hashes) expect(sbbfContains(blocks, h)).toBe(true)
  })

  it('rejects most absent values when sized generously', () => {
    // ~16 bits per inserted element → expected false-positive rate well under 1%
    const blocks = new Uint32Array(8 * 64)
    const present = new Set()
    for (let i = 0; i < 100; i++) {
      const h = xxhash64(new TextEncoder().encode(`present-${i}`))
      sbbfInsert(blocks, h)
      present.add(h)
    }
    let falsePositives = 0
    const trials = 10000
    for (let i = 0; i < trials; i++) {
      const h = xxhash64(new TextEncoder().encode(`absent-${i}`))
      if (present.has(h)) continue
      if (sbbfContains(blocks, h)) falsePositives++
    }
    // Generous bound: expected FPR ≈ (1 - e^(-100/64))^8 ≈ 0.0008, allow 3%
    expect(falsePositives / trials).toBeLessThan(0.03)
  })

  it('is deterministic and order-independent', () => {
    const hashes = [42n, 0xcafebaben, 0x1234_5678_9abc_def0n, 1n << 50n]
    const a = new Uint32Array(8 * 8)
    const b = new Uint32Array(8 * 8)
    for (const h of hashes) sbbfInsert(a, h)
    for (const h of [...hashes].reverse()) sbbfInsert(b, h)
    expect(Array.from(a)).toEqual(Array.from(b))
  })
})

describe('readBloomFilter', () => {
  it('parses a single-block filter and round-trips inserted hashes', () => {
    const blocks = new Uint32Array(8) // 1 block = 32 bytes
    for (const v of ['alice', 'bob', 'carol']) {
      sbbfInsert(blocks, xxhash64(new TextEncoder().encode(v)))
    }
    const bytes = [...encodeHeader(32), ...blocksToBytes(blocks)]
    const parsed = readBloomFilter(reader(bytes))
    expect(parsed).toBeDefined()
    expect(parsed?.numBytes).toBe(32)
    expect(Array.from(parsed?.blocks ?? [])).toEqual(Array.from(blocks))
    for (const v of ['alice', 'bob', 'carol']) {
      expect(sbbfContains(parsed.blocks, xxhash64(new TextEncoder().encode(v)))).toBe(true)
    }
  })

  it('parses a multi-block filter', () => {
    const numBlocks = 8
    const blocks = new Uint32Array(8 * numBlocks)
    for (let i = 0; i < 50; i++) sbbfInsert(blocks, xxhash64(new TextEncoder().encode(`item-${i}`)))
    const bytes = [...encodeHeader(32 * numBlocks), ...blocksToBytes(blocks)]
    const parsed = readBloomFilter(reader(bytes))
    expect(parsed?.numBytes).toBe(32 * numBlocks)
    expect(parsed?.blocks.length).toBe(8 * numBlocks)
    expect(Array.from(parsed?.blocks ?? [])).toEqual(Array.from(blocks))
  })

  it('advances the reader offset past header and body', () => {
    const bytes = [...encodeHeader(32), ...new Array(32).fill(0), 0xaa, 0xbb]
    const r = reader(bytes)
    readBloomFilter(r)
    // 2 trailing sentinel bytes should remain
    expect(r.view.byteLength - r.offset).toBe(2)
    expect(r.view.getUint8(r.offset)).toBe(0xaa)
  })

  it('returns undefined for unsupported algorithm', () => {
    // field id 2 of the algorithm union is not BLOCK
    const bytes = [...encodeHeader(32, { algorithm: 2 }), ...new Array(32).fill(0)]
    expect(readBloomFilter(reader(bytes))).toBeUndefined()
  })

  it('returns undefined for unsupported hash', () => {
    const bytes = [...encodeHeader(32, { hash: 2 }), ...new Array(32).fill(0)]
    expect(readBloomFilter(reader(bytes))).toBeUndefined()
  })

  it('returns undefined for unsupported compression (e.g. SNAPPY)', () => {
    const bytes = [...encodeHeader(32, { compression: 2 }), ...new Array(32).fill(0)]
    expect(readBloomFilter(reader(bytes))).toBeUndefined()
  })

  it('returns undefined for invalid numBytes', () => {
    // 0 bytes
    expect(readBloomFilter(reader(encodeHeader(0)))).toBeUndefined()
    // not a multiple of 32
    expect(readBloomFilter(reader([...encodeHeader(16), ...new Array(16).fill(0)]))).toBeUndefined()
  })

  it('throws when the body is truncated', () => {
    // header says 64 bytes but only 16 bytes of body follow
    const bytes = [...encodeHeader(64), ...new Array(16).fill(0)]
    expect(() => readBloomFilter(reader(bytes))).toThrow(/truncated/)
  })
})

/** @type {SchemaElement} */
const boolCol = { name: 'b', type: 'BOOLEAN' }
/** @type {SchemaElement} */
const int32Col = { name: 'i', type: 'INT32' }
/** @type {SchemaElement} */
const int64Col = { name: 'l', type: 'INT64' }
/** @type {SchemaElement} */
const floatCol = { name: 'f', type: 'FLOAT' }
/** @type {SchemaElement} */
const doubleCol = { name: 'd', type: 'DOUBLE' }
/** @type {SchemaElement} */
const utf8Col = { name: 's', type: 'BYTE_ARRAY', converted_type: 'UTF8' }
/** @type {SchemaElement} */
const binaryCol = { name: 'bin', type: 'BYTE_ARRAY' }
/** @type {SchemaElement} */
const flbaCol = { name: 'flba', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 4 }

describe('hashParquetValue', () => {
  it('hashes BOOLEAN values as a single byte', () => {
    expect(hashParquetValue(true, boolCol)).toBe(xxhash64(new Uint8Array([1])))
    expect(hashParquetValue(false, boolCol)).toBe(xxhash64(new Uint8Array([0])))
  })

  it('hashes INT32 as 4-byte little-endian', () => {
    const buf = new ArrayBuffer(4)
    new DataView(buf).setInt32(0, -123456, true)
    expect(hashParquetValue(-123456, int32Col)).toBe(xxhash64(new Uint8Array(buf)))
  })

  it('hashes UINT_32 max-range integers correctly', () => {
    // Hash of 0xFFFFFFFF should match whether the user writes -1 or 4294967295
    const a = hashParquetValue(-1, int32Col)
    const b = hashParquetValue(4294967295, { name: 'u', type: 'INT32', converted_type: 'UINT_32' })
    expect(a).toBeDefined()
    expect(a).toBe(b)
  })

  it('hashes INT64 from bigint and from safe-integer numbers', () => {
    const buf = new ArrayBuffer(8)
    new DataView(buf).setBigInt64(0, 123n, true)
    const expected = xxhash64(new Uint8Array(buf))
    expect(hashParquetValue(123n, int64Col)).toBe(expected)
    expect(hashParquetValue(123, int64Col)).toBe(expected)
  })

  it('hashes negative INT64 (two\'s-complement bytes match unsigned UINT_64)', () => {
    expect(hashParquetValue(-1n, int64Col))
      .toBe(hashParquetValue(0xffffffffffffffffn, { name: 'u', type: 'INT64', converted_type: 'UINT_64' }))
  })

  it('hashes FLOAT and DOUBLE via IEEE 754 little-endian', () => {
    const f = new ArrayBuffer(4)
    new DataView(f).setFloat32(0, 1.5, true)
    expect(hashParquetValue(1.5, floatCol)).toBe(xxhash64(new Uint8Array(f)))

    const d = new ArrayBuffer(8)
    new DataView(d).setFloat64(0, -3.14, true)
    expect(hashParquetValue(-3.14, doubleCol)).toBe(xxhash64(new Uint8Array(d)))
  })

  it('hashes BYTE_ARRAY UTF8 strings as UTF-8 bytes', () => {
    expect(hashParquetValue('abc', utf8Col)).toBe(xxhash64(new TextEncoder().encode('abc')))
    // multi-byte string
    expect(hashParquetValue('héllo', utf8Col)).toBe(xxhash64(new TextEncoder().encode('héllo')))
  })

  it('hashes BYTE_ARRAY raw bytes via Uint8Array passthrough', () => {
    const bytes = new Uint8Array([0xde, 0xad, 0xbe, 0xef])
    expect(hashParquetValue(bytes, binaryCol)).toBe(xxhash64(bytes))
  })

  it('hashes FIXED_LEN_BYTE_ARRAY via Uint8Array passthrough', () => {
    const bytes = new Uint8Array([1, 2, 3, 4])
    expect(hashParquetValue(bytes, flbaCol)).toBe(xxhash64(bytes))
  })

  it('returns undefined for null / undefined input', () => {
    expect(hashParquetValue(null, int32Col)).toBeUndefined()
    expect(hashParquetValue(undefined, utf8Col)).toBeUndefined()
  })

  it('returns undefined when JS type does not match column type', () => {
    expect(hashParquetValue('not a bool', boolCol)).toBeUndefined()
    expect(hashParquetValue(1.5, int32Col)).toBeUndefined() // non-integer
    expect(hashParquetValue(2n ** 60n, int32Col)).toBeUndefined() // bigint into int32
    expect(hashParquetValue('123', int64Col)).toBeUndefined()
    expect(hashParquetValue(123, utf8Col)).toBeUndefined()
    expect(hashParquetValue('hi', flbaCol)).toBeUndefined() // string into FLBA
  })

  it('returns undefined for lossy / ambiguous column types', () => {
    // DATE
    expect(hashParquetValue(new Date(), { name: 'd', type: 'INT32', converted_type: 'DATE' })).toBeUndefined()
    expect(hashParquetValue(0, { name: 'd', type: 'INT32', logical_type: { type: 'DATE' } })).toBeUndefined()
    // TIMESTAMP
    expect(hashParquetValue(0n, { name: 't', type: 'INT64', converted_type: 'TIMESTAMP_MILLIS' })).toBeUndefined()
    expect(hashParquetValue(0n, { name: 't', type: 'INT64', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MICROS' } })).toBeUndefined()
    // TIME
    expect(hashParquetValue(0, { name: 't', type: 'INT32', converted_type: 'TIME_MILLIS' })).toBeUndefined()
    expect(hashParquetValue(0n, { name: 't', type: 'INT64', converted_type: 'TIME_MICROS' })).toBeUndefined()
    // DECIMAL
    expect(hashParquetValue(0, { name: 'd', type: 'INT32', converted_type: 'DECIMAL', precision: 5, scale: 2 })).toBeUndefined()
    expect(hashParquetValue(new Uint8Array(8), { name: 'd', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 8, converted_type: 'DECIMAL', precision: 18, scale: 4 })).toBeUndefined()
    // JSON / BSON
    expect(hashParquetValue({ a: 1 }, { name: 'j', type: 'BYTE_ARRAY', converted_type: 'JSON' })).toBeUndefined()
    expect(hashParquetValue(new Uint8Array(0), { name: 'b', type: 'BYTE_ARRAY', converted_type: 'BSON' })).toBeUndefined()
    // INT96
    expect(hashParquetValue(0n, { name: 'i96', type: 'INT96' })).toBeUndefined()
    // FLOAT16, UUID, GEOMETRY, GEOGRAPHY, INTERVAL
    expect(hashParquetValue(new Uint8Array(2), { name: 'f16', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 2, logical_type: { type: 'FLOAT16' } })).toBeUndefined()
    expect(hashParquetValue(new Uint8Array(16), { name: 'u', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 16, logical_type: { type: 'UUID' } })).toBeUndefined()
    expect(hashParquetValue(new Uint8Array(0), { name: 'g', type: 'BYTE_ARRAY', logical_type: { type: 'GEOMETRY' } })).toBeUndefined()
  })

  it('end-to-end: a serialized bloom filter recognises values hashed via hashParquetValue', () => {
    const present = ['alice', 'bob', 'carol']
    const blocks = new Uint32Array(8 * 4)
    for (const v of present) {
      const h = hashParquetValue(v, utf8Col)
      if (h === undefined) throw new Error('hash should be defined')
      sbbfInsert(blocks, h)
    }
    // Round-trip through the on-disk encoding
    const bytes = [...encodeHeader(blocks.byteLength), ...blocksToBytes(blocks)]
    const parsed = readBloomFilter(reader(bytes))
    if (!parsed) throw new Error('parsed should be defined')
    for (const v of present) {
      const h = hashParquetValue(v, utf8Col)
      if (h === undefined) throw new Error('hash should be defined')
      expect(sbbfContains(parsed.blocks, h)).toBe(true)
    }
  })
})
