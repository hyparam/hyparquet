import { describe, expect, it } from 'vitest'
import { readBloomFilter, sbbfContains, sbbfInsert } from '../src/bloom.js'
import { xxhash64 } from '../src/xxhash.js'

/**
 * @import {DataReader} from '../src/types.js'
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
