import { describe, expect, it } from 'vitest'
import { sbbfContains, sbbfInsert } from '../src/bloom.js'
import { xxhash64 } from '../src/xxhash.js'

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
