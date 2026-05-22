import { describe, expect, it } from 'vitest'
import { xxhash64 } from '../src/xxhash.js'

describe('xxhash64', () => {
  it('hashes the empty input with seed 0', () => {
    expect(xxhash64(new Uint8Array(0))).toBe(0xef46db3751d8e999n)
  })

  it('hashes the empty input with a non-zero seed', () => {
    // seed = PRIME32_1 = 2654435761
    expect(xxhash64(new Uint8Array(0), 0x9e3779b1n)).toBe(0xac75fda2929b17efn)
  })

  it('hashes a short string (< 32 bytes)', () => {
    expect(xxhash64(new TextEncoder().encode('abc'))).toBe(0x44bc2cf5ad770999n)
  })

  it('hashes a string longer than 32 bytes (block loop + tail)', () => {
    // 43 bytes - exercises the 32-byte block loop and the trailing bytes
    expect(xxhash64(new TextEncoder().encode('The quick brown fox jumps over the lazy dog')))
      .toBe(0x0b242d361fda71bcn)
  })

  it('handles all tail-length branches without throwing', () => {
    // Lengths chosen to cover the 8-byte chunk loop, 4-byte chunk, 1-byte loop boundaries
    for (const len of [0, 1, 3, 4, 5, 7, 8, 9, 15, 16, 31, 32, 33, 63, 64, 65]) {
      const input = new Uint8Array(len)
      for (let i = 0; i < len; i++) input[i] = i & 0xff
      const h = xxhash64(input)
      expect(typeof h).toBe('bigint')
      expect(h >= 0n && h <= 0xffffffffffffffffn).toBe(true)
    }
  })

  it('is deterministic across calls', () => {
    const input = new TextEncoder().encode('hyparquet bloom filter test')
    expect(xxhash64(input)).toBe(xxhash64(input))
  })

  it('respects the byteOffset of a Uint8Array view', () => {
    const backing = new Uint8Array([0xde, 0xad, 0x61, 0x62, 0x63, 0xbe, 0xef])
    const view = new Uint8Array(backing.buffer, 2, 3)
    expect(xxhash64(view)).toBe(0x44bc2cf5ad770999n)
  })
})
