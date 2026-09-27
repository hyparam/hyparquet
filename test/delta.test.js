import { describe, expect, it } from 'vitest'
import { deltaBinaryUnpack } from '../src/delta.js'

/**
 * Build packed residuals and an independent modular BigInt reference result.
 * Padding is deliberately nonzero, including unused miniblock width bytes.
 *
 * @param {number} count
 * @param {number} width
 * @param {number} first
 * @param {number} min
 * @param {number} blockSize
 * @param {number} miniblocks
 * @returns {{bytes: Uint8Array, expected: Int32Array}}
 */
function fixture(count, width, first, min, blockSize, miniblocks) {
  const bytes = []
  const expected = new Int32Array(count)
  /**
   * @param {bigint} value
   */
  function varint(value) {
    while (value > 127n) {
      bytes.push(Number(value & 127n) | 128)
      value >>= 7n
    }
    bytes.push(Number(value))
  }
  /**
   * @param {number} value
   */
  function zigzag(value) {
    varint(BigInt(value) << 1n ^ BigInt(value) >> 31n)
  }
  varint(BigInt(blockSize))
  varint(BigInt(miniblocks))
  varint(BigInt(count))
  zigzag(first)
  expected[0] = first
  let value = BigInt(first)
  let index = 1
  let seed = 123456789
  const mask = (1n << BigInt(width)) - 1n
  const perMini = blockSize / miniblocks
  while (index < count) {
    zigzag(min)
    for (let m = 0; m < miniblocks; m++) {
      bytes.push(index + m * perMini < count ? width : 255)
    }
    for (let m = 0; m < miniblocks && index < count; m++) {
      let packed = 0n
      for (let j = 0; j < perMini; j++) {
        seed = Math.imul(seed, 1664525) + 1013904223 >>> 0
        const residual = j === 0 ? 0n : BigInt(seed) & mask
        if (index < count) {
          value = BigInt.asIntN(32, value + BigInt(min) + residual)
          expected[index++] = Number(value)
        }
        packed |= residual << BigInt(j * width)
      }
      for (let j = 0; j < perMini * width / 8; j++) {
        bytes.push(Number(packed & 255n))
        packed >>= 8n
      }
    }
  }
  return { bytes: Uint8Array.from(bytes), expected }
}

describe('deltaBinaryUnpack', () => {
  for (let width = 0; width <= 32; width++) {
    it(`decodes INT32 width ${width} with wrapping and padding`, () => {
      for (const count of [1, 2, 31, 32, 33, 34, 127, 128, 129, 130, 257]) {
        for (const [first, min] of [[-2147483648, -2147483648], [2147483647, 2147483647], [0, -1]]) {
          for (const [blockSize, miniblocks] of [[128, 4], [256, 4], [128, 1]]) {
            const { bytes, expected } = fixture(count, width, first, min, blockSize, miniblocks)
            // Exercise both a DataView byteOffset and a nonzero reader offset.
            const buffer = new Uint8Array(bytes.length + 5)
            buffer.set(bytes, 3)
            const reader = { view: new DataView(buffer.buffer, 2), offset: 1 }
            const output = new Int32Array(count)
            deltaBinaryUnpack(reader, count, output)
            expect(output).toEqual(expected)
            expect(reader.offset).toBe(bytes.length + 1)
          }
        }
      }
    })
  }

  it('decodes unsigned INT64 residuals at every bit width', () => {
    for (let width = 0; width <= 64; width++) {
      const bytes = new Uint8Array(10 + 4 * width).fill(255)
      bytes.set([128, 1, 4, 3, 0, 0, width, 0, 0, 0])
      // First residual is zero; the second and padding have all bits set.
      for (let bit = 0; bit < width; bit++) bytes[10 + (bit >> 3)] &= ~(1 << (bit & 7))
      const reader = { view: new DataView(bytes.buffer), offset: 0 }
      const output = new BigInt64Array(3)
      deltaBinaryUnpack(reader, 3, output)
      expect(output).toEqual(new BigInt64Array([0n, 0n, BigInt.asIntN(64, (1n << BigInt(width)) - 1n)]))
      expect(reader.offset).toBe(bytes.length)
    }
  })

  it('preserves INT64 values beyond Number precision', () => {
    // First value 2^60, min delta 1, all four miniblocks have zero width.
    const bytes = Uint8Array.from([128, 1, 4, 3, 128, 128, 128, 128, 128, 128, 128, 128, 32, 2, 0, 0, 0, 0])
    const reader = { view: new DataView(bytes.buffer), offset: 0 }
    const output = new BigInt64Array(3)
    deltaBinaryUnpack(reader, 3, output)
    expect(output).toEqual(new BigInt64Array([2n ** 60n, 2n ** 60n + 1n, 2n ** 60n + 2n]))
    expect(reader.offset).toBe(bytes.length)
  })
})
