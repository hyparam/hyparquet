import { describe, expect, it } from 'vitest'
import { snappyUncompress } from '../src/snappy.js'

describe('snappy uncompress', () => {
  it('decompresses valid input correctly', () => {
    const testCases = [
      { compressed: new Uint8Array([0x02, 0x04, 0x68, 0x79]), expected: 'hy' },
      { compressed: new Uint8Array([0x03, 0x08, 0x68, 0x79, 0x70]), expected: 'hyp' },
      { compressed: new Uint8Array([0x05, 0x10, 0x68, 0x79, 0x70, 0x65, 0x72]), expected: 'hyper' },
      { compressed: new Uint8Array([0x0a, 0x24, 0x68, 0x79, 0x70, 0x65, 0x72, 0x70, 0x61, 0x72, 0x61, 0x6d]), expected: 'hyperparam' },
      { compressed: new Uint8Array([0x15, 0x08, 0x68, 0x79, 0x70, 0x46, 0x03, 0x00]), expected: 'hyphyphyphyphyphyphyp' },
    ]

    testCases.forEach(({ compressed, expected }) => {
      const outputArray = new Uint8Array(expected.length)
      const result = snappyUncompress(compressed, outputArray)
      const outputStr = new TextDecoder().decode(outputArray)
      expect(result).toBe(true)
      expect(outputStr).toBe(expected)
    })
  })

  it('returns false for invalid input', () => {
    const outputArray = new Uint8Array(10)
    expect(snappyUncompress(new Uint8Array([0x03, 0x61]), outputArray)).toBe(false)
    expect(snappyUncompress(new Uint8Array([0x03, 0xf1]), outputArray)).toBe(false)
  })
})
