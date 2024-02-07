import { describe, expect, it } from 'vitest'
import { snappyUncompress } from '../src/snappy.js'

describe('snappy uncompress', () => {
  it('decompresses valid input correctly', async () => {
    const testCases = [
      { compressed: new Uint8Array([0x00]), expected: '' },
      { compressed: new Uint8Array([0x01, 0x00, 0x68]), expected: 'h' },
      { compressed: new Uint8Array([0x02, 0x04, 0x68, 0x79]), expected: 'hy' },
      { compressed: new Uint8Array([0x03, 0x08, 0x68, 0x79, 0x70]), expected: 'hyp' },
      { compressed: new Uint8Array([0x05, 0x10, 0x68, 0x79, 0x70, 0x65, 0x72]), expected: 'hyper' },
      { compressed: new Uint8Array([0x0a, 0x24, 0x68, 0x79, 0x70, 0x65, 0x72, 0x70, 0x61, 0x72, 0x61, 0x6d]), expected: 'hyperparam' },
      { compressed: new Uint8Array([0x15, 0x08, 0x68, 0x79, 0x70, 0x46, 0x03, 0x00]), expected: 'hyphyphyphyphyphyphyp' },
      {
        // from rowgroups.parquet
        compressed: new Uint8Array([
          80, 4, 1, 0, 9, 1, 0, 2, 9, 7, 4, 0, 3, 13, 8, 0, 4, 13, 8, 0, 5, 13,
          8, 0, 6, 13, 8, 0, 7, 13, 8, 0, 8, 13, 8, 60, 9, 0, 0, 0, 0, 0, 0, 0,
          10, 0, 0, 0, 0, 0, 0, 0,
        ]),
        expected: new Uint8Array([
          1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
          0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0,
          0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0,
          0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0,
        ]),
      },
    ]

    const futures = testCases.map(async ({ compressed, expected }) => {
      const outputArray = new Uint8Array(expected.length)
      await snappyUncompress(compressed, outputArray)
      if (typeof expected === 'string') {
        const outputStr = new TextDecoder().decode(outputArray)
        expect(outputStr).toBe(expected)
      } else {
        expect(outputArray).toEqual(expected) // Uint8Array
      }
    })

    await Promise.all(futures)
  })

  it('throws for invalid input', () => {
    const outputArray = new Uint8Array(10)
    expect(() => snappyUncompress(new Uint8Array([]), outputArray))
      .toThrow('invalid snappy length header')
    expect(() => snappyUncompress(new Uint8Array([0xff]), outputArray))
      .toThrow('invalid snappy length header')
    expect(() => snappyUncompress(new Uint8Array([0x03, 0x61]), outputArray))
      .toThrow('missing eof marker')
    expect(() => snappyUncompress(new Uint8Array([0x03, 0xf1]), outputArray))
      .toThrow('missing eof marker')
    // expect(() => snappyUncompress(new Uint8Array([0x02, 0x00, 0x68]), outputArray))
    //   .toThrow('premature end of input')
  })
})
