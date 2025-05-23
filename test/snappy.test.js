import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { snappyUncompress } from '../src/snappy.js'

describe('snappy uncompress', () => {
  it('decompresses valid input correctly', () => {
    const testCases = [
      { compressed: [0x00], expected: '' },
      { compressed: [0x01, 0x00, 0x68], expected: 'h' },
      { compressed: [0x02, 0x04, 0x68, 0x79], expected: 'hy' },
      { compressed: [0x03, 0x08, 0x68, 0x79, 0x70], expected: 'hyp' },
      { compressed: [0x05, 0x10, 0x68, 0x79, 0x70, 0x65, 0x72], expected: 'hyper' },
      {
        compressed: [0x0a, 0x24, 0x68, 0x79, 0x70, 0x65, 0x72, 0x70, 0x61, 0x72, 0x61, 0x6d],
        expected: 'hyperparam',
      },
      {
        compressed: [0x15, 0x08, 0x68, 0x79, 0x70, 0x46, 0x03, 0x00],
        expected: 'hyphyphyphyphyphyphyp',
      },
      {
        // from rowgroups.parquet
        compressed: [
          80, 4, 1, 0, 9, 1, 0, 2, 9, 7, 4, 0, 3, 13, 8, 0, 4, 13, 8, 0, 5, 13,
          8, 0, 6, 13, 8, 0, 7, 13, 8, 0, 8, 13, 8, 60, 9, 0, 0, 0, 0, 0, 0, 0,
          10, 0, 0, 0, 0, 0, 0, 0,
        ],
        expected: new Uint8Array([
          1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
          0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0,
          0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0,
          0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0,
        ]),
      },
      // from datapage_v2.snappy.parquet
      { compressed: [2, 4, 0, 3], expected: new Uint8Array([0, 3]) },
      { compressed: [ 6, 20, 2, 0, 0, 0, 3, 23], expected: new Uint8Array([2, 0, 0, 0, 3, 23]) },
    ]

    for (const { compressed, expected } of testCases) {
      const output = new Uint8Array(expected.length)
      snappyUncompress(new Uint8Array(compressed), output)
      if (typeof expected === 'string') {
        const outputStr = new TextDecoder().decode(output)
        expect(outputStr).toBe(expected)
      } else {
        expect(output).toEqual(expected) // Uint8Array
      }
    }
  })

  it('decompress hyparquet.jpg.snappy', async () => {
    const compressed = fs.readFileSync('test/files/hyparquet.jpg.snappy')
    const expected = fs.readFileSync('hyparquet.jpg')
    const output = new Uint8Array(expected.length)
    await snappyUncompress(compressed, output)
    expect(Array.from(output)).toEqual(Array.from(expected))
  })

  it('throws for invalid input', () => {
    const output = new Uint8Array(10)
    expect(() => snappyUncompress(new Uint8Array([]), output))
      .toThrow('invalid snappy length header')
    expect(() => snappyUncompress(new Uint8Array([0xff]), output))
      .toThrow('invalid snappy length header')
    expect(() => snappyUncompress(new Uint8Array([0x03, 0x61]), output))
      .toThrow('missing eof marker')
    expect(() => snappyUncompress(new Uint8Array([0x03, 0xf1]), output))
      .toThrow('missing eof marker')
    expect(() => snappyUncompress(new Uint8Array([0x02, 0x00, 0x68]), output))
      .toThrow('premature end of input')
  })
})
