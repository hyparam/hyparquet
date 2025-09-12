import { describe, expect, it } from 'vitest'
import { decodeWKB } from '../src/wkb.js'

describe('WKB decoding', () => {
  it('should decoding well-known binary Point', () => {
    const buffer = new Uint8Array([
      1, // little endian
      1, 0, 0, 0, // type Point
      0, 0, 0, 0, 0, 128, 89, 64, // 102
      0, 0, 0, 0, 0, 0, 224, 63, // 0.5
    ])
    const json = decodeWKB(buffer)
    const expected = {
      type: 'Point',
      coordinates: [102, 0.5],
    }
    expect(json).toEqual(expected)
  })

  it('should decoding well-known binary MultiLineString', () => {
    // from data-multilinestring-encoding_wkb.parquet
    const buffer = new Uint8Array([
      1, // little endian
      5, 0, 0, 0, // type MultiLineString
      2, 0, 0, 0, // num linestrings
      1,
      2, 0, 0, 0,
      3, 0, 0, 0, // 3 points
      0, 0, 0, 0, 0, 0, 36, 64, // 10
      0, 0, 0, 0, 0, 0, 36, 64, // 10
      0, 0, 0, 0, 0, 0, 52, 64, // 20
      0, 0, 0, 0, 0, 0, 52, 64, // 20
      0, 0, 0, 0, 0, 0, 36, 64, // 10
      0, 0, 0, 0, 0, 0, 68, 64, // 40
      1,
      2, 0, 0, 0,
      4, 0, 0, 0, // 4 points
      0, 0, 0, 0, 0, 0, 68, 64, // 40
      0, 0, 0, 0, 0, 0, 68, 64, // 40
      0, 0, 0, 0, 0, 0, 62, 64, // 30
      0, 0, 0, 0, 0, 0, 62, 64, // 30
      0, 0, 0, 0, 0, 0, 68, 64, // 40
      0, 0, 0, 0, 0, 0, 52, 64, // 20
      0, 0, 0, 0, 0, 0, 62, 64, // 30
      0, 0, 0, 0, 0, 0, 36, 64, // 10
    ])
    const json = decodeWKB(buffer)
    const expected = {
      type: 'MultiLineString',
      coordinates: [
        [[10, 10], [20, 20], [10, 40]],
        [[40, 40], [30, 30], [40, 20], [30, 10]],
      ],
    }
    expect(json).toEqual(expected)
  })
})
