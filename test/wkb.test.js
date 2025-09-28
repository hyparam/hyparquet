import { describe, expect, it } from 'vitest'
import { decodeWKB } from '../src/wkb.js'

/**
 * @param {Uint8Array} buffer
 * @returns {import('../src/types.d.ts').DataReader}
 */
function makeReader(buffer) {
  return {
    view: new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength),
    offset: 0,
  }
}

describe('decodeWKB', () => {
  it('decodes little-endian Point', () => {
    const buffer = new Uint8Array([
      1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 128, 89, 64, 0, 0, 0, 0, 0, 0, 224,
      63,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'Point',
      coordinates: [102, 0.5],
    })
  })

  it('decodes big-endian LineString', () => {
    const buffer = new Uint8Array([
      0, 0, 0, 0, 2, 0, 0, 0, 2, 63, 248, 0, 0, 0, 0, 0, 0, 192, 12, 0,
      0, 0, 0, 0, 0, 64, 17, 0, 0, 0, 0, 0, 0, 64, 23, 0, 0, 0, 0, 0,
      0,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'LineString',
      coordinates: [
        [1.5, -3.5],
        [4.25, 5.75],
      ],
    })
  })

  it('decodes little-endian Polygon', () => {
    const buffer = new Uint8Array([
      1, 3, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240,
      63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'Polygon',
      coordinates: [
        [
          [0, 0],
          [1, 0],
          [1, 1],
          [0, 0],
        ],
      ],
    })
  })

  it('decodes little-endian MultiLineString', () => {
    const buffer = new Uint8Array([
      1, 5, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0,
      0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0,
      0, 16, 64, 0, 0, 0, 0, 0, 0, 16, 64,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'MultiLineString',
      coordinates: [
        [
          [1, 1],
          [2, 2],
        ],
        [
          [3, 3],
          [4, 4],
        ],
      ],
    })
  })

  it('decodes mixed-endian MultiPoint', () => {
    const buffer = new Uint8Array([
      1, 4, 0, 0, 0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 1, 191, 240, 0, 0, 0,
      0, 0, 0, 63, 224, 0, 0, 0, 0, 0, 0,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'MultiPoint',
      coordinates: [
        [2, 3],
        [-1, 0.5],
      ],
    })
  })

  it('decodes nested MultiPolygon', () => {
    const buffer = new Uint8Array([
      1, 6, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0,
      0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0,
      0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'MultiPolygon',
      coordinates: [
        [
          [
            [0, 0],
            [0, 2],
            [2, 2],
            [0, 0],
          ],
        ],
      ],
    })
  })

  it('decodes GeometryCollection', () => {
    const buffer = new Uint8Array([
      1, 7, 0, 0, 0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240,
      63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 2, 0, 0, 0, 2, 64, 8, 0,
      0, 0, 0, 0, 0, 64, 16, 0, 0, 0, 0, 0, 0, 64, 20, 0, 0, 0, 0, 0, 0,
      64, 24, 0, 0, 0, 0, 0, 0,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'GeometryCollection',
      geometries: [
        { type: 'Point', coordinates: [1, 2] },
        {
          type: 'LineString',
          coordinates: [
            [3, 4],
            [5, 6],
          ],
        },
      ],
    })
  })

  it('throws when MultiPoint contains non-point geometry', () => {
    const buffer = new Uint8Array([
      1, 4, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ])

    expect(() => decodeWKB(makeReader(buffer))).toThrowError('Expected Point in MultiPoint, got 2')
  })

  it('throws when MultiPolygon contains non-polygon geometry', () => {
    const buffer = new Uint8Array([
      1, 6, 0, 0, 0, 1, 0, 0, 0, 1, 4, 0, 0, 0,
    ])

    expect(() => decodeWKB(makeReader(buffer))).toThrowError('Expected Polygon in MultiPolygon, got 4')
  })

  it('throws when MultiLineString contains non-linestring geometry', () => {
    const buffer = new Uint8Array([
      1, 5, 0, 0, 0, 1, 0, 0, 0, 1, 3, 0, 0, 0,
    ])

    expect(() => decodeWKB(makeReader(buffer))).toThrowError('Expected LineString in MultiLineString, got 3')
  })

  it('throws on unsupported geometry type', () => {
    const buffer = new Uint8Array([
      1, 99, 0, 0, 0,
    ])

    expect(() => decodeWKB(makeReader(buffer))).toThrowError('Unsupported geometry type: 99')
  })

  it('decodes EWKB Point with SRID and Z/M flags', () => {
    const buffer = new Uint8Array([
      1, 1, 0, 0, 224, 17, 15, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
      0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0,
      0, 0, 0, 16, 64,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'Point',
      coordinates: [1, 2, 3, 4],
    })
  })

  it('decodes point encoded with dimensional offsets', () => {
    const buffer = new Uint8Array([
      1, 185, 11, 0, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0,
      0, 24, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 32, 64,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'Point',
      coordinates: [5, 6, 7, 8],
    })
  })

  it('decodes point with M-only dimensional offset', () => {
    const buffer = new Uint8Array([
      1, 209, 7, 0, 0, 0, 0, 0, 0, 0, 0, 34, 64, 0, 0, 0, 0, 0,
      0, 36, 64, 0, 0, 0, 0, 0, 0, 38, 64,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'Point',
      coordinates: [9, 10, 11],
    })
  })

  it('decodes point with Z-only dimensional offset', () => {
    const buffer = new Uint8Array([
      1, 233, 3, 0, 0, 0, 0, 0, 0, 0, 0, 40, 64, 0, 0, 0, 0, 0,
      0, 42, 64, 0, 0, 0, 0, 0, 0, 44, 64,
    ])

    expect(decodeWKB(makeReader(buffer))).toEqual({
      type: 'Point',
      coordinates: [12, 13, 14],
    })
  })

  it('skips SRID payloads inside multi geometries', () => {
    const mpBuffer = new Uint8Array([
      1, 4, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 32, 231, 3, 0, 0, 0,
      0, 0, 0, 0, 0, 38, 64, 0, 0, 0, 0, 0, 0, 54, 64,
    ])

    expect(decodeWKB(makeReader(mpBuffer))).toEqual({
      type: 'MultiPoint',
      coordinates: [[11, 22]],
    })

    const mlBuffer = new Uint8Array([
      1, 5, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 32, 123, 0, 0, 0, 2,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
      0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 16, 64,
    ])

    expect(decodeWKB(makeReader(mlBuffer))).toEqual({
      type: 'MultiLineString',
      coordinates: [
        [
          [1, 2],
          [3, 4],
        ],
      ],
    })

    const mpBuffer2 = new Uint8Array([
      1, 6, 0, 0, 0, 1, 0, 0, 0, 1, 3, 0, 0, 32, 230, 16, 0, 0,
      1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0,
      240, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ])

    expect(decodeWKB(makeReader(mpBuffer2))).toEqual({
      type: 'MultiPolygon',
      coordinates: [
        [
          [
            [0, 0],
            [0, 1],
            [1, 1],
            [0, 0],
          ],
        ],
      ],
    })
  })
})
