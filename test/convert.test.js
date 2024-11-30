import { describe, expect, it } from 'vitest'
import { convert, parseFloat16 } from '../src/convert.js'

/**
 * @import {SchemaElement} from '../src/types.js'
 */

describe('convert function', () => {
  const name = 'name'
  it('returns the same data if converted_type is undefined', () => {
    const data = [1, 2, 3]
    const schemaElement = { name }
    expect(convert(data, schemaElement)).toEqual(data)
  })

  it('converts byte arrays to utf8', () => {
    const data = [new TextEncoder().encode('foo'), new TextEncoder().encode('bar')]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'UTF8' }
    expect(convert(data, schemaElement)).toEqual(['foo', 'bar'])
  })

  it('converts byte arrays to utf8 default true', () => {
    const data = [new TextEncoder().encode('foo'), new TextEncoder().encode('bar')]
    /** @type {SchemaElement} */
    const schemaElement = { name, type: 'BYTE_ARRAY' }
    expect(convert(data, schemaElement)).toEqual(['foo', 'bar'])
  })

  it('preserves byte arrays utf8=false', () => {
    const data = [new TextEncoder().encode('foo'), new TextEncoder().encode('bar')]
    /** @type {SchemaElement} */
    const schemaElement = { name, type: 'BYTE_ARRAY' }
    expect(convert(data, schemaElement, false)).toEqual([
      new Uint8Array([102, 111, 111]), new Uint8Array([98, 97, 114]),
    ])
  })

  it('converts numbers to DECIMAL', () => {
    const data = [100, 200]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'DECIMAL' }
    expect(convert(data, schemaElement)).toEqual([100, 200])
  })

  it('converts numbers to DECIMAL with scale', () => {
    const data = [100, 200]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'DECIMAL', scale: 2 }
    expect(convert(data, schemaElement)).toEqual([1, 2])
  })

  it('converts bigint to DECIMAL', () => {
    const data = [BigInt(1000), BigInt(2000)]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'DECIMAL' }
    expect(convert(data, schemaElement)).toEqual([1000, 2000])
  })

  it('converts bigint to DECIMAL with scale', () => {
    const data = [BigInt(10), BigInt(20)]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'DECIMAL', scale: 2 }
    expect(convert(data, schemaElement)).toEqual([0.1, 0.2])
  })

  it('converts byte arrays to DECIMAL', () => {
    const data = [new Uint8Array([0, 0, 0, 100]), new Uint8Array([0, 0, 0, 200])]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'DECIMAL', scale: 0 }
    expect(convert(data, schemaElement)).toEqual([100, 200])
  })

  it('converts epoch time to DATE', () => {
    const data = [1, 2] // days since epoch
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'DATE' }
    expect(convert(data, schemaElement)).toEqual([new Date(86400000), new Date(86400000 * 2)])
  })

  it('converts INT96 to DATE', () => {
    // from alltypes_plain.parquet
    const data = [45284764452596988585705472n, 45284764452597048585705472n]
    /** @type {SchemaElement} */
    const schemaElement = { name, type: 'INT96' }
    expect(convert(data, schemaElement)).toEqual([new Date('2009-03-01T00:00:00.000Z'), new Date('2009-03-01T00:01:00.000Z')])
  })

  it('converts epoch time to TIMESTAMP_MILLIS', () => {
    const data = [1716506900000n, 1716507000000n]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'TIMESTAMP_MILLIS' }
    expect(convert(data, schemaElement)).toEqual([
      new Date('2024-05-23T23:28:20.000Z'), new Date('2024-05-23T23:30:00.000Z'),
    ])
  })

  it('converts epoch time to TIMESTAMP_MICROS', () => {
    const data = [1716506900000000n, 1716507000000000n]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'TIMESTAMP_MICROS' }
    expect(convert(data, schemaElement)).toEqual([
      new Date('2024-05-23T23:28:20.000Z'), new Date('2024-05-23T23:30:00.000Z'),
    ])
  })

  it('parses strings to JSON', () => {
    const encoder = new TextEncoder()
    const data = ['{"key": true}', '{"quay": 314}']
      .map(str => encoder.encode(str))
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'JSON' }
    expect(convert(data, schemaElement)).toEqual([{ key: true }, { quay: 314 }])
  })

  it('converts uint64', () => {
    const data = [BigInt(100), BigInt(-100)]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'UINT_64' }
    expect(convert(data, schemaElement)).toEqual(new BigUint64Array([100n, 18446744073709551516n]))
  })

  it('converts to float16', () => {
    const data = [new Uint8Array([0x00, 0x3c]), new Uint8Array([0x00, 0x40])]
    /** @type {SchemaElement} */
    const schemaElement = { name, logical_type: { type: 'FLOAT16' } }
    expect(convert(data, schemaElement)).toEqual([1, 2])
  })

  it('converts timestamp with units', () => {
    const data = [1716506900000000n, 1716507000000000n]
    /** @type {SchemaElement} */
    const schemaElement = { name, logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MICROS' } }
    expect(convert(data, schemaElement)).toEqual([
      new Date('2024-05-23T23:28:20.000Z'), new Date('2024-05-23T23:30:00.000Z'),
    ])
  })

  it('throws error for BSON conversion', () => {
    const data = [{}]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'BSON' }
    expect(() => convert(data, schemaElement))
      .toThrow('parquet bson not supported')
  })

  it('throws error for INTERVAL conversion', () => {
    const data = [{}]
    /** @type {SchemaElement} */
    const schemaElement = { name, converted_type: 'INTERVAL' }
    expect(() => convert(data, schemaElement))
      .toThrow('parquet interval not supported')
  })
})

describe('parseFloat16', () => {
  it('convert float16 numbers', () => {
    expect(parseFloat16(undefined)).toBe(undefined)
    expect(parseFloat16(new Uint8Array([0x00, 0xbc]))).toBe(-1)
    expect(parseFloat16(new Uint8Array([0x00, 0x00]))).toBe(0)
    expect(parseFloat16(new Uint8Array([0x00, 0x38]))).toBe(0.5)
    expect(parseFloat16(new Uint8Array([0x00, 0x3c]))).toBe(1)
    expect(parseFloat16(new Uint8Array([0x00, 0x40]))).toBe(2)
  })

  it('convert float16 -0', () => {
    expect(parseFloat16(new Uint8Array([0x00, 0x80]))).toBe(-0)
    expect(parseFloat16(new Uint8Array([0x00, 0x80]))).not.toBe(0)
  })

  it('convert float16 Infinity', () => {
    expect(parseFloat16(new Uint8Array([0x00, 0x7c]))).toBe(Infinity)
    expect(parseFloat16(new Uint8Array([0x00, 0xfc]))).toBe(-Infinity)
  })

  it('convert float16 NaN', () => {
    expect(parseFloat16(new Uint8Array([0x00, 0x7e]))).toBeNaN()
    expect(parseFloat16(new Uint8Array([0x01, 0x7e]))).toBeNaN()
  })

  it('convert float16 subnormal number', () => {
    expect(parseFloat16(new Uint8Array([0xff, 0x03])))
      .toBeCloseTo(Math.pow(2, -14) * (1023 / 1024), 5)
  })
})
