import { describe, expect, it } from 'vitest'
import { DEFAULT_PARSERS, convert, parseDecimal, parseFloat16 } from '../src/convert.js'

/**
 * @import {ColumnDecoder, SchemaElement, SchemaTree} from '../src/types.js'
 */

describe('convert function', () => {
  const name = 'name'
  /** @type {ColumnDecoder} */
  const columnDecoder = {
    pathInSchema: [],
    type: 'BOOLEAN',
    element: { name },
    schemaPath: [],
    codec: 'UNCOMPRESSED',
    parsers: DEFAULT_PARSERS,
  }

  it('returns the same data if converted_type is undefined', () => {
    const data = [1, 2, 3]
    const element = { name }
    expect(convert(data, { ...columnDecoder, element })).toEqual(data)
  })

  it('converts byte arrays to utf8', () => {
    const data = [new TextEncoder().encode('foo'), new TextEncoder().encode('bar')]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'UTF8' }
    expect(convert(data, { ...columnDecoder, element })).toEqual(['foo', 'bar'])
  })

  it('converts byte arrays to utf8 default true', () => {
    const data = [new TextEncoder().encode('foo'), new TextEncoder().encode('bar')]
    /** @type {SchemaElement} */
    const element = { name, type: 'BYTE_ARRAY' }
    expect(convert(data, { ...columnDecoder, element })).toEqual(['foo', 'bar'])
  })

  it('preserves byte arrays utf8=false', () => {
    const data = [new TextEncoder().encode('foo'), new TextEncoder().encode('bar')]
    /** @type {SchemaElement} */
    const element = { name, type: 'BYTE_ARRAY' }
    expect(convert(data, { ...columnDecoder, element, utf8: false })).toEqual([
      new Uint8Array([102, 111, 111]), new Uint8Array([98, 97, 114]),
    ])
  })

  it('decodes geometry logical type with default parser', () => {
    const pointWkb = new Uint8Array([
      1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 128, 89, 64, 0, 0, 0, 0, 0, 0, 224,
      63,
    ])
    const data = [pointWkb]
    /** @type {SchemaElement} */
    const element = { name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOMETRY' } }
    expect(convert(data, { ...columnDecoder, element })).toEqual([
      { type: 'Point', coordinates: [102, 0.5] },
    ])
  })

  it('decodes geography logical type with default parser', () => {
    const pointWkb = new Uint8Array([
      1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 128, 89, 64, 0, 0, 0, 0, 0, 0, 224,
      63,
    ])
    const data = [pointWkb]
    /** @type {SchemaElement} */
    const element = { name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOGRAPHY' } }
    expect(convert(data, { ...columnDecoder, element })).toEqual([
      { type: 'Point', coordinates: [102, 0.5] },
    ])
  })

  it('converts numbers to DECIMAL', () => {
    const data = [100, 200]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'DECIMAL' }
    expect(convert(data, { ...columnDecoder, element })).toEqual([100, 200])
  })

  it('converts numbers to DECIMAL with scale', () => {
    const data = [100, 200]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'DECIMAL', scale: 2 }
    expect(convert(data, { ...columnDecoder, element })).toEqual([1, 2])
  })

  it('converts bigint to DECIMAL', () => {
    const data = [1000n, 2000n]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'DECIMAL' }
    expect(convert(data, { ...columnDecoder, element })).toEqual([1000, 2000])
  })

  it('converts bigint to DECIMAL with scale', () => {
    const data = [10n, 20n]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'DECIMAL', scale: 2 }
    expect(convert(data, { ...columnDecoder, element })).toEqual([0.1, 0.2])
  })

  it('converts byte arrays to DECIMAL', () => {
    const data = [new Uint8Array([0, 0, 0, 100]), new Uint8Array([0, 0, 0, 200])]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'DECIMAL', scale: 0 }
    expect(convert(data, { ...columnDecoder, element })).toEqual([100, 200])
  })

  it('converts byte array from issue #59 to DECIMAL', () => {
    const data = [new Uint8Array([18, 83, 137, 151, 156, 0])]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'DECIMAL', scale: 10, precision: 14 }
    expect(convert(data, { ...columnDecoder, element })).toEqual([2015])
  })

  it('converts epoch time to DATE', () => {
    const data = [1, 2] // days since epoch
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'DATE' }
    expect(convert(data, { ...columnDecoder, element })).toEqual([new Date(86400000), new Date(86400000 * 2)])
  })

  it('converts INT96 to DATE', () => {
    // from alltypes_plain.parquet
    const data = [45284764452596988585705472n, 45284764452597048585705472n]
    /** @type {SchemaElement} */
    const element = { name, type: 'INT96' }
    expect(convert(data, { ...columnDecoder, element })).toEqual([
      new Date('2009-03-01T00:00:00.000Z'), new Date('2009-03-01T00:01:00.000Z'),
    ])
  })

  it('converts epoch time to TIMESTAMP_MILLIS', () => {
    const data = [1716506900000n, 1716507000000n]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'TIMESTAMP_MILLIS' }
    expect(convert(data, { ...columnDecoder, element })).toEqual([
      new Date('2024-05-23T23:28:20.000Z'), new Date('2024-05-23T23:30:00.000Z'),
    ])
  })

  it('converts epoch time to TIMESTAMP_MICROS', () => {
    const data = [1716506900000000n, 1716507000000000n]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'TIMESTAMP_MICROS' }
    expect(convert(data, { ...columnDecoder, element })).toEqual([
      new Date('2024-05-23T23:28:20.000Z'), new Date('2024-05-23T23:30:00.000Z'),
    ])
  })

  it('parses strings to JSON', () => {
    const encoder = new TextEncoder()
    const data = ['{"key": true}', '{"quay": 314}']
      .map(str => encoder.encode(str))
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'JSON' }
    expect(convert(data, { ...columnDecoder, element })).toEqual([{ key: true }, { quay: 314 }])
  })

  it('converts uint64', () => {
    const data = [100n, -100n]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'UINT_64' }
    expect(convert(data, { ...columnDecoder, element })).toEqual(new BigUint64Array([100n, 18446744073709551516n]))
  })

  it('converts to float16', () => {
    const data = [new Uint8Array([0x00, 0x3c]), new Uint8Array([0x00, 0x40])]
    /** @type {SchemaElement} */
    const element = { name, logical_type: { type: 'FLOAT16' } }
    expect(convert(data, { ...columnDecoder, element })).toEqual([1, 2])
  })

  it('converts timestamp with units', () => {
    const data = [1716506900000000n, 1716507000000000n]
    /** @type {SchemaElement} */
    const element = { name, logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MICROS' } }
    expect(convert(data, { ...columnDecoder, element })).toEqual([
      new Date('2024-05-23T23:28:20.000Z'), new Date('2024-05-23T23:30:00.000Z'),
    ])
  })

  it('throws error for BSON conversion', () => {
    const data = [{}]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'BSON' }
    expect(() => convert(data, { ...columnDecoder, element }))
      .toThrow('parquet bson not supported')
  })

  it('throws error for INTERVAL conversion', () => {
    const data = [{}]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'INTERVAL' }
    expect(() => convert(data, { ...columnDecoder, element }))
      .toThrow('parquet interval not supported')
  })

  it('respects custom parsers - dateFromDays', () => {
    const data = [1, 2] // days since epoch
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'DATE' }
    /** @type {ColumnDecoder} */
    const cd = {
      ...columnDecoder,
      element,
      parsers: {
        ...columnDecoder.parsers,
        dateFromDays: days => days,
      },
    }

    expect(convert(data, cd)).toEqual([ 1, 2 ])
  })

  it('respects custom parsers - timestampFromMilliseconds', () => {
    const data = [1716506900000n, 1716507000000n]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'TIMESTAMP_MILLIS' }
    /** @type {ColumnDecoder} */
    const cd = {
      ...columnDecoder,
      element,
      parsers: {
        ...columnDecoder.parsers,
        timestampFromMilliseconds: millis => Number(millis / 100000n),
      },
    }

    expect(convert(data, cd)).toEqual([ 17165069, 17165070 ])
  })

  it('respects custom parsers - timestampFromMicroseconds', () => {
    const data = [1716506900000000n, 1716507000000000n]
    /** @type {SchemaElement} */
    const element = { name, logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MICROS' } }
    /** @type {ColumnDecoder} */
    const cd = {
      ...columnDecoder,
      element,
      parsers: {
        ...columnDecoder.parsers,
        timestampFromMicroseconds: micros => Number(micros / 100000000n),
      },
    }

    expect(convert(data, cd)).toEqual([ 17165069, 17165070 ])
  })

  it('respects custom parsers - timestampFromNanoseconds', () => {
    // from alltypes_plain.parquet
    const data = [45284764452596988585705472n, 45284764452597048585705472n]
    /** @type {SchemaElement} */
    const element = { name, type: 'INT96' }
    /** @type {ColumnDecoder} */
    const cd = {
      ...columnDecoder,
      element,
      parsers: {
        ...columnDecoder.parsers,
        timestampFromNanoseconds: nanos => Number(nanos / 100000000000n),
      },
    }

    expect(convert(data, cd)).toEqual([ 12358656, 12358656 ])
  })

  it('respects custom parsers - stringFromBytes', () => {
    const encoder = new TextEncoder()
    const data = [encoder.encode('foo'), undefined]
    /** @type {SchemaElement} */
    const element = { name, converted_type: 'UTF8' }
    /** @type {ColumnDecoder} */
    const cd = {
      ...columnDecoder,
      element,
      parsers: {
        ...columnDecoder.parsers,
        stringFromBytes: bytes => bytes && `custom-${new TextDecoder().decode(bytes)}`,
      },
    }

    expect(convert(data, cd)).toEqual(['custom-foo', undefined])
  })

  it('respects custom parsers - geometryFromBytes', () => {
    const pointWkb = new Uint8Array([
      1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 128, 89, 64, 0, 0, 0, 0, 0, 0, 224,
      63,
    ])
    const data = [pointWkb]
    /** @type {SchemaElement} */
    const element = { name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOMETRY' } }
    /** @type {ColumnDecoder} */
    const cd = {
      ...columnDecoder,
      element,
      parsers: {
        ...columnDecoder.parsers,
        geometryFromBytes: () => 'custom-geometry',
      },
    }

    expect(convert(data, cd)).toEqual(['custom-geometry'])
  })

  it('respects custom parsers - geographyFromBytes', () => {
    const pointWkb = new Uint8Array([
      1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 128, 89, 64, 0, 0, 0, 0, 0, 0, 224,
      63,
    ])
    const data = [pointWkb]
    /** @type {SchemaElement} */
    const element = { name, type: 'BYTE_ARRAY', logical_type: { type: 'GEOGRAPHY' } }
    /** @type {ColumnDecoder} */
    const cd = {
      ...columnDecoder,
      element,
      parsers: {
        ...columnDecoder.parsers,
        geographyFromBytes: () => 'custom-geojson',
      },
    }

    expect(convert(data, cd)).toEqual(['custom-geojson'])
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
      .toBeCloseTo(2 ** -14 * (1023 / 1024), 5)
  })
})

describe('parseDecimal', () => {
  it('should return 0 for an empty Uint8Array', () => {
    const result = parseDecimal(new Uint8Array())
    expect(result).toBe(0)
  })

  it('should parse a single byte', () => {
    const result = parseDecimal(new Uint8Array([42]))
    expect(result).toBe(42)
  })

  it('should parse two bytes in big-endian order', () => {
    const result = parseDecimal(new Uint8Array([1, 0]))
    expect(result).toBe(256)
  })

  it('should parse three bytes', () => {
    const result = parseDecimal(new Uint8Array([1, 2, 3]))
    expect(result).toBe(66051)
  })

  it('should parse -1 as a 32-bit number', () => {
    const result = parseDecimal(new Uint8Array([255, 255, 255, 255]))
    expect(result).toBe(-1)
  })
})
