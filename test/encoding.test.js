import { describe, expect, it } from 'vitest'
import { ParquetType } from '../src/constants.js'
import { readPlain, readRleBitPackedHybrid } from '../src/encoding.js'

describe('readPlain', () => {

  it('reads BOOLEAN values correctly', () => {
    const dataView = new DataView(new ArrayBuffer(1))
    dataView.setUint8(0, 0b00000001) // Set the first bit to 1
    const result = readPlain(dataView, ParquetType.BOOLEAN, 1, 0)
    expect(result).toEqual({ value: [true], byteLength: 1 })
  })

  it('reads INT32 values correctly', () => {
    const dataView = new DataView(new ArrayBuffer(4))
    dataView.setInt32(0, 123456789, true) // little-endian
    const result = readPlain(dataView, ParquetType.INT32, 1, 0)
    expect(result).toEqual({ value: [123456789], byteLength: 4 })
  })

  it('reads INT64 values correctly', () => {
    const dataView = new DataView(new ArrayBuffer(8))
    dataView.setBigInt64(0, BigInt('1234567890123456789'), true)
    const result = readPlain(dataView, ParquetType.INT64, 1, 0)
    expect(result).toEqual({ value: [1234567890123456789n], byteLength: 8 })
  })

  it('reads INT96 values correctly', () => {
    const buffer = new ArrayBuffer(12)
    const dataView = new DataView(buffer)

    // Example INT96 value split into 64-bit low part and 32-bit high part
    const low = BigInt('0x0123456789ABCDEF')
    const high = 0x02345678
    dataView.setBigInt64(0, low, true)
    dataView.setInt32(8, high, true)
    const expectedValue = (BigInt(high) << BigInt(32)) | low

    const result = readPlain(dataView, ParquetType.INT96, 1, 0)
    expect(result).toEqual({
      value: [expectedValue],
      byteLength: 12,
    })
  })

  it('reads FLOAT values correctly', () => {
    const dataView = new DataView(new ArrayBuffer(4))
    dataView.setFloat32(0, 1234.5, true) // little-endian
    const result = readPlain(dataView, ParquetType.FLOAT, 1, 0)
    expect(result).toEqual({ value: [1234.5], byteLength: 4 })
  })

  it('reads DOUBLE values correctly', () => {
    const dataView = new DataView(new ArrayBuffer(8))
    dataView.setFloat64(0, 12345.6789, true) // little-endian
    const result = readPlain(dataView, ParquetType.DOUBLE, 1, 0)
    expect(result).toEqual({ value: [12345.6789], byteLength: 8 })
  })

  it('reads BYTE_ARRAY values correctly', () => {
    const dataView = new DataView(new ArrayBuffer(10))
    dataView.setInt32(0, 3, true) // length of the first byte array
    dataView.setUint8(4, 1) // first byte array data
    dataView.setUint8(5, 2)
    dataView.setUint8(6, 3)
    const result = readPlain(dataView, ParquetType.BYTE_ARRAY, 1, 0)
    expect(result).toEqual({
      value: [new Uint8Array([1, 2, 3])],
      byteLength: 7,
    })
  })

  it('reads FIXED_LEN_BYTE_ARRAY values correctly', () => {
    const fixedLength = 3
    const dataView = new DataView(new ArrayBuffer(fixedLength))
    dataView.setUint8(0, 4)
    dataView.setUint8(1, 5)
    dataView.setUint8(2, 6)
    const result = readPlain(dataView, ParquetType.FIXED_LEN_BYTE_ARRAY, fixedLength, 0)
    expect(result).toEqual({
      value: new Uint8Array([4, 5, 6]),
      byteLength: fixedLength,
    })
  })

  it('throws an error for unhandled types', () => {
    const dataView = new DataView(new ArrayBuffer(0))
    const invalidType = 999
    expect(() => readPlain(dataView, invalidType, 1, 0)).toThrow(`parquet unhandled type: ${invalidType}`)
  })
})

describe('readRleBitPackedHybrid', () => {
  it('reads RLE bit-packed hybrid values with explicit length', () => {
    // Example buffer: 1 RLE group followed by 1 bit-packed group
    // RLE values: true x3
    // Bit-packed values: false, false, true
    const buffer = new ArrayBuffer(4)
    const dataView = new DataView(buffer)
    dataView.setUint8(0, 0b00000110) // RLE header for 3 true values
    dataView.setUint8(1, 0b00000001) // RLE value (true)
    dataView.setUint8(2, 0b00000011) // Bit-packed header for 3 values
    dataView.setUint8(3, 0b00000100) // Bit-packed values (false, false, true)

    const { byteLength, value } = readRleBitPackedHybrid(dataView, 0, 1, 3, 6)
    expect(byteLength).toBe(4)
    expect(value).toEqual([1, 1, 1, 0, 0, 1])
  })

  it('reads RLE bit-packed hybrid values with implicit length', () => {
    // Example buffer: same as previous test, but with implicit length
    const buffer = new ArrayBuffer(8)
    const dataView = new DataView(buffer)
    dataView.setInt32(0, 3, true) // length 3 little-endian
    dataView.setUint8(4, 0b00000110) // RLE header for 3 true values
    dataView.setUint8(5, 0b00000001) // RLE value (true)
    dataView.setUint8(6, 0b00000011) // Bit-packed header for 3 values
    dataView.setUint8(7, 0b00000100) // Bit-packed values (false, false, true)

    const { byteLength, value } = readRleBitPackedHybrid(dataView, 0, 1, 0, 6)
    expect(byteLength).toBe(8)
    expect(value).toEqual([1, 1, 1, 0, 0, 1])
  })
})
