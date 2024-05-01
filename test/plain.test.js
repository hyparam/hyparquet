import { describe, expect, it } from 'vitest'
import { readPlain } from '../src/plain.js'

describe('readPlain', () => {

  it('reads BOOLEAN values correctly', () => {
    const view = new DataView(new ArrayBuffer(1))
    view.setUint8(0, 0b00000001) // Set the first bit to 1
    const reader = { view, offset: 0 }
    const result = readPlain(reader, 'BOOLEAN', 1, false)
    expect(result).toEqual([true])
    expect(reader.offset).toBe(1)
  })

  it('reads INT32 values correctly', () => {
    const view = new DataView(new ArrayBuffer(4))
    view.setInt32(0, 123456789, true) // little-endian
    const reader = { view, offset: 0 }
    const result = readPlain(reader, 'INT32', 1, false)
    expect(result).toEqual([123456789])
    expect(reader.offset).toBe(4)
  })

  it('reads INT64 values correctly', () => {
    const view = new DataView(new ArrayBuffer(8))
    view.setBigInt64(0, BigInt('1234567890123456789'), true)
    const reader = { view, offset: 0 }
    const result = readPlain(reader, 'INT64', 1, false)
    expect(result).toEqual([1234567890123456789n])
    expect(reader.offset).toBe(8)
  })

  it('reads INT96 values correctly', () => {
    const buffer = new ArrayBuffer(12)
    const view = new DataView(buffer)

    // Example INT96 value split into 64-bit low part and 32-bit high part
    const low = BigInt('0x0123456789ABCDEF')
    const high = 0x02345678
    view.setBigInt64(0, low, true)
    view.setInt32(8, high, true)
    const reader = { view, offset: 0 }
    const result = readPlain(reader, 'INT96', 1, false)
    const expectedValue = (BigInt(high) << BigInt(32)) | low
    expect(result).toEqual([expectedValue])
    expect(reader.offset).toBe(12)
  })

  it('reads FLOAT values correctly', () => {
    const view = new DataView(new ArrayBuffer(4))
    view.setFloat32(0, 1234.5, true) // little-endian
    const reader = { view, offset: 0 }
    const result = readPlain(reader, 'FLOAT', 1, false)
    expect(result).toEqual([1234.5])
    expect(reader.offset).toBe(4)
  })

  it('reads DOUBLE values correctly', () => {
    const view = new DataView(new ArrayBuffer(8))
    view.setFloat64(0, 12345.6789, true) // little-endian
    const reader = { view, offset: 0 }
    const result = readPlain(reader, 'DOUBLE', 1, false)
    expect(result).toEqual([12345.6789])
    expect(reader.offset).toBe(8)
  })

  it('reads BYTE_ARRAY values correctly', () => {
    const view = new DataView(new ArrayBuffer(10))
    view.setInt32(0, 3, true) // length of the first byte array
    view.setUint8(4, 1) // first byte array data
    view.setUint8(5, 2)
    view.setUint8(6, 3)
    const reader = { view, offset: 0 }
    const result = readPlain(reader, 'BYTE_ARRAY', 1, false)
    expect(result).toEqual([new Uint8Array([1, 2, 3])])
    expect(reader.offset).toBe(7)
  })

  it('reads FIXED_LEN_BYTE_ARRAY values correctly', () => {
    const fixedLength = 3
    const view = new DataView(new ArrayBuffer(fixedLength))
    view.setUint8(0, 4)
    view.setUint8(1, 5)
    view.setUint8(2, 6)
    const reader = { view, offset: 0 }
    const result = readPlain(reader, 'FIXED_LEN_BYTE_ARRAY', fixedLength, false)
    expect(result).toEqual(new Uint8Array([4, 5, 6]))
    expect(reader.offset).toBe(fixedLength)
  })

  it('throws an error for unhandled types', () => {
    const view = new DataView(new ArrayBuffer(0))
    const reader = { view, offset: 0 }
    /** @type any */
    const invalidType = 'invalidType'
    expect(() => readPlain(reader, invalidType, 1, false))
      .toThrow(`parquet unhandled type: ${invalidType}`)
  })
})
