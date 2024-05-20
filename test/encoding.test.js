import { describe, expect, it } from 'vitest'
import { readRleBitPackedHybrid, widthFromMaxInt } from '../src/encoding.js'

describe('readRleBitPackedHybrid', () => {
  it('reads RLE values with explicit length', () => {
    const buffer = new ArrayBuffer(4)
    const view = new DataView(buffer)
    // RLE 3x true
    view.setUint8(0, 0b00000110)
    view.setUint8(1, 1)
    // RLE 3x 100
    view.setUint8(2, 0b00000110)
    view.setUint8(3, 100)
    const reader = { view, offset: 0 }

    const values = new Array(6)
    readRleBitPackedHybrid(reader, 1, 6, values)
    expect(reader.offset).toBe(4)
    expect(values).toEqual([1, 1, 1, 100, 100, 100])
  })

  it('reads RLE values with bitwidth=16', () => {
    const buffer = new ArrayBuffer(6)
    const view = new DataView(buffer)
    // RLE 3x 65535
    view.setUint8(3, 0b00000110)
    view.setUint16(4, 65535, true)
    const reader = { view, offset: 0 }

    const values = new Array(3)
    readRleBitPackedHybrid(reader, 16, 6, values)
    expect(reader.offset).toBe(6)
    expect(values).toEqual([65535, 65535, 65535])
  })

  it('reads RLE values with bitwidth=32', () => {
    const buffer = new ArrayBuffer(5)
    const view = new DataView(buffer)
    // RLE 3x 234000
    view.setUint8(0, 0b00000110)
    view.setUint32(1, 234000, true)
    const reader = { view, offset: 0 }

    const values = new Array(3)
    readRleBitPackedHybrid(reader, 32, 3, values)
    expect(reader.offset).toBe(5)
    expect(values).toEqual([234000, 234000, 234000])
  })

  it('throws for invalid bitwidth', () => {
    const buffer = new ArrayBuffer(1)
    const view = new DataView(buffer)
    view.setUint8(0, 0b00000110)
    const reader = { view, offset: 0 }

    const values = new Array(3)
    expect(() => readRleBitPackedHybrid(reader, 24, 3, values))
      .toThrow('parquet invalid rle width 3')
  })

  it('reads bit-packed values with implicit length', () => {
    // Bit-packed values: false, false, true
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setInt32(0, 3, true) // length 3 little-endian
    view.setUint8(4, 0b00000011) // Bit-packed header for 1-8 values
    view.setUint8(5, 0b00000100) // Bit-packed values (false, false, true)
    const reader = { view, offset: 0 }

    const values = new Array(3)
    readRleBitPackedHybrid(reader, 1, 0, values)
    expect(reader.offset).toBe(6)
    expect(values).toEqual([0, 0, 1])
  })

  it('reads multi-byte bit-packed values', () => {
    // Bit-packed 9x true
    const buffer = new ArrayBuffer(3)
    const view = new DataView(buffer)
    view.setUint8(0, 0b00000101) // Bit-packed header for 9-16 values
    view.setUint8(1, 0b11111111)
    view.setUint8(2, 0b00000001)
    const reader = { view, offset: 0 }

    const values = new Array(9)
    readRleBitPackedHybrid(reader, 1, 9, values)
    expect(reader.offset).toBe(3)
    expect(values).toEqual([1, 1, 1, 1, 1, 1, 1, 1, 1])
  })

  it('throws for invalid bit-packed offset', () => {
    const buffer = new ArrayBuffer(1)
    const view = new DataView(buffer)
    view.setUint8(0, 0b00000011) // Bit-packed header for 3 values
    const reader = { view, offset: 0 }

    const values = new Array(3)
    expect(() => readRleBitPackedHybrid(reader, 1, 3, values))
      .toThrow('parquet bitpack offset 1 out of range')
  })
})

describe('widthFromMaxInt', () => {
  it('calculates bit widths', () => {
    // Test a range of inputs and their expected outputs
    expect(widthFromMaxInt(0)).toBe(0)
    expect(widthFromMaxInt(1)).toBe(1)
    expect(widthFromMaxInt(255)).toBe(8)
    expect(widthFromMaxInt(256)).toBe(9)
    expect(widthFromMaxInt(1023)).toBe(10)
    expect(widthFromMaxInt(1048575)).toBe(20)
  })
})
