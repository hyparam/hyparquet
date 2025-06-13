import { describe, expect, it } from 'vitest'
import { bitWidth, readRleBitPackedHybrid } from '../src/encoding.js'

describe('readRle', () => {
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
    readRleBitPackedHybrid(reader, 1, 4, values)
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

  it('reads RLE values with bitwidth=24', () => {
    const buffer = new ArrayBuffer(4)
    const view = new DataView(buffer)
    // RLE 2x 16777215
    view.setUint8(0, 0b00000100)
    view.setUint8(1, 255)
    view.setUint8(2, 255)
    view.setUint8(3, 255)
    const reader = { view, offset: 0 }

    const values = new Array(2)
    readRleBitPackedHybrid(reader, 24, 4, values)
    expect(reader.offset).toBe(4)
    expect(values).toEqual([16777215, 16777215])
  })

  it('reads RLE values with bitwidth=32', () => {
    const buffer = new ArrayBuffer(5)
    const view = new DataView(buffer)
    // RLE 3x 234000
    view.setUint8(0, 0b00000110)
    view.setUint32(1, 234000, true)
    const reader = { view, offset: 0 }

    const values = new Array(3)
    readRleBitPackedHybrid(reader, 32, 5, values)
    expect(reader.offset).toBe(5)
    expect(values).toEqual([234000, 234000, 234000])
  })
})

describe('readBitPacked', () => {
  it('reads bit-packed values with implicit length', () => {
    // Bit-packed values: false, false, true
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setInt32(0, 2, true) // length 2 little-endian
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
    readRleBitPackedHybrid(reader, 1, 3, values)
    expect(reader.offset).toBe(3)
    expect(values).toEqual([1, 1, 1, 1, 1, 1, 1, 1, 1])
  })

  it('handles bitpack unsigned shifting', () => {
    // Bit-packed [131071, 0, ..., 0, 131071, 0, ...]
    // Tests for issue #13 where leftmost bit is set to 1 and shifted
    const buffer = new ArrayBuffer(154)
    const view = new DataView(buffer)
    view.setUint8(0, 0b00010011) // Bit-packed header for 72 values
    view.setUint8(1, 0b11111111)
    view.setUint8(2, 0b11111111)
    view.setUint8(3, 0b00000001)
    view.setUint8(139, 0b11111110)
    view.setUint8(140, 0b11111111)
    view.setUint8(141, 0b0000011)
    const reader = { view, offset: 0 }

    const values = new Array(72)
    readRleBitPackedHybrid(reader, 17, 154, values)
    expect(reader.offset).toBe(154)
    expect(values).toEqual([
      131071, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0,
      0, 131071, 0, 0, 0, 0, 0, 0,
    ])
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

describe('bitWidth', () => {
  it('calculates bit widths', () => {
    expect(bitWidth(0)).toBe(0)
    expect(bitWidth(1)).toBe(1)
    expect(bitWidth(7)).toBe(3)
    expect(bitWidth(8)).toBe(4)
    expect(bitWidth(255)).toBe(8)
    expect(bitWidth(256)).toBe(9)
    expect(bitWidth(1023)).toBe(10)
    expect(bitWidth(1048575)).toBe(20)
  })
})
