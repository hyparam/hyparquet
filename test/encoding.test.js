import { describe, expect, it } from 'vitest'
import { readRleBitPackedHybrid } from '../src/encoding.js'

describe('readRleBitPackedHybrid', () => {
  it('reads RLE bit-packed hybrid values with explicit length', () => {
    // Example buffer: 1 RLE group followed by 1 bit-packed group
    // RLE values: true x3
    // Bit-packed values: false, false, true
    const buffer = new ArrayBuffer(4)
    const view = new DataView(buffer)
    view.setUint8(0, 0b00000110) // RLE header for 3 true values
    view.setUint8(1, 0b00000001) // RLE value (true)
    view.setUint8(2, 0b00000011) // Bit-packed header for 3 values
    view.setUint8(3, 0b00000100) // Bit-packed values (false, false, true)
    const reader = { view, offset: 0 }

    const values = new Array(6)
    readRleBitPackedHybrid(reader, 1, 3, values)
    expect(reader.offset).toBe(4)
    expect(values).toEqual([1, 1, 1, 0, 0, 1])
  })

  it('reads RLE bit-packed hybrid values with implicit length', () => {
    // Example buffer: same as previous test, but with implicit length
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setInt32(0, 3, true) // length 3 little-endian
    view.setUint8(4, 0b00000110) // RLE header for 3 true values
    view.setUint8(5, 0b00000001) // RLE value (true)
    view.setUint8(6, 0b00000011) // Bit-packed header for 3 values
    view.setUint8(7, 0b00000100) // Bit-packed values (false, false, true)
    const reader = { view, offset: 0 }

    const values = new Array(6)
    readRleBitPackedHybrid(reader, 1, 0, values)
    expect(reader.offset).toBe(8)
    expect(values).toEqual([1, 1, 1, 0, 0, 1])
  })
})
