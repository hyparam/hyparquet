import { describe, expect, it } from 'vitest'
import { alpDecode, alpDecodeDouble, alpDecodeFloat } from '../src/alp.js'

describe('alpDecodeFloat', () => {
  it('decodes simple decimal values without exceptions', () => {
    // 4 float values: 1.23, 4.56, 7.89, 0.12
    // Encoded with exponent=2, factor=0 -> multiply by 100
    // encoded = [123, 456, 789, 12]
    // FOR: min=12, deltas = [111, 444, 777, 0]
    // max_delta = 777, bitWidth = ceil(log2(778)) = 10
    const buffer = new ArrayBuffer(100)
    const view = new DataView(buffer)
    let offset = 0

    // Header (8 bytes)
    view.setUint8(offset++, 1) // version
    view.setUint8(offset++, 0) // compression_mode (ALP)
    view.setUint8(offset++, 0) // integer_encoding (FOR+BitPack)
    view.setUint8(offset++, 2) // log_vector_size (2^2 = 4 elements per vector)
    view.setInt32(offset, 4, true) // num_elements
    offset += 4

    // AlpInfo (4 bytes)
    view.setUint8(offset++, 2) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 0, true) // num_exceptions
    offset += 2

    // ForInfo (5 bytes for float)
    view.setInt32(offset, 12, true) // frame_of_reference
    offset += 4
    view.setUint8(offset++, 10) // bitWidth

    // Bit-packed deltas [111, 444, 777, 0] at 10 bits each = 40 bits = 5 bytes
    // 111 = 0b0001101111
    // 444 = 0b0110111100
    // 777 = 0b1100001001
    // 0   = 0b0000000000
    // Pack: 111 (10 bits) | 444 (10 bits) | 777 (10 bits) | 0 (10 bits)
    // Byte 0: bits 0-7 of 111 = 0b01101111 = 0x6F
    // Byte 1: bits 8-9 of 111 + bits 0-5 of 444 = 0b11110000 = 0xF0
    // Byte 2: bits 6-9 of 444 + bits 0-3 of 777 = 0b10010110 = 0x96
    // Byte 3: bits 4-9 of 777 + bits 0-1 of 0 = 0b00110000 = 0x30
    // Byte 4: bits 2-9 of 0 = 0b00000000 = 0x00
    view.setUint8(offset++, 0x6F)
    view.setUint8(offset++, 0xF0)
    view.setUint8(offset++, 0x96)
    view.setUint8(offset++, 0x30)
    view.setUint8(offset++, 0x00)

    const reader = { view, offset: 0 }
    const result = alpDecodeFloat(reader, 4)

    expect(result.length).toBe(4)
    expect(result[0]).toBeCloseTo(1.23, 5)
    expect(result[1]).toBeCloseTo(4.56, 5)
    expect(result[2]).toBeCloseTo(7.89, 5)
    expect(result[3]).toBeCloseTo(0.12, 5)
  })

  it('handles zero bitWidth (all identical values)', () => {
    // 4 float values all equal to 5.0
    // Encoded with exponent=0, factor=0 -> no scaling
    // encoded = [5, 5, 5, 5]
    // FOR: min=5, all deltas = 0
    // bitWidth = 0
    const buffer = new ArrayBuffer(50)
    const view = new DataView(buffer)
    let offset = 0

    // Header (8 bytes)
    view.setUint8(offset++, 1) // version
    view.setUint8(offset++, 0) // compression_mode
    view.setUint8(offset++, 0) // integer_encoding
    view.setUint8(offset++, 2) // log_vector_size (4)
    view.setInt32(offset, 4, true) // num_elements
    offset += 4

    // AlpInfo (4 bytes)
    view.setUint8(offset++, 0) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 0, true) // num_exceptions
    offset += 2

    // ForInfo (5 bytes)
    view.setInt32(offset, 5, true) // frame_of_reference
    offset += 4
    view.setUint8(offset++, 0) // bitWidth = 0

    // No packed data when bitWidth = 0

    const reader = { view, offset: 0 }
    const result = alpDecodeFloat(reader, 4)

    expect(result.length).toBe(4)
    expect(result[0]).toBe(5)
    expect(result[1]).toBe(5)
    expect(result[2]).toBe(5)
    expect(result[3]).toBe(5)
  })

  it('handles exceptions (NaN, Inf)', () => {
    // 4 values: 1.5, NaN, 2.5, Inf
    // Encoded with exponent=1, factor=0
    // Non-exception encoded values: 15, placeholder(15), 25, placeholder(15)
    // After FOR (min=15): deltas = [0, 0, 10, 0]
    // bitWidth = ceil(log2(11)) = 4
    // Exceptions at positions [1, 3]
    const buffer = new ArrayBuffer(100)
    const view = new DataView(buffer)
    let offset = 0

    // Header (8 bytes)
    view.setUint8(offset++, 1) // version
    view.setUint8(offset++, 0) // compression_mode
    view.setUint8(offset++, 0) // integer_encoding
    view.setUint8(offset++, 2) // log_vector_size (4)
    view.setInt32(offset, 4, true) // num_elements
    offset += 4

    // AlpInfo (4 bytes)
    view.setUint8(offset++, 1) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 2, true) // num_exceptions = 2
    offset += 2

    // ForInfo (5 bytes)
    view.setInt32(offset, 15, true) // frame_of_reference
    offset += 4
    view.setUint8(offset++, 4) // bitWidth

    // Bit-packed deltas [0, 0, 10, 0] at 4 bits each = 16 bits = 2 bytes
    // Little-endian bit packing:
    // Byte 0: val[0] (bits 0-3) | val[1] (bits 4-7) = 0b00000000 = 0x00
    // Byte 1: val[2] (bits 0-3) | val[3] (bits 4-7) = 0b00001010 = 0x0A
    view.setUint8(offset++, 0x00)
    view.setUint8(offset++, 0x0A)

    // Exception positions (uint16[])
    view.setUint16(offset, 1, true) // position 1
    offset += 2
    view.setUint16(offset, 3, true) // position 3
    offset += 2

    // Exception values (float32[])
    view.setFloat32(offset, NaN, true)
    offset += 4
    view.setFloat32(offset, Infinity, true)
    offset += 4

    const reader = { view, offset: 0 }
    const result = alpDecodeFloat(reader, 4)

    expect(result.length).toBe(4)
    expect(result[0]).toBeCloseTo(1.5, 5)
    expect(result[1]).toBeNaN()
    expect(result[2]).toBeCloseTo(2.5, 5)
    expect(result[3]).toBe(Infinity)
  })
})

describe('alpDecodeDouble', () => {
  it('decodes simple decimal values', () => {
    // 4 double values: 1.23, 4.56, 7.89, 0.12
    // Same encoding as float test but with double types
    const buffer = new ArrayBuffer(100)
    const view = new DataView(buffer)
    let offset = 0

    // Header (8 bytes)
    view.setUint8(offset++, 1) // version
    view.setUint8(offset++, 0) // compression_mode
    view.setUint8(offset++, 0) // integer_encoding
    view.setUint8(offset++, 2) // log_vector_size (4)
    view.setInt32(offset, 4, true) // num_elements
    offset += 4

    // AlpInfo (4 bytes)
    view.setUint8(offset++, 2) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 0, true) // num_exceptions
    offset += 2

    // ForInfo (9 bytes for double)
    view.setBigInt64(offset, 12n, true) // frame_of_reference
    offset += 8
    view.setUint8(offset++, 10) // bitWidth

    // Bit-packed deltas [111, 444, 777, 0] at 10 bits each = 5 bytes
    view.setUint8(offset++, 0x6F)
    view.setUint8(offset++, 0xF0)
    view.setUint8(offset++, 0x96)
    view.setUint8(offset++, 0x30)
    view.setUint8(offset++, 0x00)

    const reader = { view, offset: 0 }
    const result = alpDecodeDouble(reader, 4)

    expect(result.length).toBe(4)
    expect(result[0]).toBeCloseTo(1.23, 10)
    expect(result[1]).toBeCloseTo(4.56, 10)
    expect(result[2]).toBeCloseTo(7.89, 10)
    expect(result[3]).toBeCloseTo(0.12, 10)
  })

  it('handles exceptions for double', () => {
    // 2 values: 1.5, -Infinity
    const buffer = new ArrayBuffer(100)
    const view = new DataView(buffer)
    let offset = 0

    // Header (8 bytes)
    view.setUint8(offset++, 1) // version
    view.setUint8(offset++, 0) // compression_mode
    view.setUint8(offset++, 0) // integer_encoding
    view.setUint8(offset++, 1) // log_vector_size (2)
    view.setInt32(offset, 2, true) // num_elements
    offset += 4

    // AlpInfo (4 bytes)
    view.setUint8(offset++, 1) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 1, true) // num_exceptions = 1
    offset += 2

    // ForInfo (9 bytes)
    view.setBigInt64(offset, 15n, true) // frame_of_reference (placeholder for exception)
    offset += 8
    view.setUint8(offset++, 0) // bitWidth = 0 (all deltas are 0)

    // No bit-packed data

    // Exception positions (uint16[])
    view.setUint16(offset, 1, true) // position 1
    offset += 2

    // Exception values (float64[])
    view.setFloat64(offset, -Infinity, true)
    offset += 8

    const reader = { view, offset: 0 }
    const result = alpDecodeDouble(reader, 2)

    expect(result.length).toBe(2)
    expect(result[0]).toBeCloseTo(1.5, 10)
    expect(result[1]).toBe(-Infinity)
  })
})

describe('alpDecode', () => {
  it('dispatches to float decoder', () => {
    const buffer = new ArrayBuffer(50)
    const view = new DataView(buffer)
    let offset = 0

    // Minimal valid ALP data for 1 float value
    view.setUint8(offset++, 1) // version
    view.setUint8(offset++, 0) // compression_mode
    view.setUint8(offset++, 0) // integer_encoding
    view.setUint8(offset++, 0) // log_vector_size (1 element per vector)
    view.setInt32(offset, 1, true) // num_elements
    offset += 4

    // AlpInfo
    view.setUint8(offset++, 0) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 0, true) // num_exceptions
    offset += 2

    // ForInfo
    view.setInt32(offset, 42, true) // frame_of_reference
    offset += 4
    view.setUint8(offset++, 0) // bitWidth

    const reader = { view, offset: 0 }
    const result = alpDecode(reader, 1, 'FLOAT')

    expect(result).toBeInstanceOf(Float32Array)
    expect(result[0]).toBe(42)
  })

  it('dispatches to double decoder', () => {
    const buffer = new ArrayBuffer(50)
    const view = new DataView(buffer)
    let offset = 0

    // Minimal valid ALP data for 1 double value
    view.setUint8(offset++, 1) // version
    view.setUint8(offset++, 0) // compression_mode
    view.setUint8(offset++, 0) // integer_encoding
    view.setUint8(offset++, 0) // log_vector_size (1 element per vector)
    view.setInt32(offset, 1, true) // num_elements
    offset += 4

    // AlpInfo
    view.setUint8(offset++, 0) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 0, true) // num_exceptions
    offset += 2

    // ForInfo (9 bytes for double)
    view.setBigInt64(offset, 42n, true) // frame_of_reference
    offset += 8
    view.setUint8(offset++, 0) // bitWidth

    const reader = { view, offset: 0 }
    const result = alpDecode(reader, 1, 'DOUBLE')

    expect(result).toBeInstanceOf(Float64Array)
    expect(result[0]).toBe(42)
  })

  it('throws for unsupported type', () => {
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    const reader = { view, offset: 0 }

    expect(() => alpDecode(reader, 1, 'INT32')).toThrow('ALP encoding unsupported type: INT32')
  })
})

describe('alpDecodeFloat multiple vectors', () => {
  it('decodes multiple vectors', () => {
    // 5 float values with vector size 4 (so 2 vectors, second one partial)
    // Values: 1.0, 2.0, 3.0, 4.0, 5.0
    const buffer = new ArrayBuffer(100)
    const view = new DataView(buffer)
    let offset = 0

    // Header (8 bytes)
    view.setUint8(offset++, 1) // version
    view.setUint8(offset++, 0) // compression_mode
    view.setUint8(offset++, 0) // integer_encoding
    view.setUint8(offset++, 2) // log_vector_size (4)
    view.setInt32(offset, 5, true) // num_elements
    offset += 4

    // AlpInfo for vector 0 (4 bytes)
    view.setUint8(offset++, 0) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 0, true) // num_exceptions
    offset += 2

    // AlpInfo for vector 1 (4 bytes)
    view.setUint8(offset++, 0) // exponent
    view.setUint8(offset++, 0) // factor
    view.setUint16(offset, 0, true) // num_exceptions
    offset += 2

    // ForInfo for vector 0 (5 bytes)
    // encoded = [1, 2, 3, 4], min=1, deltas=[0, 1, 2, 3], bitWidth=2
    view.setInt32(offset, 1, true) // frame_of_reference
    offset += 4
    view.setUint8(offset++, 2) // bitWidth

    // ForInfo for vector 1 (5 bytes)
    // encoded = [5], min=5, deltas=[0], bitWidth=0
    view.setInt32(offset, 5, true) // frame_of_reference
    offset += 4
    view.setUint8(offset++, 0) // bitWidth

    // Bit-packed data for vector 0: [0, 1, 2, 3] at 2 bits = 8 bits = 1 byte
    // 0b11100100 = 0xE4 (little endian: 00, 01, 10, 11)
    view.setUint8(offset++, 0xE4)

    // No data for vector 1 (bitWidth=0)

    const reader = { view, offset: 0 }
    const result = alpDecodeFloat(reader, 5)

    expect(result.length).toBe(5)
    expect(result[0]).toBe(1)
    expect(result[1]).toBe(2)
    expect(result[2]).toBe(3)
    expect(result[3]).toBe(4)
    expect(result[4]).toBe(5)
  })
})

describe('alpDecodeFloat with factor', () => {
  it('applies factor correctly', () => {
    // Value: 12.3 with exponent=2, factor=1
    // encoded = round(12.3 * 100 / 10) = round(123) = 123
    // decoded = 123 * 10 / 100 = 12.3
    const buffer = new ArrayBuffer(50)
    const view = new DataView(buffer)
    let offset = 0

    // Header
    view.setUint8(offset++, 1)
    view.setUint8(offset++, 0)
    view.setUint8(offset++, 0)
    view.setUint8(offset++, 0) // log_vector_size (1)
    view.setInt32(offset, 1, true)
    offset += 4

    // AlpInfo
    view.setUint8(offset++, 2) // exponent
    view.setUint8(offset++, 1) // factor
    view.setUint16(offset, 0, true)
    offset += 2

    // ForInfo
    view.setInt32(offset, 123, true)
    offset += 4
    view.setUint8(offset++, 0)

    const reader = { view, offset: 0 }
    const result = alpDecodeFloat(reader, 1)

    expect(result[0]).toBeCloseTo(12.3, 5)
  })
})

describe('ALP version and mode validation', () => {
  it('throws for unsupported version', () => {
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setUint8(0, 2) // version 2 (unsupported)

    const reader = { view, offset: 0 }
    expect(() => alpDecodeFloat(reader, 1)).toThrow('ALP unsupported version: 2')
  })

  it('throws for unsupported compression mode', () => {
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setUint8(0, 1) // version 1
    view.setUint8(1, 1) // compression mode 1 (unsupported)

    const reader = { view, offset: 0 }
    expect(() => alpDecodeFloat(reader, 1)).toThrow('ALP unsupported compression mode: 1')
  })

  it('throws for unsupported integer encoding', () => {
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setUint8(0, 1) // version 1
    view.setUint8(1, 0) // compression mode 0
    view.setUint8(2, 1) // integer encoding 1 (unsupported)

    const reader = { view, offset: 0 }
    expect(() => alpDecodeFloat(reader, 1)).toThrow('ALP unsupported integer encoding: 1')
  })
})
