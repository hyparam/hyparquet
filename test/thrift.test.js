import { describe, expect, it } from 'vitest'
import { deserializeTCompactProtocol, toVarInt } from '../src/thrift.js'

describe('deserializeTCompactProtocol function', () => {

  it('parses basic types correctly', () => {
    // Setup a buffer with thrift encoded data for basic types
    const buffer = new ArrayBuffer(128)
    const view = new DataView(buffer)
    let index = 0

    // Boolean
    view.setUint8(index++, 0x11) // Field 1 type TRUE
    view.setUint8(index++, 0x12) // Field 2 type FALSE

    // Byte
    view.setUint8(index++, 0x13) // Field 3 type BYTE
    view.setUint8(index++, 0x7f) // Max value for a signed byte

    // Int16
    view.setUint8(index++, 0x14) // Field 4 type int16
    view.setUint8(index++, 0xfe) // 0xfffe zigzag => 16-bit max value 0x7fff
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0x3)

    // Int32
    view.setUint8(index++, 0x15) // Field 5 type int32
    view.setUint8(index++, 0xfe) // 0xfffffffe zigzag => 32-bit max value 0x7fffffff
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0x0f)

    // Int64
    view.setUint8(index++, 0x16) // Field 6 type int64
    view.setUint8(index++, 0xfe)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0xff)
    view.setUint8(index++, 0x01)

    // Double
    view.setUint8(index++, 0x17) // Field 7 type DOUBLE
    view.setFloat64(index, 123.456, true)
    index += 8

    // String
    const str = 'Hello, Thrift!'
    view.setUint8(index++, 0x18) // Field 8 type STRING
    // write string length as varint
    const stringLengthVarInt = toVarInt(str.length)
    stringLengthVarInt.forEach(byte => view.setUint8(index++, byte))
    // write string bytes
    for (let i = 0; i < str.length; i++) {
      view.setUint8(index++, str.charCodeAt(i))
    }

    // Mark the end of the structure
    view.setUint8(index, 0x00) // STOP field

    const { byteLength, value } = deserializeTCompactProtocol(buffer)
    expect(byteLength).toBe(index + 1)

    // Assertions for each basic type
    expect(value.field_1).toBe(true) // TRUE
    expect(value.field_2).toBe(false) // FALSE
    expect(value.field_3).toBe(0x7f) // BYTE
    expect(value.field_4).toBe(0x7fff) // I16
    expect(value.field_5).toBe(0x7fffffff) // I32
    expect(value.field_6).toBe(BigInt('0x7fffffffffffffff')) // I64
    expect(value.field_7).toBeCloseTo(123.456) // DOUBLE
    expect(value.field_8).toBe('Hello, Thrift!') // STRING
  })

})
