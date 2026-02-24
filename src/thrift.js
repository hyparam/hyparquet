/**
 * @import {DataReader, ThriftObject, ThriftType} from '../src/types.d.ts'
 */

// TCompactProtocol types
const STOP = 0
const TRUE = 1
const FALSE = 2
const BYTE = 3
const I16 = 4
const I32 = 5
const I64 = 6
const DOUBLE = 7
const BINARY = 8
const LIST = 9
const STRUCT = 12

/**
 * Parse TCompactProtocol
 *
 * @param {DataReader} reader
 * @returns {{ [key: `field_${number}`]: any }}
 */
export function deserializeTCompactProtocol(reader) {
  /** @type {ThriftObject} */
  const value = {}
  let fid = 0

  while (reader.offset < reader.view.byteLength) {
    // Parse each field based on its type and add to the result object
    const byte = reader.view.getUint8(reader.offset++)
    const type = byte & 0x0f
    if (type === STOP) break
    const delta = byte >> 4
    fid = delta ? fid + delta : readZigZag(reader)
    value[`field_${fid}`] = readElement(reader, type)
  }

  return value
}

/**
 * Read a single element based on its type
 *
 * @param {DataReader} reader
 * @param {number} type
 * @returns {ThriftType}
 */
function readElement(reader, type) {
  switch (type) {
  case TRUE:
    return true
  case FALSE:
    return false
  case BYTE:
    return reader.view.getInt8(reader.offset++)
  case I16:
  case I32:
    return readZigZag(reader)
  case I64:
    return readZigZagBigInt(reader)
  case DOUBLE: {
    const value = reader.view.getFloat64(reader.offset, true)
    reader.offset += 8
    return value
  }
  case BINARY: {
    const stringLength = readVarInt(reader)
    const strBytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, stringLength)
    reader.offset += stringLength
    return strBytes
  }
  case LIST: {
    const byte = reader.view.getUint8(reader.offset++)
    const elemType = byte & 0x0f
    let listSize = byte >> 4
    if (listSize === 15) {
      listSize = readVarInt(reader)
    }
    const boolType = elemType === TRUE || elemType === FALSE
    const values = new Array(listSize)
    for (let i = 0; i < listSize; i++) {
      values[i] = boolType ? readElement(reader, BYTE) === 1 : readElement(reader, elemType)
    }
    return values
  }
  case STRUCT:
    // main function handles struct parsing
    return deserializeTCompactProtocol(reader)
  default:
    // MAP, SET, UUID not used by parquet
    throw new Error(`thrift unhandled type: ${type}`)
  }
}

/**
 * Read varint aka Unsigned LEB128.
 *
 * @param {DataReader} reader
 * @returns {number}
 */
export function readVarInt(reader) {
  let result = 0
  let shift = 0
  while (true) {
    // Read groups of 7 low bits until high bit is 0
    const byte = reader.view.getUint8(reader.offset++)
    result |= (byte & 0x7f) << shift
    if (!(byte & 0x80)) {
      return result
    }
    shift += 7
  }
}

/**
 * Read a varint as a bigint.
 *
 * @param {DataReader} reader
 * @returns {bigint}
 */
function readVarBigInt(reader) {
  let result = 0n
  let shift = 0n
  while (true) {
    const byte = reader.view.getUint8(reader.offset++)
    result |= BigInt(byte & 0x7f) << shift
    if (!(byte & 0x80)) {
      return result
    }
    shift += 7n
  }
}

/**
 * Read a zigzag number.
 * Zigzag folds positive and negative numbers into the positive number space.
 *
 * @param {DataReader} reader
 * @returns {number}
 */
export function readZigZag(reader) {
  const zigzag = readVarInt(reader)
  return zigzag >>> 1 ^ -(zigzag & 1)
}

/**
 * Read a zigzag bigint.
 *
 * @param {DataReader} reader
 * @returns {bigint}
 */
export function readZigZagBigInt(reader) {
  const zigzag = readVarBigInt(reader)
  return zigzag >> 1n ^ -(zigzag & 1n)
}
