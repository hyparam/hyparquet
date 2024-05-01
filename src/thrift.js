// TCompactProtocol types
const CompactType = {
  STOP: 0,
  TRUE: 1,
  FALSE: 2,
  BYTE: 3,
  I16: 4,
  I32: 5,
  I64: 6,
  DOUBLE: 7,
  BINARY: 8,
  LIST: 9,
  SET: 10,
  MAP: 11,
  STRUCT: 12,
  UUID: 13,
}

/**
 * Parse TCompactProtocol
 *
 * @param {DataReader} reader
 * @returns {Record<string, any>}
 */
export function deserializeTCompactProtocol(reader) {
  let lastFid = 0
  /** @type {Record<string, any>} */
  const value = {}

  while (reader.offset < reader.view.byteLength) {
    // Parse each field based on its type and add to the result object
    const [type, fid, newLastFid] = readFieldBegin(reader, lastFid)
    lastFid = newLastFid

    if (type === CompactType.STOP) {
      break
    }

    // Handle the field based on its type
    value[`field_${fid}`] = readElement(reader, type)
  }

  return value
}

/**
 * Read a single element based on its type
 *
 * @param {DataReader} reader
 * @param {number} type
 * @returns {any} value
 */
function readElement(reader, type) {
  switch (type) {
  case CompactType.TRUE:
    return true
  case CompactType.FALSE:
    return false
  case CompactType.BYTE:
    // read byte directly
    return reader.view.getInt8(reader.offset++)
  case CompactType.I16:
  case CompactType.I32:
    return readZigZag(reader)
  case CompactType.I64:
    return readZigZagBigInt(reader)
  case CompactType.DOUBLE: {
    const value = reader.view.getFloat64(reader.offset, true)
    reader.offset += 8
    return value
  }
  case CompactType.BINARY: {
    // strings are encoded as utf-8, no \0 delimiter
    const stringLength = readVarInt(reader)
    const strBytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, stringLength)
    reader.offset += stringLength
    return new TextDecoder().decode(strBytes)
  }
  case CompactType.LIST: {
    const [elemType, listSize] = readCollectionBegin(reader)
    const values = new Array(listSize)
    for (let i = 0; i < listSize; i++) {
      values[i] = readElement(reader, elemType)
    }
    return values
  }
  case CompactType.STRUCT: {
    /** @type {Record<string, any>} */
    const structValues = {}
    let structLastFid = 0
    while (true) {
      let structFieldType, structFid
      [structFieldType, structFid, structLastFid] = readFieldBegin(reader, structLastFid)
      if (structFieldType === CompactType.STOP) {
        break
      }
      structValues[`field_${structFid}`] = readElement(reader, structFieldType)
    }
    return structValues
  }
  // TODO: MAP and SET
  case CompactType.UUID: {
    // Read 16 bytes to uuid string
    let uuid = ''
    for (let i = 0; i < 16; i++) {
      uuid += reader.view.getUint8(reader.offset++).toString(16).padStart(2, '0')
    }
    return uuid
  }
  default:
    throw new Error(`thrift unhandled type: ${type}`)
  }
}

/**
 * Var int, also known as Unsigned LEB128.
 * Var ints take 1 to 5 bytes (int32) or 1 to 10 bytes (int64).
 * Takes a Big Endian unsigned integer, left-pads the bit-string to make it a
 * multiple of 7 bits, splits it into 7-bit groups, prefix the most-significant
 * 7-bit group with the 0 bit, prefixing the remaining 7-bit groups with the
 * 1 bit and encode the resulting bit-string as Little Endian.
 *
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader
 * @returns {number} value
 */
export function readVarInt(reader) {
  let result = 0
  let shift = 0
  while (true) {
    const byte = reader.view.getUint8(reader.offset++)
    result |= (byte & 0x7f) << shift
    if ((byte & 0x80) === 0) {
      return result
    }
    shift += 7
  }
}

/**
 * Read a varint as a bigint.
 *
 * @param {DataReader} reader
 * @returns {bigint} value
 */
function readVarBigInt(reader) {
  let result = BigInt(0)
  let shift = BigInt(0)
  while (true) {
    const byte = BigInt(reader.view.getUint8(reader.offset++))
    result |= (byte & BigInt(0x7f)) << shift
    if ((byte & BigInt(0x80)) === BigInt(0)) {
      return result
    }
    shift += BigInt(7)
  }
}

/**
 * Values of type int32 and int64 are transformed to a zigzag int.
 * A zigzag int folds positive and negative numbers into the positive number space.
 *
 * @param {DataReader} reader
 * @returns {number} value
 */
export function readZigZag(reader) {
  const zigzag = readVarInt(reader)
  // convert zigzag to int
  return (zigzag >>> 1) ^ -(zigzag & 1)
}

/**
 * A zigzag int folds positive and negative numbers into the positive number space.
 * This version returns a BigInt.
 *
 * @param {DataReader} reader
 * @returns {bigint} value
 */
function readZigZagBigInt(reader) {
  const zigzag = readVarBigInt(reader)
  // convert zigzag to int
  return (zigzag >> BigInt(1)) ^ -(zigzag & BigInt(1))
}

/**
 * Get thrift type from half a byte
 *
 * @param {number} byte
 * @returns {number}
 */
function getCompactType(byte) {
  return byte & 0x0f
}

/**
 * Read field type and field id
 *
 * @param {DataReader} reader
 * @param {number} lastFid
 * @returns {[number, number, number]} [type, fid, newLastFid]
 */
function readFieldBegin(reader, lastFid) {
  const type = reader.view.getUint8(reader.offset++)
  if ((type & 0x0f) === CompactType.STOP) {
    // STOP also ends a struct
    return [0, 0, lastFid]
  }
  const delta = type >> 4
  let fid // field id
  if (delta === 0) {
    // not a delta, read zigzag varint field id
    fid = readZigZag(reader)
  } else {
    // add delta to last field id
    fid = lastFid + delta
  }
  return [getCompactType(type), fid, fid]
}

/**
 * Read collection type and size
 *
 * @param {DataReader} reader
 * @returns {[number, number]} [type, size]
 */
function readCollectionBegin(reader) {
  const sizeType = reader.view.getUint8(reader.offset++)
  const size = sizeType >> 4
  const type = getCompactType(sizeType)
  if (size === 15) {
    const newSize = readVarInt(reader)
    return [type, newSize]
  }
  return [type, size]
}

/**
 * Convert int to varint. Outputs 1-5 bytes for int32.
 *
 * @param {number} n
 * @returns {number[]}
 */
export function toVarInt(n) {
  let idx = 0
  const varInt = []
  while (true) {
    if ((n & ~0x7f) === 0) {
      varInt[idx++] = n
      break
    } else {
      varInt[idx++] = (n & 0x7f) | 0x80
      n >>>= 7
    }
  }
  return varInt
}
