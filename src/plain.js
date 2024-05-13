/**
 * Read `count` values of the given type from the reader.view.
 *
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @typedef {import("./types.d.ts").DecodedArray} DecodedArray
 * @typedef {import("./types.d.ts").ParquetType} ParquetType
 * @param {DataReader} reader - buffer to read data from
 * @param {ParquetType} type - parquet type of the data
 * @param {number} count - number of values to read
 * @returns {DecodedArray} array of values
 */
export function readPlain(reader, type, count) {
  if (count === 0) return []
  if (type === 'BOOLEAN') {
    return readPlainBoolean(reader, count)
  } else if (type === 'INT32') {
    return readPlainInt32(reader, count)
  } else if (type === 'INT64') {
    return readPlainInt64(reader, count)
  } else if (type === 'INT96') {
    return readPlainInt96(reader, count)
  } else if (type === 'FLOAT') {
    return readPlainFloat(reader, count)
  } else if (type === 'DOUBLE') {
    return readPlainDouble(reader, count)
  } else if (type === 'BYTE_ARRAY') {
    return readPlainByteArray(reader, count)
  } else if (type === 'FIXED_LEN_BYTE_ARRAY') {
    return readPlainByteArrayFixed(reader, count)
  } else {
    throw new Error(`parquet unhandled type: ${type}`)
  }
}

/**
 * Read `count` boolean values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {boolean[]}
 */
function readPlainBoolean(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const byteOffset = reader.offset + Math.floor(i / 8)
    const bitOffset = i % 8
    const byte = reader.view.getUint8(byteOffset)
    values[i] = (byte & (1 << bitOffset)) !== 0
  }
  reader.offset += Math.ceil(count / 8)
  return values
}

/**
 * Read `count` int32 values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Int32Array}
 */
function readPlainInt32(reader, count) {
  const values = (reader.view.byteOffset + reader.offset) % 4
    ? new Int32Array(align(reader.view.buffer, reader.view.byteOffset + reader.offset, count * 4))
    : new Int32Array(reader.view.buffer, reader.view.byteOffset + reader.offset, count)
  reader.offset += count * 4
  return values
}

/**
 * Read `count` int64 values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {BigInt64Array}
 */
function readPlainInt64(reader, count) {
  const values = (reader.view.byteOffset + reader.offset) % 8
    ? new BigInt64Array(align(reader.view.buffer, reader.view.byteOffset + reader.offset, count * 8))
    : new BigInt64Array(reader.view.buffer, reader.view.byteOffset + reader.offset, count)
  reader.offset += count * 8
  return values
}

/**
 * Read `count` int96 values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {bigint[]}
 */
function readPlainInt96(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const low = reader.view.getBigInt64(reader.offset + i * 12, true)
    const high = reader.view.getInt32(reader.offset + i * 12 + 8, true)
    values[i] = (BigInt(high) << 64n) | low
  }
  reader.offset += count * 12
  return values
}

/**
 * Read `count` float values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Float32Array}
 */
function readPlainFloat(reader, count) {
  const values = (reader.view.byteOffset + reader.offset) % 4
    ? new Float32Array(align(reader.view.buffer, reader.view.byteOffset + reader.offset, count * 4))
    : new Float32Array(reader.view.buffer, reader.view.byteOffset + reader.offset, count)
  reader.offset += count * 4
  return values
}

/**
 * Read `count` double values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Float64Array}
 */
function readPlainDouble(reader, count) {
  const values = (reader.view.byteOffset + reader.offset) % 8
    ? new Float64Array(align(reader.view.buffer, reader.view.byteOffset + reader.offset, count * 8))
    : new Float64Array(reader.view.buffer, reader.view.byteOffset + reader.offset, count)
  reader.offset += count * 8
  return values
}

/**
 * Read `count` byte array values.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @returns {Uint8Array[]}
 */
function readPlainByteArray(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const length = reader.view.getInt32(reader.offset, true)
    reader.offset += 4
    values[i] = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
    reader.offset += length
  }
  return values
}

/**
 * Read a fixed length byte array.
 *
 * @param {DataReader} reader
 * @param {number} fixedLength
 * @returns {Uint8Array}
 */
function readPlainByteArrayFixed(reader, fixedLength) {
  reader.offset += fixedLength
  return new Uint8Array(
    reader.view.buffer,
    reader.view.byteOffset + reader.offset - fixedLength,
    fixedLength
  )
}

/**
 * Create a new buffer with the offset and size.
 *
 * @param {ArrayBuffer} buffer
 * @param {number} offset
 * @param {number} size
 * @returns {ArrayBuffer}
 */
function align(buffer, offset, size) {
  const aligned = new ArrayBuffer(size)
  new Uint8Array(aligned).set(new Uint8Array(buffer, offset, size))
  return aligned
}
