/**
 * Read `count` boolean values.
 *
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {boolean[]} array of boolean values
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
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {number[]} array of int32 values
 */
function readPlainInt32(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = reader.view.getInt32(reader.offset + i * 4, true)
  }
  reader.offset += count * 4
  return values
}

/**
 * Read `count` int64 values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {bigint[]} array of int64 values
 */
function readPlainInt64(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = reader.view.getBigInt64(reader.offset + i * 8, true)
  }
  reader.offset += count * 8
  return values
}

/**
 * Read `count` int96 values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {bigint[]} array of int96 values
 */
function readPlainInt96(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    const low = reader.view.getBigInt64(reader.offset + i * 12, true)
    const high = reader.view.getInt32(reader.offset + i * 12 + 8, true)
    values[i] = (BigInt(high) << BigInt(32)) | low
  }
  reader.offset += count * 12
  return values
}

/**
 * Read `count` float values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {number[]} array of float values
 */
function readPlainFloat(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = reader.view.getFloat32(reader.offset + i * 4, true)
  }
  reader.offset += count * 4
  return values
}

/**
 * Read `count` double values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {number[]} array of double values
 */
function readPlainDouble(reader, count) {
  const values = new Array(count)
  for (let i = 0; i < count; i++) {
    values[i] = reader.view.getFloat64(reader.offset + i * 8, true)
  }
  reader.offset += count * 8
  return values
}

/**
 * Read `count` byte array values.
 *
 * @param {DataReader} reader - buffer to read data from
 * @param {number} count - number of values to read
 * @returns {Uint8Array[]} array of byte arrays
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
 * @param {DataReader} reader - buffer to read data from
 * @param {number} fixedLength - length of each fixed length byte array
 * @returns {Uint8Array} array of fixed length byte arrays
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
 * Read `count` values of the given type from the reader.view.
 *
 * @typedef {import("./types.d.ts").DecodedArray} DecodedArray
 * @typedef {import("./types.d.ts").ParquetType} ParquetType
 * @param {DataReader} reader - buffer to read data from
 * @param {ParquetType} type - parquet type of the data
 * @param {number} count - number of values to read
 * @param {boolean} utf8 - whether to decode byte arrays as UTF-8
 * @returns {DecodedArray} array of values
 */
export function readPlain(reader, type, count, utf8) {
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
    const byteArray = readPlainByteArray(reader, count)
    if (utf8) {
      const decoder = new TextDecoder()
      return byteArray.map(bytes => decoder.decode(bytes))
    }
    return byteArray
  } else if (type === 'FIXED_LEN_BYTE_ARRAY') {
    return readPlainByteArrayFixed(reader, count)
  } else {
    throw new Error(`parquet unhandled type: ${type}`)
  }
}
