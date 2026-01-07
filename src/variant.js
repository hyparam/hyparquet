import { DEFAULT_PARSERS } from './convert.js'

/** @import {DataReader, ParquetParsers, VariantMetadata} from './types.d.ts' */

const decoder = new TextDecoder()
/** @type {WeakMap<object, Map<string, VariantMetadata>>} */
const metadataCache = new WeakMap()

/**
 * Recursively decode variant structs into native values.
 *
 * @param {any} value
 * @param {ParquetParsers} [parsers]
 * @returns {any}
 */
export function decodeVariantColumn(value, parsers = DEFAULT_PARSERS) {
  if (Array.isArray(value)) {
    return value.map(entry => decodeVariantColumn(entry, parsers))
  }
  if (typeof value !== 'object') return value

  if ('metadata' in value) {
    const metadata = parseVariantMetadata(value.metadata)

    // Decode shredded fields from typed_value
    const shreddedFields = value.typed_value && decodeTypedValue(value.typed_value, metadata, parsers)

    // Decode binary value (may contain additional fields for partially shredded objects)
    const binaryValue = value.value && readVariant(makeReader(value.value), metadata, parsers)

    // Merge shredded and binary values for partially shredded objects
    if (shreddedFields && binaryValue) {
      return { ...binaryValue, ...shreddedFields }
    }
    return shreddedFields ?? binaryValue
  }

  return value
}

/**
 * Decode a shredded variant typed_value field.
 *
 * @param {any} typedValue
 * @param {VariantMetadata} metadata
 * @param {ParquetParsers} parsers
 * @returns {any}
 */
function decodeTypedValue(typedValue, metadata, parsers) {
  // Handle {typed_value, value} wrapper - unwrap and recurse
  if (typedValue && typeof typedValue === 'object' && !Array.isArray(typedValue) && !(typedValue instanceof Uint8Array)) {
    if ('typed_value' in typedValue) {
      return decodeTypedValue(typedValue.typed_value, metadata, parsers)
    }
    if ('value' in typedValue && typedValue.value instanceof Uint8Array) {
      return readVariant(makeReader(typedValue.value), metadata, parsers)
    }
    // Shredded object: each field value gets decoded
    /** @type {Record<string, any>} */
    const result = {}
    for (const [key, field] of Object.entries(typedValue)) {
      result[key] = decodeTypedValue(field, metadata, parsers)
    }
    return result
  }

  // Uint8Array: decode as binary variant
  if (typedValue instanceof Uint8Array) {
    return readVariant(makeReader(typedValue), metadata, parsers)
  }

  // Arrays
  if (Array.isArray(typedValue)) {
    return typedValue.map(element => decodeTypedValue(element, metadata, parsers))
  }

  return typedValue
}

/**
 * @param {Uint8Array} bytes
 * @returns {DataReader}
 */
function makeReader(bytes) {
  return { view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength), offset: 0 }
}

/**
 * Parse and cache variant metadata dictionary.
 *
 * @param {Uint8Array} bytes
 * @returns {VariantMetadata}
 */
function parseVariantMetadata(bytes) {
  let bufferCache = metadataCache.get(bytes.buffer)
  if (!bufferCache) {
    bufferCache = new Map()
    metadataCache.set(bytes.buffer, bufferCache)
  }
  const key = `${bytes.byteOffset}:${bytes.byteLength}`
  const cached = bufferCache.get(key)
  if (cached) return cached

  const reader = {
    view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
    offset: 0,
  }
  const header = reader.view.getUint8(reader.offset++)
  const version = header & 0x0f
  if (version !== 1) throw new Error(`parquet unsupported variant metadata version: ${version}`)
  const sorted = (header >> 4 & 0x1) === 1
  const offsetSize = (header >> 6 & 0x3) + 1

  const dictionarySize = readUnsigned(reader, offsetSize)

  const offsets = new Array(dictionarySize + 1)
  for (let i = 0; i < offsets.length; i++) {
    offsets[i] = readUnsigned(reader, offsetSize)
  }

  const base = reader.offset
  const dictionary = new Array(dictionarySize)
  for (let i = 0; i < dictionarySize; i++) {
    const start = offsets[i]
    const end = offsets[i + 1]
    const strBytes = new Uint8Array(bytes.buffer, bytes.byteOffset + base + start, end - start)
    dictionary[i] = decoder.decode(strBytes)
  }

  const metadata = { dictionary, sorted }
  bufferCache.set(key, metadata)
  return metadata
}

/**
 * @param {DataReader} reader
 * @param {number} byteWidth
 * @returns {number}
 */
function readUnsigned(reader, byteWidth) {
  let value = 0
  for (let i = 0; i < byteWidth; i++) {
    value |= reader.view.getUint8(reader.offset + i) << i * 8
  }
  reader.offset += byteWidth
  return value
}

/**
 * @param {DataReader} reader
 * @param {VariantMetadata} metadata
 * @param {ParquetParsers} parsers
 * @returns {any}
 */
function readVariant(reader, metadata, parsers) {
  const typeByte = reader.view.getUint8(reader.offset++)
  const basicType = typeByte & 0x3
  const header = typeByte >> 2
  if (basicType === 0) return readVariantPrimitive(reader, header, parsers)
  if (basicType === 2) return readVariantObject(reader, header, metadata, parsers)
  if (basicType === 3) return readVariantArray(reader, header, metadata, parsers)
  // else short string
  const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, header)
  reader.offset += header
  return decoder.decode(bytes)
}

/**
 * @param {DataReader} reader
 * @param {number} typeId
 * @param {ParquetParsers} parsers
 * @returns {any}
 */
function readVariantPrimitive(reader, typeId, parsers) {
  switch (typeId) {
  case 0: return null
  case 1: return true
  case 2: return false
  case 3: {
    const value = reader.view.getInt8(reader.offset)
    reader.offset += 1
    return value
  }
  case 4: {
    const value = reader.view.getInt16(reader.offset, true)
    reader.offset += 2
    return value
  }
  case 5: {
    const value = reader.view.getInt32(reader.offset, true)
    reader.offset += 4
    return value
  }
  case 6: {
    const value = reader.view.getBigInt64(reader.offset, true)
    reader.offset += 8
    return value
  }
  case 7: {
    const value = reader.view.getFloat64(reader.offset, true)
    reader.offset += 8
    return value
  }
  case 8:
    return readVariantDecimal(reader, 4)
  case 9:
    return readVariantDecimal(reader, 8)
  case 10:
    return readVariantDecimal(reader, 16)
  case 11: {
    const value = reader.view.getInt32(reader.offset, true)
    reader.offset += 4
    return parsers.dateFromDays(value)
  }
  case 12: // timestamp_micros (utc)
  case 13: { // timestamp_micros_ntz (no timezone)
    const value = reader.view.getBigInt64(reader.offset, true)
    reader.offset += 8
    return parsers.timestampFromMicroseconds(value)
  }
  case 14: {
    const value = reader.view.getFloat32(reader.offset, true)
    reader.offset += 4
    return value
  }
  case 15:
    return readVariantBinary(reader)
  case 16: {
    const bytes = readVariantBinary(reader)
    return decoder.decode(bytes)
  }
  case 17: {
    // time: microseconds since midnight
    const value = reader.view.getBigInt64(reader.offset, true)
    reader.offset += 8
    return value
  }
  case 18: // timestamp_nanos (utc)
  case 19: { // timestamp_nanos_ntz (no timezone)
    const value = reader.view.getBigInt64(reader.offset, true)
    reader.offset += 8
    return parsers.timestampFromNanoseconds(value)
  }
  case 20: {
    const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, 16)
    reader.offset += 16
    const hex = Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('')
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
  }
  default:
    throw new Error(`parquet unsupported variant primitive type: ${typeId}`)
  }
}

/**
 * @param {DataReader} reader
 * @param {number} header
 * @param {VariantMetadata} metadata
 * @param {ParquetParsers} parsers
 * @returns {Record<string, any>}
 */
function readVariantObject(reader, header, metadata, parsers) {
  const offsetWidth = (header & 0x3) + 1
  const idWidth = (header >> 2 & 0x3) + 1
  const isLarge = header >> 4 & 0x1
  const numElements = isLarge ? readUnsigned(reader, 4) : reader.view.getUint8(reader.offset++)

  /** @type {number[]} */
  const fieldIds = new Array(numElements)
  for (let i = 0; i < numElements; i++) {
    fieldIds[i] = readUnsigned(reader, idWidth)
  }

  const offsets = new Array(numElements + 1)
  for (let i = 0; i < offsets.length; i++) {
    offsets[i] = readUnsigned(reader, offsetWidth)
  }

  /** @type {Record<string, any>} */
  const out = {}
  for (let i = 0; i < numElements; i++) {
    const key = metadata.dictionary[fieldIds[i]]
    // Read value at the given offset
    const valueReader = {
      view: reader.view,
      offset: reader.offset + offsets[i],
    }
    out[key] = readVariant(valueReader, metadata, parsers)
  }
  reader.offset += offsets[offsets.length - 1]
  return out
}

/**
 * @param {DataReader} reader
 * @param {number} header
 * @param {VariantMetadata} metadata
 * @param {ParquetParsers} parsers
 * @returns {any[]}
 */
function readVariantArray(reader, header, metadata, parsers) {
  const fieldOffsetSize = header & 0x3
  const isLarge = header >> 2 & 0x1
  const offsetWidth = fieldOffsetSize + 1
  const numElements = readUnsigned(reader, isLarge ? 4 : 1)

  const offsets = new Array(numElements + 1)
  for (let i = 0; i < offsets.length; i++) {
    offsets[i] = readUnsigned(reader, offsetWidth)
  }

  const valuesStart = reader.offset
  const result = new Array(numElements)
  for (let i = 0; i < numElements; i++) {
    const valueReader = {
      view: reader.view,
      offset: valuesStart + offsets[i],
    }
    result[i] = readVariant(valueReader, metadata, parsers)
  }
  reader.offset = valuesStart + offsets[offsets.length - 1]
  return result
}

/**
 * @param {DataReader} reader
 * @param {number} width
 * @returns {number}
 */
function readVariantDecimal(reader, width) {
  const scale = reader.view.getUint8(reader.offset)
  reader.offset += 1
  let unscaled
  if (width === 4) {
    unscaled = BigInt(reader.view.getInt32(reader.offset, true))
    reader.offset += 4
  } else if (width === 8) {
    unscaled = reader.view.getBigInt64(reader.offset, true)
    reader.offset += 8
  } else {
    const low = reader.view.getBigUint64(reader.offset, true)
    const high = reader.view.getBigInt64(reader.offset + 8, true)
    unscaled = high << 64n | low
    reader.offset += 16
  }

  return Number(unscaled) * 10 ** -scale
}

/**
 * @param {DataReader} reader
 * @returns {Uint8Array}
 */
function readVariantBinary(reader) {
  const length = reader.view.getUint32(reader.offset, true)
  reader.offset += 4
  const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
  reader.offset += length
  return bytes
}
