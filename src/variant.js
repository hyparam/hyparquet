import { DEFAULT_PARSERS } from './convert.js'

/** @import {ParquetParsers, VariantMetadata} from './types.d.ts' */

const variantStringDecoder = new TextDecoder()
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
  if (!value || typeof value !== 'object') return value

  if ('metadata' in value) {
    const metadataBytes = value.metadata
    const valueBytes = value.value
    if (!(metadataBytes instanceof Uint8Array)) return undefined
    if (!(valueBytes instanceof Uint8Array)) return undefined
    const metadata = parseVariantMetadata(metadataBytes)
    return decodeVariantValue(metadata, valueBytes, parsers)
  }

  return value
}

/**
 * Decode a single variant value.
 *
 * @param {VariantMetadata} metadata
 * @param {Uint8Array} bytes
 * @param {ParquetParsers} parsers
 * @returns {any}
 */
function decodeVariantValue(metadata, bytes, parsers) {
  if (!bytes.length) return undefined
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const reader = { view, offset: 0 }
  return readVariant(reader, metadata, parsers)
}

/**
 * Parse and cache variant metadata dictionary.
 *
 * @param {Uint8Array} bytes
 * @returns {VariantMetadata}
 */
function parseVariantMetadata(bytes) {
  if (bytes.byteLength === 0) {
    return { dictionary: [], sorted: true }
  }
  let safeBytes = bytes
  if (!(safeBytes.buffer instanceof ArrayBuffer)) {
    safeBytes = safeBytes.slice()
  }
  let bufferCache = metadataCache.get(safeBytes.buffer)
  if (!bufferCache) {
    bufferCache = new Map()
    metadataCache.set(safeBytes.buffer, bufferCache)
  }
  const key = `${safeBytes.byteOffset}:${safeBytes.byteLength}`
  const cached = bufferCache.get(key)
  if (cached) return cached

  const view = new DataView(safeBytes.buffer, safeBytes.byteOffset, safeBytes.byteLength)
  let offset = 0
  const header = view.getUint8(offset++)
  const version = header & 0x0f
  if (version !== 1) throw new Error(`parquet unsupported variant metadata version: ${version}`)
  const sorted = (header >> 4 & 0x1) === 1
  const offsetSize = (header >> 6 & 0x3) + 1

  const dictionarySize = readUnsigned(view, offset, offsetSize)
  offset += offsetSize

  const offsets = new Array(dictionarySize + 1)
  for (let i = 0; i < offsets.length; i++) {
    offsets[i] = readUnsigned(view, offset, offsetSize)
    offset += offsetSize
  }

  const base = offset
  const dictionary = new Array(dictionarySize)
  for (let i = 0; i < dictionarySize; i++) {
    const start = offsets[i]
    const end = offsets[i + 1]
    const length = end - start
    const strBytes = new Uint8Array(safeBytes.buffer, safeBytes.byteOffset + base + start, length)
    dictionary[i] = variantStringDecoder.decode(strBytes)
  }

  const metadata = { dictionary, sorted }
  bufferCache.set(key, metadata)
  return metadata
}

/**
 * Read an unsigned little-endian integer from a DataView without advancing the offset.
 *
 * @param {DataView} view
 * @param {number} offset
 * @param {number} byteWidth
 * @returns {number}
 */
function readUnsigned(view, offset, byteWidth) {
  let value = 0
  for (let i = 0; i < byteWidth; i++) {
    value |= view.getUint8(offset + i) << i * 8
  }
  return value
}

/**
 * @param {{ view: DataView, offset: number }} reader
 * @param {VariantMetadata} metadata
 * @param {ParquetParsers} parsers
 * @returns {any}
 */
function readVariant(reader, metadata, parsers) {
  if (reader.offset >= reader.view.byteLength) {
    throw new Error('parquet variant truncated value')
  }
  const typeByte = reader.view.getUint8(reader.offset++)
  const basicType = typeByte & 0x3
  const header = typeByte >> 2
  if (basicType === 0) return readVariantPrimitive(reader, header, parsers)
  if (basicType === 1) return readVariantShortString(reader, header)
  if (basicType === 2) return readVariantObject(reader, header, metadata, parsers)
  if (basicType === 3) return readVariantArray(reader, header, metadata, parsers)
  throw new Error(`parquet unsupported variant basic type: ${basicType}`)
}

/**
 * @param {{ view: DataView, offset: number }} reader
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
  case 16:
    return readVariantString(reader)
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
  case 20:
    return readVariantUuid(reader)
  default:
    throw new Error(`parquet unsupported variant primitive type: ${typeId}`)
  }
}

/**
 * @param {{ view: DataView, offset: number }} reader
 * @param {number} length
 * @returns {string}
 */
function readVariantShortString(reader, length) {
  const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
  reader.offset += length
  return variantStringDecoder.decode(bytes)
}

/**
 * @param {{ view: DataView, offset: number }} reader
 * @param {number} header
 * @param {VariantMetadata} metadata
 * @param {ParquetParsers} parsers
 * @returns {Record<string, any>}
 */
function readVariantObject(reader, header, metadata, parsers) {
  const fieldOffsetSize = header & 0x3
  const fieldIdSize = header >> 2 & 0x3
  const isLarge = header >> 4 & 0x1
  const offsetWidth = fieldOffsetSize + 1
  const idWidth = fieldIdSize + 1
  const numElements = isLarge ? readUnsigned(reader.view, reader.offset, 4) : reader.view.getUint8(reader.offset)
  reader.offset += isLarge ? 4 : 1

  /** @type {number[]} */
  const fieldIds = new Array(numElements)
  for (let i = 0; i < numElements; i++) {
    fieldIds[i] = readUnsigned(reader.view, reader.offset, idWidth)
    reader.offset += idWidth
  }

  const offsets = new Array(numElements + 1)
  for (let i = 0; i < offsets.length; i++) {
    offsets[i] = readUnsigned(reader.view, reader.offset, offsetWidth)
    reader.offset += offsetWidth
  }

  const valuesStart = reader.offset
  const base = reader.view.byteOffset + valuesStart
  /** @type {Record<string, any>} */
  const out = {}
  for (let i = 0; i < numElements; i++) {
    const key = metadata.dictionary[fieldIds[i]]
    if (key === undefined) {
      throw new Error('parquet variant field id out of range')
    }
    const start = offsets[i]
    const end = offsets[i + 1]
    const length = end - start
    const slice = new Uint8Array(reader.view.buffer, base + start, length)
    out[key] = decodeVariantValue(metadata, slice, parsers)
  }
  reader.offset = valuesStart + offsets[offsets.length - 1]
  return out
}

/**
 * @param {{ view: DataView, offset: number }} reader
 * @param {number} header
 * @param {VariantMetadata} metadata
 * @param {ParquetParsers} parsers
 * @returns {any[]}
 */
function readVariantArray(reader, header, metadata, parsers) {
  const fieldOffsetSize = header & 0x3
  const isLarge = header >> 2 & 0x1
  const offsetWidth = fieldOffsetSize + 1
  const numElements = isLarge ? readUnsigned(reader.view, reader.offset, 4) : reader.view.getUint8(reader.offset)
  reader.offset += isLarge ? 4 : 1

  const offsets = new Array(numElements + 1)
  for (let i = 0; i < offsets.length; i++) {
    offsets[i] = readUnsigned(reader.view, reader.offset, offsetWidth)
    reader.offset += offsetWidth
  }

  const valuesStart = reader.offset
  const base = reader.view.byteOffset + valuesStart
  const result = new Array(numElements)
  for (let i = 0; i < numElements; i++) {
    const start = offsets[i]
    const end = offsets[i + 1]
    const length = end - start
    const slice = new Uint8Array(reader.view.buffer, base + start, length)
    result[i] = decodeVariantValue(metadata, slice, parsers)
  }
  reader.offset = valuesStart + offsets[offsets.length - 1]
  return result
}

/**
 * @param {{ view: DataView, offset: number }} reader
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
    unscaled = readLittleEndianBigInt(reader, 16)
  }

  return Number(unscaled) * 10 ** -scale
}

/**
 * @param {{ view: DataView, offset: number }} reader
 * @returns {Uint8Array}
 */
function readVariantBinary(reader) {
  const length = reader.view.getUint32(reader.offset, true)
  reader.offset += 4
  const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, length)
  reader.offset += length
  return bytes
}

/**
 * @param {{ view: DataView, offset: number }} reader
 * @returns {string}
 */
function readVariantString(reader) {
  const bytes = readVariantBinary(reader)
  return variantStringDecoder.decode(bytes)
}

/**
 * @param {{ view: DataView, offset: number }} reader
 * @returns {string}
 */
function readVariantUuid(reader) {
  const bytes = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, 16)
  reader.offset += 16
  let hex = ''
  for (const byte of bytes) {
    hex += byte.toString(16).padStart(2, '0')
  }
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

/**
 * Read little-endian signed bigint of given width.
 *
 * @param {{ view: DataView, offset: number }} reader
 * @param {number} width
 * @returns {bigint}
 */
function readLittleEndianBigInt(reader, width) {
  let value = 0n
  for (let i = 0; i < width; i++) {
    value |= BigInt(reader.view.getUint8(reader.offset + i)) << BigInt(i * 8)
  }
  const signBit = 1n << BigInt(width * 8 - 1)
  if (value & signBit) {
    value -= 1n << BigInt(width * 8)
  }
  reader.offset += width
  return value
}
