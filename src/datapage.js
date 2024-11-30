import { deltaBinaryUnpack, deltaByteArray, deltaLengthByteArray } from './delta.js'
import { bitWidth, byteStreamSplit, readRleBitPackedHybrid } from './encoding.js'
import { readPlain } from './plain.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'
import { snappyUncompress } from './snappy.js'

/**
 * Read a data page from uncompressed reader.
 *
 * @param {Uint8Array} bytes raw page data (should already be decompressed)
 * @param {DataPageHeader} daph data page header
 * @param {SchemaTree[]} schemaPath
 * @param {ColumnMetaData} columnMetadata
 * @returns {DataPage} definition levels, repetition levels, and array of values
 */
export function readDataPage(bytes, daph, schemaPath, { type }) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const reader = { view, offset: 0 }
  /** @type {DecodedArray} */
  let dataPage

  // repetition and definition levels
  const repetitionLevels = readRepetitionLevels(reader, daph, schemaPath)
  // assert(!repetitionLevels.length || repetitionLevels.length === daph.num_values)
  const { definitionLevels, numNulls } = readDefinitionLevels(reader, daph, schemaPath)
  // assert(!definitionLevels.length || definitionLevels.length === daph.num_values)

  // read values based on encoding
  const nValues = daph.num_values - numNulls
  if (daph.encoding === 'PLAIN') {
    const { type_length } = schemaPath[schemaPath.length - 1].element
    dataPage = readPlain(reader, type, nValues, type_length)
  } else if (
    daph.encoding === 'PLAIN_DICTIONARY' ||
    daph.encoding === 'RLE_DICTIONARY' ||
    daph.encoding === 'RLE'
  ) {
    const bitWidth = type === 'BOOLEAN' ? 1 : view.getUint8(reader.offset++)
    if (bitWidth) {
      dataPage = new Array(nValues)
      readRleBitPackedHybrid(reader, bitWidth, view.byteLength - reader.offset, dataPage)
    } else {
      dataPage = new Uint8Array(nValues) // nValue zeroes
    }
  } else if (daph.encoding === 'BYTE_STREAM_SPLIT') {
    const { type_length } = schemaPath[schemaPath.length - 1].element
    dataPage = byteStreamSplit(reader, nValues, type, type_length)
  } else {
    throw new Error(`parquet unsupported encoding: ${daph.encoding}`)
  }

  return { definitionLevels, repetitionLevels, dataPage }
}

/**
 * @param {Uint8Array} bytes raw page data
 * @param {DictionaryPageHeader} diph dictionary page header
 * @param {ColumnMetaData} columnMetadata
 * @param {number | undefined} typeLength - type_length from schema
 * @returns {DecodedArray}
 */
export function readDictionaryPage(bytes, diph, columnMetadata, typeLength) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const reader = { view, offset: 0 }
  return readPlain(reader, columnMetadata.type, diph.num_values, typeLength)
}

/**
 * @import {ColumnMetaData, CompressionCodec, Compressors, DataPage, DataPageHeader, DataPageHeaderV2, DataReader, DecodedArray, DictionaryPageHeader, PageHeader, SchemaTree} from '../src/types.d.ts'
 * @param {DataReader} reader data view for the page
 * @param {DataPageHeader} daph data page header
 * @param {SchemaTree[]} schemaPath
 * @returns {any[]} repetition levels and number of bytes read
 */
function readRepetitionLevels(reader, daph, schemaPath) {
  if (schemaPath.length > 1) {
    const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
    if (maxRepetitionLevel) {
      const values = new Array(daph.num_values)
      readRleBitPackedHybrid(reader, bitWidth(maxRepetitionLevel), 0, values)
      return values
    }
  }
  return []
}

/**
 * @param {DataReader} reader data view for the page
 * @param {DataPageHeader} daph data page header
 * @param {SchemaTree[]} schemaPath
 * @returns {{ definitionLevels: number[], numNulls: number }} definition levels
 */
function readDefinitionLevels(reader, daph, schemaPath) {
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  if (!maxDefinitionLevel) return { definitionLevels: [], numNulls: 0 }

  const definitionLevels = new Array(daph.num_values)
  readRleBitPackedHybrid(reader, bitWidth(maxDefinitionLevel), 0, definitionLevels)

  // count nulls
  let numNulls = daph.num_values
  for (const def of definitionLevels) {
    if (def === maxDefinitionLevel) numNulls--
  }
  if (numNulls === 0) definitionLevels.length = 0

  return { definitionLevels, numNulls }
}

/**
 * @param {Uint8Array} compressedBytes
 * @param {number} uncompressed_page_size
 * @param {CompressionCodec} codec
 * @param {Compressors | undefined} compressors
 * @returns {Uint8Array}
 */
export function decompressPage(compressedBytes, uncompressed_page_size, codec, compressors) {
  /** @type {Uint8Array} */
  let page
  const customDecompressor = compressors?.[codec]
  if (codec === 'UNCOMPRESSED') {
    page = compressedBytes
  } else if (customDecompressor) {
    page = customDecompressor(compressedBytes, uncompressed_page_size)
  } else if (codec === 'SNAPPY') {
    page = new Uint8Array(uncompressed_page_size)
    snappyUncompress(compressedBytes, page)
  } else {
    throw new Error(`parquet unsupported compression codec: ${codec}`)
  }
  if (page?.length !== uncompressed_page_size) {
    throw new Error(`parquet decompressed page length ${page?.length} does not match header ${uncompressed_page_size}`)
  }
  return page
}


/**
 * Read a data page from the given Uint8Array.
 *
 * @param {Uint8Array} compressedBytes raw page data
 * @param {PageHeader} ph page header
 * @param {SchemaTree[]} schemaPath
 * @param {ColumnMetaData} columnMetadata
 * @param {Compressors | undefined} compressors
 * @returns {DataPage} definition levels, repetition levels, and array of values
 */
export function readDataPageV2(compressedBytes, ph, schemaPath, columnMetadata, compressors) {
  const view = new DataView(compressedBytes.buffer, compressedBytes.byteOffset, compressedBytes.byteLength)
  const reader = { view, offset: 0 }
  const { codec, type } = columnMetadata
  const daph2 = ph.data_page_header_v2
  if (!daph2) throw new Error('parquet data page header v2 is undefined')

  // repetition levels
  const repetitionLevels = readRepetitionLevelsV2(reader, daph2, schemaPath)
  reader.offset = daph2.repetition_levels_byte_length // readVarInt() => len for boolean v2?

  // definition levels
  const definitionLevels = readDefinitionLevelsV2(reader, daph2, schemaPath)
  // assert(reader.offset === daph2.repetition_levels_byte_length + daph2.definition_levels_byte_length)

  const uncompressedPageSize = ph.uncompressed_page_size - daph2.definition_levels_byte_length - daph2.repetition_levels_byte_length

  let page = compressedBytes.subarray(reader.offset)
  if (daph2.is_compressed !== false) {
    page = decompressPage(page, uncompressedPageSize, codec, compressors)
  }
  const pageView = new DataView(page.buffer, page.byteOffset, page.byteLength)
  const pageReader = { view: pageView, offset: 0 }

  // read values based on encoding
  /** @type {DecodedArray} */
  let dataPage
  const nValues = daph2.num_values - daph2.num_nulls
  if (daph2.encoding === 'PLAIN') {
    const { type_length } = schemaPath[schemaPath.length - 1].element
    dataPage = readPlain(pageReader, type, nValues, type_length)
  } else if (daph2.encoding === 'RLE') {
    // assert(columnMetadata.type === 'BOOLEAN')
    dataPage = new Array(nValues)
    readRleBitPackedHybrid(pageReader, 1, 0, dataPage)
    dataPage = dataPage.map(x => !!x)
  } else if (
    daph2.encoding === 'PLAIN_DICTIONARY' ||
    daph2.encoding === 'RLE_DICTIONARY'
  ) {
    const bitWidth = pageView.getUint8(pageReader.offset++)
    dataPage = new Array(nValues)
    readRleBitPackedHybrid(pageReader, bitWidth, uncompressedPageSize - 1, dataPage)
  } else if (daph2.encoding === 'DELTA_BINARY_PACKED') {
    const int32 = type === 'INT32'
    dataPage = int32 ? new Int32Array(nValues) : new BigInt64Array(nValues)
    deltaBinaryUnpack(pageReader, nValues, dataPage)
  } else if (daph2.encoding === 'DELTA_LENGTH_BYTE_ARRAY') {
    dataPage = new Array(nValues)
    deltaLengthByteArray(pageReader, nValues, dataPage)
  } else if (daph2.encoding === 'DELTA_BYTE_ARRAY') {
    dataPage = new Array(nValues)
    deltaByteArray(pageReader, nValues, dataPage)
  } else if (daph2.encoding === 'BYTE_STREAM_SPLIT') {
    const { type_length } = schemaPath[schemaPath.length - 1].element
    dataPage = byteStreamSplit(reader, nValues, type, type_length)
  } else {
    throw new Error(`parquet unsupported encoding: ${daph2.encoding}`)
  }

  return { definitionLevels, repetitionLevels, dataPage }
}

/**
 * @param {DataReader} reader
 * @param {DataPageHeaderV2} daph2 data page header v2
 * @param {SchemaTree[]} schemaPath
 * @returns {any[]} repetition levels
 */
function readRepetitionLevelsV2(reader, daph2, schemaPath) {
  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  if (!maxRepetitionLevel) return []

  const values = new Array(daph2.num_values)
  readRleBitPackedHybrid(
    reader, bitWidth(maxRepetitionLevel), daph2.repetition_levels_byte_length, values
  )
  return values
}

/**
 * @param {DataReader} reader
 * @param {DataPageHeaderV2} daph2 data page header v2
 * @param {SchemaTree[]} schemaPath
 * @returns {number[] | undefined} definition levels
 */
function readDefinitionLevelsV2(reader, daph2, schemaPath) {
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  if (maxDefinitionLevel) {
    // V2 we know the length
    const values = new Array(daph2.num_values)
    readRleBitPackedHybrid(reader, bitWidth(maxDefinitionLevel), daph2.definition_levels_byte_length, values)
    return values
  }
}
