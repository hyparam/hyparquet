import { readRleBitPackedHybrid, widthFromMaxInt } from './encoding.js'
import { readPlain } from './plain.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel, isRequired } from './schema.js'

/**
 * Read a data page from the given Uint8Array.
 *
 * @typedef {import("./types.d.ts").DataPage} DataPage
 * @typedef {import("./types.d.ts").ColumnMetaData} ColumnMetaData
 * @typedef {import("./types.d.ts").DataPageHeader} DataPageHeader
 * @typedef {import("./types.d.ts").SchemaTree} SchemaTree
 * @typedef {import("./types.d.ts").DecodedArray} DecodedArray
 * @param {Uint8Array} bytes raw page data (should already be decompressed)
 * @param {DataPageHeader} daph data page header
 * @param {SchemaTree[]} schemaPath
 * @param {ColumnMetaData} columnMetadata
 * @returns {DataPage} definition levels, repetition levels, and array of values
 */
export function readDataPage(bytes, daph, schemaPath, columnMetadata) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const reader = { view, offset: 0 }
  /** @type {DecodedArray} */
  let dataPage = []

  // repetition levels
  const repetitionLevels = readRepetitionLevels(reader, daph, schemaPath)

  // definition levels
  const { definitionLevels, numNulls } = readDefinitionLevels(reader, daph, schemaPath)

  // read values based on encoding
  const nValues = daph.num_values - numNulls
  if (daph.encoding === 'PLAIN') {
    const { element } = schemaPath[schemaPath.length - 1]
    const utf8 = element.converted_type === 'UTF8'
    const plainObj = readPlain(reader, columnMetadata.type, nValues, utf8)
    dataPage = plainObj
  } else if (
    daph.encoding === 'PLAIN_DICTIONARY' ||
    daph.encoding === 'RLE_DICTIONARY' ||
    daph.encoding === 'RLE'
  ) {
    // bit width is stored as single byte
    let bitWidth
    // TODO: RLE encoding uses bitWidth = schemaElement.type_length
    if (columnMetadata.type === 'BOOLEAN') {
      bitWidth = 1
    } else {
      bitWidth = view.getUint8(reader.offset)
      reader.offset++
    }
    if (bitWidth) {
      dataPage = new Array(nValues)
      readRleBitPackedHybrid(reader, bitWidth, view.byteLength - reader.offset, dataPage)
    } else {
      // nval zeros
      dataPage = new Array(nValues).fill(0)
    }
  } else {
    throw new Error(`parquet unsupported encoding: ${daph.encoding}`)
  }

  return { definitionLevels, repetitionLevels, dataPage }
}

/**
 * Read a page containing dictionary data.
 *
 * @typedef {import("./types.d.ts").DictionaryPageHeader} DictionaryPageHeader
 * @param {Uint8Array} bytes raw page data
 * @param {DictionaryPageHeader} diph dictionary page header
 * @param {ColumnMetaData} columnMetadata
 * @returns {ArrayLike<any>} array of values
 */
export function readDictionaryPage(bytes, diph, columnMetadata) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const reader = { view, offset: 0 }
  return readPlain(reader, columnMetadata.type, diph.num_values, false)
}

/**
 * Read the repetition levels from this page, if any.
 *
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader data view for the page
 * @param {DataPageHeader} daph data page header
 * @param {SchemaTree[]} schemaPath
 * @returns {any[]} repetition levels and number of bytes read
 */
function readRepetitionLevels(reader, daph, schemaPath) {
  if (schemaPath.length > 1) {
    const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
    if (maxRepetitionLevel) {
      const bitWidth = widthFromMaxInt(maxRepetitionLevel)
      const values = new Array(daph.num_values)
      readRleBitPackedHybrid(reader, bitWidth, 0, values)
      return values
    }
  }
  return []
}

/**
 * Read the definition levels from this page, if any.
 *
 * @param {DataReader} reader data view for the page
 * @param {DataPageHeader} daph data page header
 * @param {SchemaTree[]} schemaPath
 * @returns {{ definitionLevels: number[], numNulls: number }} definition levels
 */
function readDefinitionLevels(reader, daph, schemaPath) {
  if (!isRequired(schemaPath)) {
    const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
    const bitWidth = widthFromMaxInt(maxDefinitionLevel)
    if (bitWidth) {
      // num_values is index 1 for either type of page header
      const definitionLevels = new Array(daph.num_values)
      readRleBitPackedHybrid(reader, bitWidth, 0, definitionLevels)

      // count nulls
      let numNulls = daph.num_values
      for (const def of definitionLevels) {
        if (def === maxDefinitionLevel) numNulls--
      }
      if (numNulls === 0) {
        definitionLevels.length = 0
      }

      return { definitionLevels, numNulls }
    }
  }
  return { definitionLevels: [], numNulls: 0 }
}
