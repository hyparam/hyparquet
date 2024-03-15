import { readData, readPlain, readRleBitPackedHybrid, widthFromMaxInt } from './encoding.js'
import {
  getMaxDefinitionLevel,
  getMaxRepetitionLevel,
  isRequired,
  schemaElement,
  skipDefinitionBytes,
} from './schema.js'

const skipNulls = false // TODO

/**
 * @typedef {{ byteLength: number, definitionLevels: number[], numNulls: number }} DefinitionLevels
 * @typedef {import("./types.d.ts").DataPage} DataPage
 * @typedef {import("./types.d.ts").ColumnMetaData} ColumnMetaData
 * @typedef {import("./types.d.ts").DataPageHeader} DataPageHeader
 * @typedef {import("./types.d.ts").DictionaryPageHeader} DictionaryPageHeader
 * @typedef {import("./types.d.ts").SchemaElement} SchemaElement
 */
/**
 * @typedef {import("./types.d.ts").Decoded<T>} Decoded
 * @template T
 */

/**
 * Read a data page from the given Uint8Array.
 *
 * @param {Uint8Array} bytes raw page data (should already be decompressed)
 * @param {DataPageHeader} daph data page header
 * @param {SchemaElement[]} schema schema for the file
 * @param {ColumnMetaData} columnMetadata metadata for the column
 * @returns {DataPage} definition levels, repetition levels, and array of values
 */
export function readDataPage(bytes, daph, schema, columnMetadata) {
  const dataView = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  let offset = 0
  /** @type {any[]} */
  let values = []

  // repetition levels
  const { value: repetitionLevels, byteLength } = readRepetitionLevels(
    dataView, offset, daph, schema, columnMetadata
  )
  offset += byteLength

  // definition levels
  let definitionLevels = undefined
  let numNulls = 0
  // let maxDefinitionLevel = -1
  // TODO: move into readDefinitionLevels
  if (skipNulls && !isRequired(schema, columnMetadata.path_in_schema)) {
    // skip_definition_bytes
    offset += skipDefinitionBytes(daph.num_values)
  } else {
    const dl = readDefinitionLevels(dataView, offset, daph, schema, columnMetadata.path_in_schema)
    definitionLevels = dl.definitionLevels
    numNulls = dl.numNulls
    offset += dl.byteLength
  }

  // read values based on encoding
  const nValues = daph.num_values - numNulls
  if (daph.encoding === 'PLAIN') {
    const { element } = schemaElement(schema, columnMetadata.path_in_schema)
    const utf8 = element.converted_type === 'UTF8'
    const plainObj = readPlain(dataView, columnMetadata.type, nValues, offset, utf8)
    values = Array.isArray(plainObj.value) ? plainObj.value : Array.from(plainObj.value)
    offset += plainObj.byteLength
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
      bitWidth = dataView.getUint8(offset)
      offset += 1
    }
    if (bitWidth) {
      const { value, byteLength } = readRleBitPackedHybrid(
        dataView, offset, bitWidth, dataView.byteLength - offset, nValues
      )
      offset += byteLength
      values = Array.isArray(value) ? value : Array.from(value)
    } else {
      // nval zeros
      values = new Array(nValues).fill(0)
    }
  } else {
    throw new Error(`parquet unsupported encoding: ${daph.encoding}`)
  }

  return { definitionLevels, repetitionLevels, value: values }
}

/**
 * Read a page containing dictionary data.
 *
 * @param {Uint8Array} bytes raw page data
 * @param {DictionaryPageHeader} diph dictionary page header
 * @param {SchemaElement[]} schema schema for the file
 * @param {ColumnMetaData} columnMetadata metadata for the column
 * @returns {ArrayLike<any>} array of values
 */
export function readDictionaryPage(bytes, diph, schema, columnMetadata) {
  const dataView = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  return readPlain(dataView, columnMetadata.type, diph.num_values, 0, false).value
}

/**
 * Read the repetition levels from this page, if any.
 *
 * @param {DataView} dataView data view for the page
 * @param {number} offset offset to start reading from
 * @param {DataPageHeader} daph data page header
 * @param {SchemaElement[]} schema schema for the file
 * @param {ColumnMetaData} columnMetadata metadata for the column
 * @returns {Decoded<any[]>} repetition levels and number of bytes read
 */
function readRepetitionLevels(dataView, offset, daph, schema, columnMetadata) {
  if (columnMetadata.path_in_schema.length > 1) {
    const maxRepetitionLevel = getMaxRepetitionLevel(schema, columnMetadata.path_in_schema)
    if (maxRepetitionLevel) {
      const bitWidth = widthFromMaxInt(maxRepetitionLevel)
      return readData(
        dataView, daph.repetition_level_encoding, offset, daph.num_values, bitWidth
      )
    }
  }
  return { value: [], byteLength: 0 }
}

/**
 * Read the definition levels from this page, if any.
 *
 * @param {DataView} dataView data view for the page
 * @param {number} offset offset to start reading from
 * @param {DataPageHeader} daph data page header
 * @param {SchemaElement[]} schema schema for the file
 * @param {string[]} path_in_schema path in the schema
 * @returns {DefinitionLevels} definition levels and number of bytes read
 */
function readDefinitionLevels(dataView, offset, daph, schema, path_in_schema) {
  if (!isRequired(schema, path_in_schema)) {
    const maxDefinitionLevel = getMaxDefinitionLevel(schema, path_in_schema)
    const bitWidth = widthFromMaxInt(maxDefinitionLevel)
    if (bitWidth) {
      // num_values is index 1 for either type of page header
      const { value: definitionLevels, byteLength } = readData(
        dataView, daph.definition_level_encoding, offset, daph.num_values, bitWidth
      )

      // count nulls
      let numNulls = daph.num_values
      for (const def of definitionLevels) {
        if (def === maxDefinitionLevel) numNulls--
      }
      if (numNulls === 0) {
        definitionLevels.length = 0
      }

      return { byteLength, definitionLevels, numNulls }
    }
  }
  return { byteLength: 0, definitionLevels: [], numNulls: 0 }
}

/**
 * Dremel-assembly of arrays of values into lists
 *
 * @param {number[] | undefined} definitionLevels definition levels, max 3
 * @param {number[]} repetitionLevels repetition levels, max 1
 * @param {ArrayLike<any>} value values to process
 * @param {boolean} isNull can an entry be null?
 * @param {boolean} nullValue can list elements be null?
 * @param {number} maxDefinitionLevel definition level that corresponds to non-null
 * @param {number} prevIndex 1 + index where the last row in the previous page was inserted (0 if first page)
 * @returns {any[]} array of values
 */
export function assembleObjects(
  definitionLevels, repetitionLevels, value, isNull, nullValue, maxDefinitionLevel, prevIndex
) {
  let vali = 0
  let started = false
  let haveNull = false
  let i = prevIndex
  let part = []
  /** @type {any[]} */
  const assign = []

  for (let counter = 0; counter < repetitionLevels.length; counter++) {
    const def = definitionLevels?.length ? definitionLevels[counter] : maxDefinitionLevel
    const rep = repetitionLevels[counter]

    if (!rep) {
      // new row - save what we have
      if (started) {
        assign[i] = haveNull ? undefined : part
        part = []
        i++
      } else {
        // first time: no row to save yet, unless it's a row continued from previous page
        if (vali > 0) {
          assign[i - 1] = assign[i - 1]?.concat(part) // add items to previous row
          part = []
          // don't increment i since we only filled i-1
        }
        started = true
      }
    }

    if (def === maxDefinitionLevel) {
      // append real value to current item
      part.push(value[vali])
      vali++
    } else if (def > 0) {
      // append null to current item
      part.push(undefined)
    }

    haveNull = def === 0 && isNull
  }

  if (started) {
    assign[i] = haveNull ? undefined : part
  } else if (vali > 0) {
    assign[i - 1] = assign[i - 1]?.concat(part)
  }

  return assign
}
