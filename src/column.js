import { Encoding, PageType } from './constants.js'
import { assembleObjects, readDataPage, readDictionaryPage } from './datapage.js'
import { parquetHeader } from './header.js'
import { getMaxDefinitionLevel, isRequired, schemaElement } from './schema.js'
import { snappyUncompress } from './snappy.js'

/**
 * @typedef {import('./types.js').ArrayBufferLike} ArrayBufferLike
 * @typedef {import('./types.js').SchemaElement} SchemaElement
 * @typedef {import('./types.js').ColumnMetaData} ColumnMetaData
 * @typedef {import('./types.js').RowGroup} RowGroup
 */

const dayMillis = 86400000000000 // 1 day in milliseconds

/**
 * Shared expanding buffer for snappy decompression.
 * @type {Uint8Array | undefined}
 */
let sharedBuffer

/**
 * Read a column from the file.
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @param {number} columnOffset offset to start reading from
 * @param {RowGroup} rowGroup row group metadata
 * @param {ColumnMetaData} columnMetadata column metadata
 * @param {SchemaElement[]} schema schema for the file
 * @returns {ArrayLike<any>} array of values
 */
export function readColumn(arrayBuffer, columnOffset, rowGroup, columnMetadata, schema) {
  // parse column data
  let valuesSeen = 0
  let byteOffset = 0 // byteOffset within the column
  /** @type {ArrayLike<any> | undefined} */
  let dictionary = undefined
  const rowIndex = [0] // map/list object index
  const rowData = []
  while (valuesSeen < rowGroup.num_rows) {
    // parse column header
    const { value: header, byteLength: headerLength } = parquetHeader(arrayBuffer, columnOffset + byteOffset)
    byteOffset += headerLength
    if (header.compressed_page_size === undefined) {
      throw new Error(`parquet compressed page size is undefined in column '${columnMetadata.path_in_schema}'`)
    }

    // read compressed_page_size bytes starting at offset
    const compressedBytes = new Uint8Array(arrayBuffer).subarray(
      columnOffset + byteOffset,
      columnOffset + byteOffset + header.compressed_page_size
    )
    // decompress bytes
    /** @type {Uint8Array | undefined} */
    let page
    const uncompressed_page_size = Number(header.uncompressed_page_size)
    const { codec } = columnMetadata
    if (codec === 'UNCOMPRESSED') {
      page = compressedBytes
    } else if (codec === 'SNAPPY') {
      if (!sharedBuffer || sharedBuffer.byteLength < uncompressed_page_size) {
        // expand shared buffer
        sharedBuffer = new Uint8Array(uncompressed_page_size)
      }
      page = sharedBuffer
      snappyUncompress(compressedBytes, page)
    } else {
      throw new Error(`parquet unsupported compression codec: ${codec}`)
    }
    // if (page?.length !== uncompressed_page_size) {
    //   throw new Error(`parquet decompressed page length ${page?.length} does not match header ${uncompressed_page_size}`)
    // }

    // parse page data by type
    if (header.type === PageType.DATA_PAGE) {
      const daph = header.data_page_header
      if (!daph) throw new Error('parquet data page header is undefined')

      const { definitionLevels, repetitionLevels, value: dataPage } = readDataPage(page, daph, schema, columnMetadata)
      valuesSeen += daph.num_values

      const dictionaryEncoding = daph.encoding === Encoding.PLAIN_DICTIONARY || daph.encoding === Encoding.RLE_DICTIONARY

      // construct output values: skip nulls and construct lists
      let values
      if (repetitionLevels.length) {
        // Use repetition levels to construct lists
        if (dictionaryEncoding && dictionary !== undefined && Array.isArray(dataPage)) {
          // dereference dictionary values
          for (let i = 0; i < dataPage.length; i++) {
            dataPage[i] = dictionary[dataPage[i]]
          }
        }
        const isNull = columnMetadata && !isRequired(schema, [columnMetadata.path_in_schema[0]])
        const nullValue = false // TODO: unused?
        const maxDefinitionLevel = getMaxDefinitionLevel(schema, columnMetadata.path_in_schema)
        values = assembleObjects(definitionLevels, repetitionLevels, dataPage, isNull, nullValue, maxDefinitionLevel, rowIndex[0])
      } else if (definitionLevels?.length) {
        const maxDefinitionLevel = getMaxDefinitionLevel(schema, columnMetadata.path_in_schema)
        // Use definition levels to skip nulls
        let index = 0
        values = []
        const decoder = new TextDecoder()
        for (let i = 0; i < definitionLevels.length; i++) {
          if (definitionLevels[i] === maxDefinitionLevel) {
            if (index > dataPage.length) {
              throw new Error(`parquet index ${index} exceeds data page length ${dataPage.length}`)
            }
            let v = dataPage[index++]
            // map to dictionary value
            if (dictionary) {
              v = dictionary[v]
              if (v instanceof Uint8Array) {
                try {
                  v = decoder.decode(v)
                } catch (e) {
                  console.warn('parquet failed to decode byte array as string', e)
                }
              }
            }
            values[i] = v
          } else {
            values[i] = undefined
          }
        }
      } else {
        if (dictionaryEncoding && dictionary !== undefined && Array.isArray(dataPage)) {
          // dereference dictionary values
          values = []
          for (let i = 0; i < dataPage.length; i++) {
            values[i] = dictionary[dataPage[i]]
          }
          values = convert(values, schemaElement(schema, columnMetadata.path_in_schema))
        } else if (Array.isArray(dataPage)) {
          // convert primitive types to rich types
          values = convert(dataPage, schemaElement(schema, columnMetadata.path_in_schema))
        } else {
          values = dataPage // TODO: data page shouldn't be a fixed byte array?
        }
      }

      // TODO: check that we are at the end of the page
      // values.length !== daph.num_values isn't right. In cases like arrays,
      // you need the total number of children, not the number of top-level values.

      rowData.push(...Array.from(values))
    } else if (header.type === PageType.DICTIONARY_PAGE) {
      const diph = header.dictionary_page_header
      if (!diph) throw new Error('parquet dictionary page header is undefined')

      dictionary = readDictionaryPage(page, diph, schema, columnMetadata)
    } else if (header.type === PageType.DATA_PAGE_V2) {
      throw new Error('parquet data page v2 not supported')
    } else {
      throw new Error(`parquet unsupported page type: ${header.type}`)
    }
    byteOffset += header.compressed_page_size
  }
  if (rowData.length !== Number(rowGroup.num_rows)) {
    throw new Error(`parquet column length ${rowData.length} does not match row group length ${rowGroup.num_rows}}`)
  }
  return rowData
}

/**
 * Find the start byte offset for a column chunk.
 *
 * @param {ColumnMetaData} columnMetadata column metadata
 * @returns {number} byte offset
 */
export function getColumnOffset(columnMetadata) {
  const { dictionary_page_offset, data_page_offset } = columnMetadata
  let columnOffset = dictionary_page_offset
  if (dictionary_page_offset === undefined || data_page_offset < dictionary_page_offset) {
    columnOffset = data_page_offset
  }
  return Number(columnOffset)
}

/**
 * Convert known types from primitive to rich.
 *
 * @param {any[]} data series of primitive types
 * @param {SchemaElement} schemaElement schema element for the data
 * @returns {any[]} series of rich types
 */
function convert(data, schemaElement) {
  const ctype = schemaElement.converted_type
  if (ctype === undefined) return data
  if (ctype === 'UTF8') {
    const decoder = new TextDecoder()
    return data.map(v => decoder.decode(v))
  }
  if (ctype === 'DECIMAL') {
    const scaleFactor = Math.pow(10, schemaElement.scale || 0)
    if (typeof data[0] === 'number') {
      return scaleFactor === 1 ? data : data.map(v => v * scaleFactor)
    } else if (typeof data[0] === 'bigint') {
      return scaleFactor === 1 ? data : data.map(v => Number(v) * scaleFactor)
    } else {
      return data.map(v => parseDecimal(v) * scaleFactor)
    }
  }
  if (ctype === 'DATE') {
    return data.map(v => new Date(v * dayMillis))
  }
  if (ctype === 'TIME_MILLIS') {
    return data.map(v => new Date(v))
  }
  if (ctype === 'JSON') {
    return data.map(v => JSON.parse(v))
  }
  if (ctype === 'BSON') {
    throw new Error('parquet bson not supported')
  }
  if (ctype === 'INTERVAL') {
    throw new Error('parquet interval not supported')
  }
  return data
}

/**
 * Parse decimal from byte array.
 *
 * @param {Uint8Array} bytes
 * @returns {number}
 */
function parseDecimal(bytes) {
  // TODO: handle signed
  let value = 0
  for (const byte of bytes) {
    value = value << 8 | byte
  }
  return value
}
