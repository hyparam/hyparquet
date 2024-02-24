import { Encoding, PageType } from './constants.js'
import { convert } from './convert.js'
import { assembleObjects, readDataPage, readDictionaryPage } from './datapage.js'
import { readDataPageV2 } from './datapageV2.js'
import { parquetHeader } from './header.js'
import { getMaxDefinitionLevel, isRequired, schemaElement } from './schema.js'
import { snappyUncompress } from './snappy.js'

/**
 * @typedef {import('./types.js').SchemaElement} SchemaElement
 * @typedef {import('./types.js').ColumnMetaData} ColumnMetaData
 * @typedef {import('./types.js').RowGroup} RowGroup
 */

/**
 * Parse column data from a buffer.
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @param {number} columnOffset offset to start reading from
 * @param {RowGroup} rowGroup row group metadata
 * @param {ColumnMetaData} columnMetadata column metadata
 * @param {SchemaElement[]} schema schema for the file
 * @returns {ArrayLike<any>} array of values
 */
export function readColumn(arrayBuffer, columnOffset, rowGroup, columnMetadata, schema) {
  /** @type {ArrayLike<any> | undefined} */
  let dictionary = undefined
  let valuesSeen = 0
  let byteOffset = 0 // byteOffset within the column
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

    // parse page data by type
    if (header.type === PageType.DATA_PAGE) {
      const daph = header.data_page_header
      if (!daph) throw new Error('parquet data page header is undefined')

      const page = decompressPage(compressedBytes, Number(header.uncompressed_page_size), columnMetadata.codec)
      const { definitionLevels, repetitionLevels, value: dataPage } = readDataPage(page, daph, schema, columnMetadata)
      valuesSeen += daph.num_values

      const dictionaryEncoding = daph.encoding === Encoding.PLAIN_DICTIONARY || daph.encoding === Encoding.RLE_DICTIONARY

      // construct output values: skip nulls and construct lists
      /** @type {any[]} */
      let values
      if (repetitionLevels.length) {
        dereferenceDictionary(dictionary, dataPage)
        // Use repetition levels to construct lists
        const isNull = columnMetadata && !isRequired(schema, [columnMetadata.path_in_schema[0]])
        const nullValue = false // TODO: unused?
        const maxDefinitionLevel = getMaxDefinitionLevel(schema, columnMetadata.path_in_schema)
        values = assembleObjects(definitionLevels, repetitionLevels, dataPage, isNull, nullValue, maxDefinitionLevel, rowIndex[0])
      } else if (definitionLevels?.length) {
        const maxDefinitionLevel = getMaxDefinitionLevel(schema, columnMetadata.path_in_schema)
        // Use definition levels to skip nulls
        values = []
        skipNulls(definitionLevels, maxDefinitionLevel, dataPage, dictionary, values)
      } else {
        if (dictionaryEncoding && dictionary) {
          dereferenceDictionary(dictionary, dataPage)
          values = convert(dataPage, schemaElement(schema, columnMetadata.path_in_schema))
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

      const page = decompressPage(
        compressedBytes, Number(header.uncompressed_page_size), columnMetadata.codec
      )
      dictionary = readDictionaryPage(page, diph, schema, columnMetadata)
    } else if (header.type === PageType.DATA_PAGE_V2) {
      const daph2 = header.data_page_header_v2
      if (!daph2) throw new Error('parquet data page header v2 is undefined')

      const { definitionLevels, repetitionLevels, value: dataPage } = readDataPageV2(
        compressedBytes, header, schema, columnMetadata
      )
      valuesSeen += daph2.num_values

      const maxDefinitionLevel = getMaxDefinitionLevel(schema, columnMetadata.path_in_schema)
      if (repetitionLevels.length) {
        dereferenceDictionary(dictionary, dataPage)
        // Use repetition levels to construct lists
        rowData.push(...assembleObjects(
          definitionLevels, repetitionLevels, dataPage, true, false, maxDefinitionLevel, rowIndex[0]
        ))
      } else if (daph2.num_nulls) {
        // skip nulls
        if (!definitionLevels) throw new Error('parquet data page v2 nulls missing definition levels')
        skipNulls(definitionLevels, maxDefinitionLevel, dataPage, dictionary, rowData)
      } else {
        dereferenceDictionary(dictionary, dataPage)
        rowData.push(...dataPage)
      }
      // TODO: convert?
    } else {
      throw new Error(`parquet unsupported page type: ${header.type}`)
    }
    byteOffset += header.compressed_page_size
  }
  if (rowData.length !== Number(rowGroup.num_rows)) {
    throw new Error(`parquet row data length ${rowData.length} does not match row group length ${rowGroup.num_rows}}`)
  }
  return rowData
}

/**
 * Map data to dictionary values in place.
 *
 * @param {ArrayLike<any> | undefined} dictionary
 * @param {number[]} dataPage
 */
function dereferenceDictionary(dictionary, dataPage) {
  if (dictionary) {
    for (let i = 0; i < dataPage.length; i++) {
      dataPage[i] = dictionary[dataPage[i]]
    }
  }
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
 * @typedef {import('./types.js').PageHeader} PageHeader
 * @typedef {import('./types.js').CompressionCodec} CompressionCodec
 * @param {Uint8Array} compressedBytes
 * @param {number} uncompressed_page_size
 * @param {CompressionCodec} codec
 * @returns {Uint8Array}
 */
export function decompressPage(compressedBytes, uncompressed_page_size, codec) {
  /** @type {Uint8Array | undefined} */
  let page
  if (codec === 'UNCOMPRESSED') {
    page = compressedBytes
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
 * Expand data page list with nulls and convert to utf8.
 * @param {number[]} definitionLevels
 * @param {number} maxDefinitionLevel
 * @param {ArrayLike<any>} dataPage
 * @param {any} dictionary
 * @param {any[]} output
 */
function skipNulls(definitionLevels, maxDefinitionLevel, dataPage, dictionary, output) {
  if (output.length) throw new Error('parquet output array is not empty')
  // Use definition levels to skip nulls
  let index = 0
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
      output[i] = v
    } else {
      output[i] = undefined
    }
  }
}
