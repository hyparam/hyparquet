import { assembleObjects } from './assemble.js'
import { convert } from './convert.js'
import { readDataPage, readDictionaryPage } from './datapage.js'
import { readDataPageV2 } from './datapageV2.js'
import { parquetHeader } from './header.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel, isRequired } from './schema.js'
import { snappyUncompress } from './snappy.js'
import { concat } from './utils.js'

/**
 * @typedef {import('./types.js').SchemaTree} SchemaTree
 * @typedef {import('./types.js').ColumnMetaData} ColumnMetaData
 * @typedef {import('./types.js').Compressors} Compressors
 * @typedef {import('./types.js').RowGroup} RowGroup
 */

/**
 * Parse column data from a buffer.
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @param {number} columnOffset offset to start reading from
 * @param {RowGroup} rowGroup row group metadata
 * @param {ColumnMetaData} columnMetadata column metadata
 * @param {SchemaTree[]} schemaPath schema path for the column
 * @param {Compressors} [compressors] custom decompressors
 * @returns {ArrayLike<any>} array of values
 */
export function readColumn(arrayBuffer, columnOffset, rowGroup, columnMetadata, schemaPath, compressors) {
  /** @type {ArrayLike<any> | undefined} */
  let dictionary = undefined
  let valuesSeen = 0
  /** @type {any[]} */
  const rowData = []
  const { element } = schemaPath[schemaPath.length - 1]
  // column reader:
  const reader = { view: new DataView(arrayBuffer, columnOffset), offset: 0 }

  while (valuesSeen < rowGroup.num_rows) {
    // parse column header
    const header = parquetHeader(reader)
    if (header.compressed_page_size === undefined) {
      throw new Error(`parquet compressed page size is undefined in column '${columnMetadata.path_in_schema}'`)
    }

    // read compressed_page_size bytes starting at offset
    const compressedBytes = new Uint8Array(arrayBuffer).subarray(
      columnOffset + reader.offset,
      columnOffset + reader.offset + header.compressed_page_size
    )

    // parse page data by type
    if (header.type === 'DATA_PAGE') {
      const daph = header.data_page_header
      if (!daph) throw new Error('parquet data page header is undefined')

      const page = decompressPage(
        compressedBytes, Number(header.uncompressed_page_size), columnMetadata.codec, compressors
      )
      const { definitionLevels, repetitionLevels, dataPage } = readDataPage(page, daph, schemaPath, columnMetadata)
      valuesSeen += daph.num_values

      const dictionaryEncoding = daph.encoding === 'PLAIN_DICTIONARY' || daph.encoding === 'RLE_DICTIONARY'

      // construct output values: skip nulls and construct lists
      /** @type {DecodedArray} */
      let values
      if (repetitionLevels.length) {
        dereferenceDictionary(dictionary, dataPage)
        // Use repetition levels to construct lists
        const isNullable = columnMetadata && !isRequired(schemaPath.slice(0, 2))
        const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
        const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
        // convert primitive types to rich types
        values = convert(dataPage, element)
        values = assembleObjects(
          definitionLevels, repetitionLevels, values, isNullable, maxDefinitionLevel, maxRepetitionLevel
        )
      } else if (definitionLevels?.length) {
        const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
        // Use definition levels to skip nulls
        values = []
        skipNulls(definitionLevels, maxDefinitionLevel, dataPage, dictionary, values)
      } else {
        if (dictionaryEncoding && dictionary) {
          dereferenceDictionary(dictionary, dataPage)
          values = convert(dataPage, element)
        } else if (Array.isArray(dataPage)) {
          // convert primitive types to rich types
          values = convert(dataPage, element)
        } else {
          values = dataPage // TODO: data page shouldn't be a fixed byte array?
        }
      }

      // TODO: check that we are at the end of the page
      // values.length !== daph.num_values isn't right. In cases like arrays,
      // you need the total number of children, not the number of top-level values.

      concat(rowData, values)
    } else if (header.type === 'DICTIONARY_PAGE') {
      const diph = header.dictionary_page_header
      if (!diph) throw new Error('parquet dictionary page header is undefined')

      const page = decompressPage(
        compressedBytes, Number(header.uncompressed_page_size), columnMetadata.codec, compressors
      )
      dictionary = readDictionaryPage(page, diph, columnMetadata)
    } else if (header.type === 'DATA_PAGE_V2') {
      const daph2 = header.data_page_header_v2
      if (!daph2) throw new Error('parquet data page header v2 is undefined')

      const { definitionLevels, repetitionLevels, dataPage } = readDataPageV2(
        compressedBytes, header, schemaPath, columnMetadata, compressors
      )
      valuesSeen += daph2.num_values

      const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
      const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
      if (repetitionLevels.length) {
        dereferenceDictionary(dictionary, dataPage)
        // Use repetition levels to construct lists
        concat(rowData, assembleObjects(
          definitionLevels, repetitionLevels, dataPage, true, maxDefinitionLevel, maxRepetitionLevel
        ))
      } else if (daph2.num_nulls) {
        // skip nulls
        if (!definitionLevels) throw new Error('parquet data page v2 nulls missing definition levels')
        skipNulls(definitionLevels, maxDefinitionLevel, dataPage, dictionary, rowData)
      } else {
        dereferenceDictionary(dictionary, dataPage)
        concat(rowData, dataPage)
      }
      // TODO: convert?
    } else {
      throw new Error(`parquet unsupported page type: ${header.type}`)
    }
    reader.offset += header.compressed_page_size
  }
  if (rowData.length !== Number(rowGroup.num_rows)) {
    throw new Error(`parquet row data length ${rowData.length} does not match row group length ${rowGroup.num_rows}}`)
  }
  return rowData
}

/**
 * Map data to dictionary values in place.
 *
 * @typedef {import('./types.js').DecodedArray} DecodedArray
 * @param {ArrayLike<any> | undefined} dictionary
 * @param {DecodedArray} dataPage
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
 * @param {ColumnMetaData} columnMetadata
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
 * @param {Uint8Array} compressedBytes
 * @param {number} uncompressed_page_size
 * @param {import('./types.js').CompressionCodec} codec
 * @param {Compressors | undefined} compressors
 * @returns {Uint8Array}
 */
export function decompressPage(compressedBytes, uncompressed_page_size, codec, compressors) {
  /** @type {Uint8Array | undefined} */
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
 * Expand data page list with nulls and convert to utf8.
 *
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
