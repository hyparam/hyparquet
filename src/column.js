import { assembleLists } from './assemble.js'
import { convert } from './convert.js'
import { readDataPage, readDictionaryPage } from './datapage.js'
import { readDataPageV2 } from './datapageV2.js'
import { parquetHeader } from './header.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'
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
 * @returns {any[]} array of values
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
      throw new Error('parquet compressed page size is undefined')
    }

    // read compressed_page_size bytes starting at offset
    const compressedBytes = new Uint8Array(
      arrayBuffer, columnOffset + reader.offset, header.compressed_page_size
    )

    // parse page data by type
    /** @type {DecodedArray} */
    let values
    if (header.type === 'DATA_PAGE') {
      const daph = header.data_page_header
      if (!daph) throw new Error('parquet data page header is undefined')

      const page = decompressPage(
        compressedBytes, Number(header.uncompressed_page_size), columnMetadata.codec, compressors
      )
      const { definitionLevels, repetitionLevels, dataPage } = readDataPage(page, daph, schemaPath, columnMetadata)
      valuesSeen += daph.num_values
      // assert(!daph.statistics || daph.statistics.null_count === BigInt(daph.num_values - dataPage.length))

      // construct output values: skip nulls and construct lists
      dereferenceDictionary(dictionary, dataPage)
      values = convert(dataPage, element)
      if (repetitionLevels.length || definitionLevels?.length) {
        // Use repetition levels to construct lists
        const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
        const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
        const repetitionPath = schemaPath.map(({ element }) => element.repetition_type)
        values = assembleLists(
          definitionLevels, repetitionLevels, values, repetitionPath, maxDefinitionLevel, maxRepetitionLevel
        )
      } else {
        // wrap nested flat data by depth
        for (let i = 2; i < schemaPath.length; i++) {
          if (schemaPath[i].element.repetition_type !== 'REQUIRED') {
            values = [values]
          }
        }
      }
      // assert(BigInt(values.length) === rowGroup.num_rows)
      concat(rowData, values)
    } else if (header.type === 'DATA_PAGE_V2') {
      const daph2 = header.data_page_header_v2
      if (!daph2) throw new Error('parquet data page header v2 is undefined')

      const { definitionLevels, repetitionLevels, dataPage } = readDataPageV2(
        compressedBytes, header, schemaPath, columnMetadata, compressors
      )
      valuesSeen += daph2.num_values

      dereferenceDictionary(dictionary, dataPage)
      values = convert(dataPage, element)
      if (repetitionLevels.length || definitionLevels?.length) {
        // Use repetition levels to construct lists
        const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
        const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
        const repetitionPath = schemaPath.map(({ element }) => element.repetition_type)
        values = assembleLists(
          definitionLevels, repetitionLevels, values, repetitionPath, maxDefinitionLevel, maxRepetitionLevel
        )
      }
      concat(rowData, values)
    } else if (header.type === 'DICTIONARY_PAGE') {
      const diph = header.dictionary_page_header
      if (!diph) throw new Error('parquet dictionary page header is undefined')

      const page = decompressPage(
        compressedBytes, Number(header.uncompressed_page_size), columnMetadata.codec, compressors
      )
      dictionary = readDictionaryPage(page, diph, columnMetadata, element.type_length)
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
