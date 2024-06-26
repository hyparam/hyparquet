import { assembleLists } from './assemble.js'
import { convertWithDictionary } from './convert.js'
import { decompressPage, readDataPage, readDictionaryPage } from './datapage.js'
import { readDataPageV2 } from './datapageV2.js'
import { parquetHeader } from './header.js'
import { getMaxDefinitionLevel } from './schema.js'
import { concat } from './utils.js'

/**
 * Parse column data from a buffer.
 *
 * @typedef {import('./types.js').ColumnMetaData} ColumnMetaData
 * @typedef {import('./types.js').DecodedArray} DecodedArray
 * @param {import('./types.js').DataReader} reader
 * @param {number} rowLimit maximum number of rows to read
 * @param {ColumnMetaData} columnMetadata column metadata
 * @param {import('./types.js').SchemaTree[]} schemaPath schema path for the column
 * @param {import('./hyparquet.js').ParquetReadOptions} options read options
 * @returns {any[]} array of values
 */
export function readColumn(reader, rowLimit, columnMetadata, schemaPath, { compressors, utf8 }) {
  const { element } = schemaPath[schemaPath.length - 1]
  /** @type {DecodedArray | undefined} */
  let dictionary = undefined
  /** @type {any[]} */
  const rowData = []

  while (rowData.length < rowLimit) {
    // parse column header
    const header = parquetHeader(reader)
    // assert(header.compressed_page_size !== undefined)

    // read compressed_page_size bytes starting at offset
    const compressedBytes = new Uint8Array(
      reader.view.buffer, reader.view.byteOffset + reader.offset, header.compressed_page_size
    )

    // parse page data by type
    /** @type {DecodedArray} */
    let values
    if (header.type === 'DATA_PAGE') {
      const daph = header.data_page_header
      if (!daph) throw new Error('parquet data page header is undefined')

      const page = decompressPage(compressedBytes, Number(header.uncompressed_page_size), columnMetadata.codec, compressors)
      const { definitionLevels, repetitionLevels, dataPage } = readDataPage(page, daph, schemaPath, columnMetadata)
      // assert(!daph.statistics?.null_count || daph.statistics.null_count === BigInt(daph.num_values - dataPage.length))

      // convert types, dereference dictionary, and assemble lists
      values = convertWithDictionary(dataPage, dictionary, element, daph.encoding, utf8)
      if (repetitionLevels.length || definitionLevels?.length) {
        const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
        const repetitionPath = schemaPath.map(({ element }) => element.repetition_type)
        assembleLists(
          rowData, definitionLevels, repetitionLevels, values, repetitionPath, maxDefinitionLevel
        )
      } else {
        // wrap nested flat data by depth
        for (let i = 2; i < schemaPath.length; i++) {
          if (schemaPath[i].element.repetition_type !== 'REQUIRED') {
            values = Array.from(values, e => [e])
          }
        }
        concat(rowData, values)
      }
    } else if (header.type === 'DATA_PAGE_V2') {
      const daph2 = header.data_page_header_v2
      if (!daph2) throw new Error('parquet data page header v2 is undefined')

      const { definitionLevels, repetitionLevels, dataPage } = readDataPageV2(
        compressedBytes, header, schemaPath, columnMetadata, compressors
      )

      // convert types, dereference dictionary, and assemble lists
      values = convertWithDictionary(dataPage, dictionary, element, daph2.encoding, utf8)
      if (repetitionLevels.length || definitionLevels?.length) {
        const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
        const repetitionPath = schemaPath.map(({ element }) => element.repetition_type)
        assembleLists(
          rowData, definitionLevels, repetitionLevels, values, repetitionPath, maxDefinitionLevel
        )
      } else {
        concat(rowData, values)
      }
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
  if (rowData.length < rowLimit) {
    throw new Error(`parquet row data length ${rowData.length} does not match row group limit ${rowLimit}}`)
  }
  if (rowData.length > rowLimit) {
    rowData.length = rowLimit // truncate to row limit
  }
  return rowData
}

/**
 * Find the start byte offset for a column chunk.
 *
 * @param {ColumnMetaData} columnMetadata
 * @returns {[bigint, bigint]} byte offset range
 */
export function getColumnRange({ dictionary_page_offset, data_page_offset, total_compressed_size }) {
  let columnOffset = dictionary_page_offset
  if (!columnOffset || data_page_offset < columnOffset) {
    columnOffset = data_page_offset
  }
  return [columnOffset, columnOffset + total_compressed_size]
}
