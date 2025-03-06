import { assembleLists } from './assemble.js'
import { Encoding, PageType } from './constants.js'
import { convertWithDictionary } from './convert.js'
import { decompressPage, readDataPage, readDataPageV2, readDictionaryPage } from './datapage.js'
import { getMaxDefinitionLevel } from './schema.js'
import { deserializeTCompactProtocol } from './thrift.js'
import { concat } from './utils.js'

/**
 * Parse column data from a buffer.
 *
 * @param {DataReader} reader
 * @param {number | undefined} rowLimit maximum number of rows to read (undefined reads all rows)
 * @param {ColumnMetaData} columnMetadata column metadata
 * @param {SchemaTree[]} schemaPath schema path for the column
 * @param {ParquetReadOptions} options read options
 * @returns {any[]} array of values
 */
export function readColumn(reader, rowLimit, columnMetadata, schemaPath, { compressors, utf8 }) {
  const { element } = schemaPath[schemaPath.length - 1]
  /** @type {DecodedArray | undefined} */
  let dictionary = undefined
  /** @type {any[]} */
  const rowData = []
  const hasRowLimit = rowLimit !== undefined && rowLimit >= 0 && isFinite(rowLimit)

  while (!hasRowLimit || rowData.length < rowLimit) {
    if (reader.offset >= reader.view.byteLength - 1) break // end of reader
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
  if (hasRowLimit) {
    if (rowData.length < rowLimit) {
      throw new Error(`parquet row data length ${rowData.length} does not match row group limit ${rowLimit}}`)
    }
    if (rowData.length > rowLimit) {
      rowData.length = rowLimit // truncate to row limit
    }
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

/**
 * Read parquet header from a buffer.
 *
 * @import {ColumnMetaData, DecodedArray, DataReader, PageHeader, ParquetReadOptions, SchemaTree} from '../src/types.d.ts'
 * @param {DataReader} reader - parquet file reader
 * @returns {PageHeader} metadata object and bytes read
 */
function parquetHeader(reader) {
  const header = deserializeTCompactProtocol(reader)

  // Parse parquet header from thrift data
  const type = PageType[header.field_1]
  const uncompressed_page_size = header.field_2
  const compressed_page_size = header.field_3
  const crc = header.field_4
  const data_page_header = header.field_5 && {
    num_values: header.field_5.field_1,
    encoding: Encoding[header.field_5.field_2],
    definition_level_encoding: Encoding[header.field_5.field_3],
    repetition_level_encoding: Encoding[header.field_5.field_4],
    statistics: header.field_5.field_5 && {
      max: header.field_5.field_5.field_1,
      min: header.field_5.field_5.field_2,
      null_count: header.field_5.field_5.field_3,
      distinct_count: header.field_5.field_5.field_4,
      max_value: header.field_5.field_5.field_5,
      min_value: header.field_5.field_5.field_6,
    },
  }
  const index_page_header = header.field_6
  const dictionary_page_header = header.field_7 && {
    num_values: header.field_7.field_1,
    encoding: Encoding[header.field_7.field_2],
    is_sorted: header.field_7.field_3,
  }
  const data_page_header_v2 = header.field_8 && {
    num_values: header.field_8.field_1,
    num_nulls: header.field_8.field_2,
    num_rows: header.field_8.field_3,
    encoding: Encoding[header.field_8.field_4],
    definition_levels_byte_length: header.field_8.field_5,
    repetition_levels_byte_length: header.field_8.field_6,
    is_compressed: header.field_8.field_7 === undefined ? true : header.field_8.field_7, // default true
    statistics: header.field_8.field_8,
  }

  return {
    type,
    uncompressed_page_size,
    compressed_page_size,
    crc,
    data_page_header,
    index_page_header,
    dictionary_page_header,
    data_page_header_v2,
  }
}
