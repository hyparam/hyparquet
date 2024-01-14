import { assembleObjects, readDataPage, readDictionaryPage } from './datapage.js'
import { parquetHeader } from './header.js'
import { getMaxDefinitionLevel, isRequired } from './schema.js'
import { snappyUncompress } from './snappy.js'
import { CompressionCodec, Encoding, PageType } from './types.js'

/**
 * @typedef {import('./types.js').ArrayBufferLike} ArrayBufferLike
 * @typedef {import('./types.js').SchemaElement} SchemaElement
 * @typedef {import('./types.js').ColumnMetaData} ColumnMetaData
 * @typedef {import('./types.js').RowGroup} RowGroup
 */

/**
 * Read a column from the file.
 *
 * @param {ArrayBufferLike} arrayBuffer parquet file contents
 * @param {RowGroup} rowGroup row group metadata
 * @param {ColumnMetaData} columnMetadata column metadata
 * @param {SchemaElement[]} schema schema for the file
 * @returns {ArrayLike<any>} array of values
 */
export function readColumn(arrayBuffer, rowGroup, columnMetadata, schema) {
  // find start of column data
  const columnOffset = getColumnOffset(columnMetadata)

  // parse column data
  let valuesSeen = 0
  let byteOffset = 0 // byteOffset within the column
  let dictionary = undefined
  const rowIndex = [0] // map/list object index
  const rowData = []
  while (valuesSeen < rowGroup.num_rows) {
    // parse column header
    const { value: header, byteLength: headerLength } = parquetHeader(arrayBuffer, columnOffset + byteOffset)
    byteOffset += headerLength
    if (!header || header.compressed_page_size === undefined) throw new Error('parquet header is undefined')

    // read compressed_page_size bytes starting at offset
    const compressedBytes = new Uint8Array(arrayBuffer.slice(
      columnOffset + byteOffset,
      columnOffset + byteOffset + header.compressed_page_size
    ))
    // decompress bytes
    let page
    const uncompressed_page_size = Number(header.uncompressed_page_size)
    const { codec } = columnMetadata
    if (codec === CompressionCodec.GZIP) {
      throw new Error('parquet gzip compression not supported')
    } else if (codec === CompressionCodec.SNAPPY) {
      page = new Uint8Array(uncompressed_page_size)
      snappyUncompress(compressedBytes, page)
    } else if (codec === CompressionCodec.LZO) {
      throw new Error('parquet lzo compression not supported')
    }
    if (!page || page.length !== uncompressed_page_size) {
      throw new Error('parquet decompressed page size does not match header')
    }

    // parse page data by type
    if (header.type === PageType.DATA_PAGE) {
      const daph = header.data_page_header
      if (!daph) throw new Error('parquet data page header is undefined')

      const { definitionLevels, repetitionLevels, value } = readDataPage(page, daph, schema, columnMetadata)
      valuesSeen += daph.num_values

      // construct output values: skip nulls and construct lists
      let values
      if (repetitionLevels.length) {
        // Use repetition levels to construct lists
        if ([Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY].includes(daph.encoding)) {
          // TODO: dereference dictionary values
        }
        const isNull = columnMetadata && !isRequired(schema, [columnMetadata.path_in_schema[0]])
        const nullValue = false // TODO: unused?
        const maxDefinitionLevel = getMaxDefinitionLevel(schema, columnMetadata.path_in_schema)
        values = assembleObjects(definitionLevels, repetitionLevels, value, isNull, nullValue, maxDefinitionLevel, rowIndex[0])
      } else if (definitionLevels) {
        const maxDefinitionLevel = getMaxDefinitionLevel(schema, columnMetadata.path_in_schema)
        // Use definition levels to skip nulls
        let index = 0
        values = []
        const decoder = new TextDecoder()
        for (let i = 0; i < definitionLevels.length; i++) {
          if (definitionLevels[i] === maxDefinitionLevel) {
            if (index > value.length) throw new Error('parquet index out of bounds')
            let v = value[index++]
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
        // TODO: use dictionary
        values = value
      }

      // check that we are at the end of the page
      if (values.length !== daph.num_values) {
        throw new Error('parquet column length does not match page header')
      }
      rowData.push(...Array.from(values))
    } else if (header.type === PageType.DICTIONARY_PAGE) {
      const diph = header.dictionary_page_header
      if (!diph) throw new Error('parquet dictionary page header is undefined')

      dictionary = readDictionaryPage(page, diph, schema, columnMetadata)
    } else {
      throw new Error(`parquet unsupported page type: ${header.type}`)
    }
    byteOffset += header.compressed_page_size
  }
  if (rowData.length !== Number(rowGroup.num_rows)) {
    throw new Error('parquet column length does not match row group length')
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
