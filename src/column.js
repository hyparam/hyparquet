import { assembleLists } from './assemble.js'
import { Encodings, PageTypes } from './constants.js'
import { convert, convertWithDictionary } from './convert.js'
import { decompressPage, readDataPage, readDataPageV2 } from './datapage.js'
import { readPlain } from './plain.js'
import { isFlatColumn } from './schema.js'
import { deserializeTCompactProtocol } from './thrift.js'

/**
 * Parse column data from a buffer.
 *
 * @param {DataReader} reader
 * @param {RowGroupSelect} rowGroupSelect row group selection
 * @param {ColumnDecoder} columnDecoder column decoder params
 * @param {(chunk: SubColumnData) => void} [onPage] callback for each page
 * @returns {{ data: DecodedArray[], skipped: number }}
 */
export function readColumn(reader, { groupStart, selectStart, selectEnd }, columnDecoder, onPage) {
  const { pathInSchema, schemaPath } = columnDecoder
  const isFlat = isFlatColumn(schemaPath)
  /** @type {DecodedArray[]} */
  const chunks = []
  /** @type {DecodedArray | undefined} */
  let dictionary = undefined
  /** @type {DecodedArray | undefined} */
  let lastChunk = undefined
  let rowCount = 0
  let skipped = 0

  const emitLastChunk = onPage && (() => {
    lastChunk && onPage({
      pathInSchema,
      columnData: lastChunk,
      rowStart: groupStart + rowCount - lastChunk.length,
      rowEnd: groupStart + rowCount,
    })
  })

  while (isFlat ? rowCount < selectEnd : reader.offset < reader.view.byteLength - 1) {
    if (reader.offset >= reader.view.byteLength - 1) break // end of reader

    // read page header
    const header = parquetHeader(reader)
    if (header.type === 'DICTIONARY_PAGE') {
      const { data } = readPage(reader, header, columnDecoder, dictionary, undefined, 0)
      if (data) dictionary = convert(data, columnDecoder)
    } else {
      const lastChunkLength = lastChunk?.length || 0
      const result = readPage(reader, header, columnDecoder, dictionary, lastChunk, selectStart - rowCount)
      if (result.skipped) {
        // skipped page - just advance row count, don't add to chunks
        if (!chunks.length) {
          skipped += result.skipped
        }
        rowCount += result.skipped
      } else if (result.data && lastChunk === result.data) {
        // continued from previous page
        rowCount += result.data.length - lastChunkLength
      } else if (result.data && result.data.length) {
        emitLastChunk?.()
        chunks.push(result.data)
        rowCount += result.data.length
        lastChunk = result.data
      }
    }
  }
  emitLastChunk?.()

  return { data: chunks, skipped }
}

/**
 * Read a page (data or dictionary) from a buffer.
 *
 * @import {PageResult} from '../src/types.d.ts'
 * @param {DataReader} reader
 * @param {PageHeader} header
 * @param {ColumnDecoder} columnDecoder
 * @param {DecodedArray | undefined} dictionary
 * @param {DecodedArray | undefined} previousChunk
 * @param {number} pageStart skip this many rows in the page
 * @returns {PageResult}
 */
export function readPage(reader, header, columnDecoder, dictionary, previousChunk, pageStart) {
  const { type, element, schemaPath, codec, compressors } = columnDecoder
  // read compressed_page_size bytes
  const compressedBytes = new Uint8Array(
    reader.view.buffer, reader.view.byteOffset + reader.offset, header.compressed_page_size
  )
  reader.offset += header.compressed_page_size

  // parse page data by type
  if (header.type === 'DATA_PAGE') {
    const daph = header.data_page_header
    if (!daph) throw new Error('parquet data page header is undefined')

    // skip unnecessary non-nested pages
    if (pageStart > daph.num_values && isFlatColumn(schemaPath)) {
      return { skipped: daph.num_values }
    }

    const page = decompressPage(compressedBytes, Number(header.uncompressed_page_size), codec, compressors)
    const { definitionLevels, repetitionLevels, dataPage } = readDataPage(page, daph, columnDecoder)
    // assert(!daph.statistics?.null_count || daph.statistics.null_count === BigInt(daph.num_values - dataPage.length))

    // convert types, dereference dictionary, and assemble lists
    const values = convertWithDictionary(dataPage, dictionary, daph.encoding, columnDecoder)
    const output = Array.isArray(previousChunk) ? previousChunk : []
    const assembled = assembleLists(output, definitionLevels, repetitionLevels, values, schemaPath)
    return { skipped: 0, data: assembled }
  } else if (header.type === 'DATA_PAGE_V2') {
    const daph2 = header.data_page_header_v2
    if (!daph2) throw new Error('parquet data page header v2 is undefined')

    // skip unnecessary pages
    if (pageStart > daph2.num_rows) {
      return { skipped: daph2.num_values }
    }

    const { definitionLevels, repetitionLevels, dataPage } =
      readDataPageV2(compressedBytes, header, columnDecoder)

    // convert types, dereference dictionary, and assemble lists
    const values = convertWithDictionary(dataPage, dictionary, daph2.encoding, columnDecoder)
    const output = Array.isArray(previousChunk) ? previousChunk : []
    const assembled = assembleLists(output, definitionLevels, repetitionLevels, values, schemaPath)
    return { skipped: 0, data: assembled }
  } else if (header.type === 'DICTIONARY_PAGE') {
    const diph = header.dictionary_page_header
    if (!diph) throw new Error('parquet dictionary page header is undefined')

    const page = decompressPage(
      compressedBytes, Number(header.uncompressed_page_size), codec, compressors
    )

    const reader = { view: new DataView(page.buffer, page.byteOffset, page.byteLength), offset: 0 }
    const dictArray = readPlain(reader, type, diph.num_values, element.type_length)
    return { skipped: 0, data: dictArray }
  } else {
    throw new Error(`parquet unsupported page type: ${header.type}`)
  }
}

/**
 * Read parquet header from a buffer.
 *
 * @import {ColumnDecoder, DataReader, DecodedArray, PageHeader, RowGroupSelect, SubColumnData} from '../src/types.d.ts'
 * @param {DataReader} reader
 * @returns {PageHeader}
 */
function parquetHeader(reader) {
  const header = deserializeTCompactProtocol(reader)

  // Parse parquet header from thrift data
  const type = PageTypes[header.field_1]
  const uncompressed_page_size = header.field_2
  const compressed_page_size = header.field_3
  const crc = header.field_4
  const data_page_header = header.field_5 && {
    num_values: header.field_5.field_1,
    encoding: Encodings[header.field_5.field_2],
    definition_level_encoding: Encodings[header.field_5.field_3],
    repetition_level_encoding: Encodings[header.field_5.field_4],
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
    encoding: Encodings[header.field_7.field_2],
    is_sorted: header.field_7.field_3,
  }
  const data_page_header_v2 = header.field_8 && {
    num_values: header.field_8.field_1,
    num_nulls: header.field_8.field_2,
    num_rows: header.field_8.field_3,
    encoding: Encodings[header.field_8.field_4],
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
