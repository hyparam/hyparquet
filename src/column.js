import { assembleLists } from './assemble.js'
import { Encodings, PageTypes } from './constants.js'
import { convert, convertWithDictionary } from './convert.js'
import { decompressPage, readDataPage, readDataPageV2, readRepetitionLevels } from './datapage.js'
import { readPlain } from './plain.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel, hasMultiChildAncestor, isFlatColumn } from './schema.js'
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
  // Terminate the loop early by assembled row count for:
  // - flat columns (each value = one row)
  // - nested columns with repetition levels (top-level rows from rep=0 count)
  // Page-skipping is handled separately and disabled for struct children.
  const canTerminateEarly = isFlatColumn(schemaPath) || getMaxRepetitionLevel(schemaPath) > 0
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

  while (canTerminateEarly ? rowCount < selectEnd : reader.offset < reader.view.byteLength - 1) {
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
  // Truncate the last chunk if we overshot selectEnd (can happen when a single
  // page produces more assembled rows than needed, e.g. in nested columns)
  if (canTerminateEarly && rowCount > selectEnd && lastChunk) {
    const excess = rowCount - selectEnd
    chunks[chunks.length - 1] = lastChunk.slice(0, lastChunk.length - excess)
    lastChunk = chunks[chunks.length - 1]
    rowCount = selectEnd
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

    // For nested columns without multi-child ancestors, decode only repetition levels
    // first to check if page can be skipped. Struct children must not skip pages
    // independently since siblings need matching row counts.
    if (getMaxRepetitionLevel(schemaPath) > 0 && !hasMultiChildAncestor(schemaPath)) {
      const repReader = { view: new DataView(page.buffer, page.byteOffset, page.byteLength), offset: 0 }
      const repLevelsOnly = readRepetitionLevels(repReader, daph, schemaPath)
      let topLevelRows = 0
      for (const rep of repLevelsOnly) {
        if (rep === 0) topLevelRows++
      }
      if (pageStart > topLevelRows) {
        return { skipped: topLevelRows }
      }
    }

    const { definitionLevels, repetitionLevels, dataPage } = readDataPage(page, daph, columnDecoder)
    // assert(!daph.statistics?.null_count || daph.statistics.null_count === BigInt(daph.num_values - dataPage.length))

    // convert types, dereference dictionary, and assemble lists
    let values = convertWithDictionary(dataPage, dictionary, daph.encoding, columnDecoder)
    const output = Array.isArray(previousChunk) ? previousChunk : []
    let defLevels = definitionLevels
    let repLevels = repetitionLevels

    // Handle orphaned continuation values from skipped pages
    if (!output.length && repLevels.length && repLevels[0] > 0) {
      const maxDefLevel = getMaxDefinitionLevel(schemaPath)
      let skipIdx = 0
      let valueSkipCount = 0
      while (skipIdx < repLevels.length && (skipIdx === 0 || repLevels[skipIdx] !== 0)) {
        const def = defLevels?.length ? defLevels[skipIdx] : maxDefLevel
        if (def === maxDefLevel) valueSkipCount++
        skipIdx++
      }
      if (skipIdx >= repLevels.length) {
        // Entire page is continuation with no new top-level rows
        return { skipped: 0, data: output }
      }
      defLevels = defLevels?.length ? defLevels.slice(skipIdx) : defLevels
      repLevels = repLevels.slice(skipIdx)
      values = Array.isArray(values) ? values.slice(valueSkipCount) : values.slice(valueSkipCount)
    }

    const assembled = assembleLists(output, defLevels, repLevels, values, schemaPath)
    return { skipped: 0, data: assembled }
  } else if (header.type === 'DATA_PAGE_V2') {
    const daph2 = header.data_page_header_v2
    if (!daph2) throw new Error('parquet data page header v2 is undefined')

    // skip unnecessary pages (struct children must not skip independently)
    if (pageStart > daph2.num_rows && !hasMultiChildAncestor(schemaPath)) {
      return { skipped: daph2.num_rows }
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
