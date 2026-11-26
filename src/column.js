/**
 * @import {AsyncPages, ColumnDecoder, DataReader, DecodedArray, Levels, PageHeader, PageResult, RowGroupSelect, SchemaTree, Statistics, SubColumnData} from '../src/types.js'
 */

import { assembleColumn } from './assemble.js'
import { Encodings, PageTypes } from './constants.js'
import { convert, convertWithDictionary } from './convert.js'
import { decompressPage, readDataPage, readDataPageV2 } from './datapage.js'
import { readPlain } from './plain.js'
import { getMaxDefinitionLevel, isFlatColumn } from './schema.js'
import { deserializeTCompactProtocol } from './thrift.js'

/**
 * Parse column data from a buffer.
 *
 * Flat columns return rows. Nested columns return levels and non-null
 * values, to be assembled with the other leaves of the column.
 *
 * @param {DataReader} reader
 * @param {RowGroupSelect} rowGroupSelect row group selection
 * @param {ColumnDecoder} columnDecoder column decoder params
 * @param {(chunk: SubColumnData) => void} [onPage] callback for each page
 * @returns {AsyncPages}
 */
export function readColumn(reader, { groupStart, selectStart, selectEnd }, columnDecoder, onPage) {
  const { pathInSchema, schemaPath } = columnDecoder
  const isFlat = isFlatColumn(schemaPath)
  /** @type {DecodedArray[]} */
  const chunks = []
  /** @type {Levels[]} */
  const definitionLevels = []
  /** @type {Levels[]} */
  const repetitionLevels = []
  // Only a repeated v1 row can continue onto the next page.
  const maxDefinition = onPage && getMaxDefinitionLevel(schemaPath)
  /** @type {DecodedArray[]} */
  let pendingValues = []
  /** @type {Levels[]} */
  let pendingDefs = []
  /** @type {Levels[]} */
  let pendingReps = []
  let emitted = 0
  /** @param {number} end */
  function emit(end) {
    if (!onPage || end === emitted) return
    const part = {
      data: pendingValues,
      skipped: emitted,
      definitionLevels: concatLevels(pendingDefs),
      repetitionLevels: concatLevels(pendingReps),
    }
    const columns = new Map([[pathInSchema.join('.'), part]])
    const columnData = assembleColumn(schemaPath[1], columns, columnDecoder.parsers, emitted, end, schemaPath)
    onPage({ pathInSchema, columnData, rowStart: groupStart + emitted, rowEnd: groupStart + end })
    emitted = end
    pendingValues = []
    pendingDefs = []
    pendingReps = []
  }
  /** @type {DecodedArray | undefined} */
  let dictionary = undefined
  let rowCount = 0
  let skipped = 0

  while (isFlat ? rowCount < selectEnd : reader.offset < reader.view.byteLength - 1) {
    if (reader.offset >= reader.view.byteLength - 1) break // end of reader

    // read page header
    const header = parquetHeader(reader)
    if (header.type === 'DICTIONARY_PAGE') {
      const { data } = readPage(reader, header, columnDecoder, dictionary, 0)
      if (data) dictionary = convert(data, columnDecoder)
      continue
    }
    const page = readPage(reader, header, columnDecoder, dictionary, selectStart - rowCount)
    if (page.skipped) {
      // skipped page - just advance row count, don't add to chunks
      if (!chunks.length) {
        skipped += page.skipped
        emitted = skipped
      }
      rowCount += page.skipped
    } else if (isFlat && page.data?.length) {
      chunks.push(page.data)
      rowCount += page.data.length
      onPage?.({
        pathInSchema,
        columnData: page.data,
        rowStart: groupStart + rowCount - page.data.length,
        rowEnd: groupStart + rowCount,
      })
    } else if (page.data && page.definitionLevels && page.repetitionLevels) {
      chunks.push(page.data)
      definitionLevels.push(page.definitionLevels)
      repetitionLevels.push(page.repetitionLevels)
      // a row starts at each repetition level zero
      let rows = page.repetitionLevels.length ? 0 : page.definitionLevels.length || page.data.length
      for (let i = 0; i < page.repetitionLevels.length; i++) {
        if (!page.repetitionLevels[i]) rows++
      }
      rowCount += rows
      if (onPage) {
        const { data, definitionLevels: defs, repetitionLevels: reps } = page
        const split = header.type === 'DATA_PAGE' && reader.offset < reader.view.byteLength - 1
          ? reps.lastIndexOf(0) : -1
        if (split >= 0) {
          let valueSplit = data.length
          for (let i = split; i < defs.length; i++) {
            if (defs[i] === maxDefinition) valueSplit--
          }
          pendingValues.push(data.slice(0, valueSplit))
          pendingDefs.push(defs.subarray(0, split))
          pendingReps.push(reps.subarray(0, split))
          emit(rowCount - 1)
          pendingValues.push(data.slice(valueSplit))
          pendingDefs.push(defs.subarray(split))
          pendingReps.push(reps.subarray(split))
        } else {
          pendingValues.push(data)
          pendingDefs.push(defs)
          pendingReps.push(reps)
          if (!reps.length || header.type === 'DATA_PAGE_V2') emit(rowCount)
        }
      }
    }
  }
  if (isFlat) return { data: chunks, skipped }

  emit(rowCount)
  return {
    data: chunks,
    skipped,
    numRows: rowCount - skipped,
    definitionLevels: concatLevels(definitionLevels),
    repetitionLevels: concatLevels(repetitionLevels),
  }
}

/**
 * @param {Levels[]} pages
 * @returns {Levels}
 */
function concatLevels(pages) {
  if (pages.length === 1) return pages[0]
  let length = 0
  for (const page of pages) length += page.length
  const levels = pages[0] instanceof Uint32Array ? new Uint32Array(length) : new Uint8Array(length)
  let offset = 0
  for (const page of pages) {
    levels.set(page, offset)
    offset += page.length
  }
  return levels
}

/**
 * Read a page (data or dictionary) from a buffer.
 *
 * @param {DataReader} reader
 * @param {PageHeader} header
 * @param {ColumnDecoder} columnDecoder
 * @param {DecodedArray | undefined} dictionary
 * @param {number} pageStart skip this many rows in the page
 * @returns {PageResult}
 */
export function readPage(reader, header, columnDecoder, dictionary, pageStart) {
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

    // convert types and dereference dictionary
    const values = convertWithDictionary(dataPage, dictionary, daph.encoding, columnDecoder)
    return pageResult(values, definitionLevels, repetitionLevels, schemaPath)
  } else if (header.type === 'DATA_PAGE_V2') {
    const daph2 = header.data_page_header_v2
    if (!daph2) throw new Error('parquet data page header v2 is undefined')

    // skip unnecessary pages
    if (pageStart > daph2.num_rows) {
      return { skipped: daph2.num_rows }
    }

    const { definitionLevels, repetitionLevels, dataPage } =
      readDataPageV2(compressedBytes, header, columnDecoder)

    // convert types and dereference dictionary
    const values = convertWithDictionary(dataPage, dictionary, daph2.encoding, columnDecoder)
    return pageResult(values, definitionLevels, repetitionLevels, schemaPath)
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
 * Flat columns fill in nulls to return rows.
 * Nested columns keep their levels for assembly.
 *
 * @param {DecodedArray} values non-null values
 * @param {Levels} definitionLevels
 * @param {Levels} repetitionLevels
 * @param {SchemaTree[]} schemaPath
 * @returns {PageResult}
 */
function pageResult(values, definitionLevels, repetitionLevels, schemaPath) {
  if (!isFlatColumn(schemaPath)) return { skipped: 0, data: values, definitionLevels, repetitionLevels }
  if (values.length === definitionLevels.length || !definitionLevels.length) return { skipped: 0, data: values }
  const data = new Array(definitionLevels.length)
  let v = 0
  for (let i = 0; i < data.length; i++) {
    data[i] = definitionLevels[i] ? values[v++] : null
  }
  return { skipped: 0, data }
}

/**
 * Read a page header from a buffer, leaving the reader at the start of the page data.
 *
 * @param {DataReader} reader
 * @returns {PageHeader}
 */
export function parquetHeader(reader) {
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
    statistics: pageStatistics(header.field_5.field_5),
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
    statistics: pageStatistics(header.field_8.field_8),
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

/**
 * @param {any} stats thrift Statistics struct
 * @returns {Statistics | undefined}
 */
function pageStatistics(stats) {
  return stats && {
    max: stats.field_1,
    min: stats.field_2,
    null_count: stats.field_3,
    distinct_count: stats.field_4,
    max_value: stats.field_5,
    min_value: stats.field_6,
    is_max_value_exact: stats.field_7,
    is_min_value_exact: stats.field_8,
  }
}
