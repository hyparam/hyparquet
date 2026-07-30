/**
 * @import {AsyncColumn, AsyncRowGroup, ChunkPlan, ColumnDecoder, DecodedArray, GroupPlan, PageLocation, ParquetParsers, ParquetReadOptions, QueryPlan, SchemaTree} from '../src/types.js'
 */

import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { DEFAULT_PARSERS } from './convert.js'
import { readOffsetIndex } from './indexes.js'
import { getSchemaPath } from './schema.js'
import { flatten } from './utils.js'

/**
 * Read a row group from a file-like object.
 *
 * @param {ParquetReadOptions} options
 * @param {QueryPlan} plan
 * @param {GroupPlan} groupPlan
 * @returns {AsyncRowGroup} resolves to column data
 */
export function readRowGroup(options, { metadata }, groupPlan) {
  /** @type {AsyncColumn[]} */
  const asyncColumns = []

  // read column data
  for (const chunk of groupPlan.chunks) {
    const { path_in_schema: pathInSchema } = chunk.columnMetadata
    const schemaPath = getSchemaPath(metadata.schema, pathInSchema)
    const columnDecoder = {
      pathInSchema,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      parsers: { ...DEFAULT_PARSERS, ...options.parsers },
      ...options,
      ...chunk.columnMetadata,
    }
    const { startByte, endByte } = chunk.range

    if ('pageLocations' in chunk) {
      // page locations already parsed from the offset index
      asyncColumns.push({
        pathInSchema,
        data: readSelectedPages(options, groupPlan, chunk, chunk.pageLocations, columnDecoder),
      })
    } else if ('offsetIndex' in chunk) {
      asyncColumns.push({
        pathInSchema,
        // fetch offset index
        data: Promise.resolve(options.file.slice(chunk.offsetIndex.startByte, chunk.offsetIndex.endByte))
          .then(arrayBuffer => {
            const pages = readOffsetIndex({ view: new DataView(arrayBuffer), offset: 0 }).page_locations
            return readSelectedPages(options, groupPlan, chunk, pages, columnDecoder)
          }),
      })
    } else {
      // full column chunk
      asyncColumns.push({
        pathInSchema,
        data: Promise.resolve(options.file.slice(startByte, endByte))
          .then(buffer => {
            const reader = { view: new DataView(buffer), offset: 0 }
            return readColumn(reader, groupPlan, columnDecoder, options.onPage)
          }),
      })
    }
  }

  return {
    groupStart: groupPlan.groupStart,
    groupRows: groupPlan.groupRows,
    selectStart: groupPlan.selectStart,
    selectEnd: groupPlan.selectEnd,
    asyncColumns,
  }
}

/**
 * Read only the pages of a column chunk that overlap the group plan's select
 * range [selectStart, selectEnd), using page locations from the offset index.
 *
 * @param {ParquetReadOptions} options
 * @param {GroupPlan} groupPlan
 * @param {ChunkPlan} chunk
 * @param {PageLocation[]} pages
 * @param {ColumnDecoder} columnDecoder
 * @returns {Promise<{data: DecodedArray[], skipped: number}>}
 */
async function readSelectedPages(options, groupPlan, chunk, pages, columnDecoder) {
  const { data_page_offset, dictionary_page_offset } = chunk.columnMetadata
  const { selectStart, selectEnd } = groupPlan
  let { startByte, endByte } = chunk.range
  let skipped = -1
  // include dictionary if present, handle polars missing dictionary_page_offset
  const hasDict = dictionary_page_offset || data_page_offset < pages[0].offset
  for (let i = 0; i < pages.length; i++) {
    const page = pages[i]
    const pageStart = Number(page.first_row_index)
    const pageEnd = i + 1 < pages.length
      ? Number(pages[i + 1].first_row_index)
      : groupPlan.groupRows // last page extends to end of row group
    // check if page overlaps with [selectStart, selectEnd)
    if (skipped < 0 && pageEnd > selectStart) {
      startByte = Number(page.offset)
      skipped = pageStart
    }
    if (pageStart < selectEnd) {
      endByte = Number(page.offset) + page.compressed_page_size
    }
  }
  if (skipped < 0) skipped = 0
  /** @type {DataView} */
  let view
  if (hasDict && skipped) {
    // fetch the dictionary page separately from the selected data pages so
    // the skipped leading pages are not transferred
    const dictLength = Number(pages[0].offset) - chunk.range.startByte
    const [dictBuffer, dataBuffer] = await Promise.all([
      options.file.slice(chunk.range.startByte, Number(pages[0].offset)),
      options.file.slice(startByte, endByte),
    ])
    // clamp in case the AsyncBuffer returned more bytes than requested
    const combined = new Uint8Array(dictLength + dataBuffer.byteLength)
    combined.set(new Uint8Array(dictBuffer, 0, dictLength))
    combined.set(new Uint8Array(dataBuffer), dictLength)
    view = new DataView(combined.buffer)
  } else if (hasDict) {
    // dictionary page is contiguous with the first selected page
    view = new DataView(await options.file.slice(chunk.range.startByte, endByte))
  } else {
    view = new DataView(await options.file.slice(startByte, endByte))
  }
  const reader = { view, offset: 0 }
  // adjust row selection for skipped pages
  const adjustedGroupPlan = skipped ? {
    ...groupPlan,
    groupStart: groupPlan.groupStart + skipped,
    selectStart: groupPlan.selectStart - skipped,
    selectEnd: groupPlan.selectEnd - skipped,
  } : groupPlan
  const { data, skipped: columnSkipped } = readColumn(reader, adjustedGroupPlan, columnDecoder, options.onPage)
  return {
    data,
    skipped: skipped + columnSkipped,
  }
}

/**
 * @overload
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @param {'object'} rowFormat
 * @returns {Promise<Record<string, any>[]>} resolves to row data
 */
/**
 * @overload
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @param {'array'} [rowFormat]
 * @returns {Promise<any[][]>} resolves to row data
 */
/**
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @param {'object' | 'array'} [rowFormat]
 * @returns {Promise<Record<string, any>[] | any[][]>} resolves to row data
 */
export async function asyncGroupToRows({ asyncColumns }, selectStart, selectEnd, columns, rowFormat) {
  // TODO: do it without flatten
  const asyncPages = await Promise.all(asyncColumns.map(column =>
    column.data.then(({ skipped, data }) => ({ skipped, data: flatten(data) }))
  ))

  // transpose columns into rows
  const selectCount = selectEnd - selectStart
  if (rowFormat === 'object') {
    /** @type {Record<string, any>[]} */
    const groupData = Array(selectCount)
    for (let selectRow = 0; selectRow < selectCount; selectRow++) {
      // return each row as an object
      /** @type {Record<string, any>} */
      const rowData = {}
      for (let i = 0; i < asyncColumns.length; i++) {
        const { data, skipped } = asyncPages[i]
        rowData[asyncColumns[i].pathInSchema[0]] = data[selectStart + selectRow - skipped]
      }
      groupData[selectRow] = rowData
    }
    return groupData
  }

  // careful mapping of column order for rowFormat: array
  const includedColumnNames = asyncColumns
    .map(child => child.pathInSchema[0])
    .filter(name => !columns || columns.includes(name))
  const columnOrder = columns ?? includedColumnNames
  const columnIndexes = columnOrder.map(name => asyncColumns.findIndex(column => column.pathInSchema[0] === name))

  /** @type {any[][]} */
  const groupData = Array(selectCount)
  for (let selectRow = 0; selectRow < selectCount; selectRow++) {
    // return each row as an array
    const rowData = Array(asyncColumns.length)
    for (let i = 0; i < columnOrder.length; i++) {
      const colIdx = columnIndexes[i]
      if (colIdx < 0) throw new Error(`parquet column not found: ${columnOrder[i]}`)
      const { data, skipped } = asyncPages[colIdx]
      rowData[i] = data[selectStart + selectRow - skipped]
    }
    groupData[selectRow] = rowData
  }
  return groupData
}

/**
 * Assemble physical columns into top-level columns asynchronously.
 *
 * @param {AsyncRowGroup} asyncRowGroup
 * @param {SchemaTree} schemaTree
 * @param {ParquetParsers} [parsers]
 * @returns {AsyncRowGroup}
 */
export function assembleAsync(asyncRowGroup, schemaTree, parsers) {
  const { asyncColumns } = asyncRowGroup
  parsers = { ...DEFAULT_PARSERS, ...parsers }
  /** @type {AsyncColumn[]} */
  const assembled = []
  for (const child of schemaTree.children) {
    if (child.children.length) {
      const childColumns = asyncColumns.filter(column => column.pathInSchema[0] === child.element.name)
      if (!childColumns.length) continue

      assembled.push({
        pathInSchema: child.path,
        data: (async () => {
          // collect subcolumn data — Promise.all observes every rejection so
          // a sibling failure cannot leak as an unhandledRejection
          const resolved = await Promise.all(childColumns.map(c => c.data))
          /** @type {Map<string, DecodedArray>} */
          const subcolumnData = new Map()
          const flattened = resolved.map(({ data }) => flatten(data))
          const skipped = Math.max(...resolved.map(result => result.skipped))
          const end = Math.min(...resolved.map((result, i) => result.skipped + flattened[i].length))
          for (let i = 0; i < childColumns.length; i++) {
            // Offset-index reads may start each physical child at a different
            // page boundary. Align them to their common absolute row range.
            const start = skipped - resolved[i].skipped
            const length = Math.max(0, end - skipped)
            subcolumnData.set(
              childColumns[i].pathInSchema.join('.'),
              flattened[i].slice(start, start + length)
            )
          }
          // assemble the column
          assembleNested(subcolumnData, child, parsers)
          const assembled = subcolumnData.get(child.element.name)
          if (!assembled) throw new Error('parquet column data not assembled')
          return { data: [assembled], skipped }
        })(),
      })
    } else {
      // leaf node, return the column
      const asyncColumn = asyncColumns.find(column => column.pathInSchema[0] === child.element.name)
      if (asyncColumn) assembled.push(asyncColumn)
    }
  }
  return { ...asyncRowGroup, asyncColumns: assembled }
}
