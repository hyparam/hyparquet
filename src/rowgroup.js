/**
 * @import {AsyncColumn, AsyncRowGroup, DecodedArray, GroupPlan, ParquetParsers, ParquetReadOptions, QueryPlan, SchemaTree} from '../src/types.js'
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
    const { data_page_offset, dictionary_page_offset, path_in_schema: pathInSchema } = chunk.columnMetadata
    const schemaPath = getSchemaPath(metadata.schema, pathInSchema)
    const columnDecoder = {
      pathInSchema,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      parsers: { ...DEFAULT_PARSERS, ...options.parsers },
      ...options,
      ...chunk.columnMetadata,
    }
    let { startByte, endByte } = chunk.range

    // non-offset-index case
    if (!('offsetIndex' in chunk)) {
      asyncColumns.push({
        pathInSchema,
        data: Promise.resolve(options.file.slice(startByte, endByte))
          .then(buffer => {
            const reader = { view: new DataView(buffer), offset: 0 }
            return readColumn(reader, groupPlan, columnDecoder, options.onPage)
          }),
      })
      continue
    }

    // offset-index case
    asyncColumns.push({
      pathInSchema,
      // fetch offset index
      data: Promise.resolve(options.file.slice(chunk.offsetIndex.startByte, chunk.offsetIndex.endByte))
        .then(async arrayBuffer => {
          // use offset index to read only necessary pages
          const { selectStart, selectEnd } = groupPlan
          const pages = readOffsetIndex({ view: new DataView(arrayBuffer), offset: 0 }).page_locations
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
            if (skipped < 0 && !hasDict && pageEnd > selectStart) {
              startByte = Number(page.offset)
              skipped = pageStart
            }
            if (pageStart < selectEnd) {
              endByte = Number(page.offset) + page.compressed_page_size
            }
          }
          if (skipped < 0) skipped = 0
          const buffer = await options.file.slice(startByte, endByte)
          const reader = { view: new DataView(buffer), offset: 0 }
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
        }),
    })
  }

  return { groupStart: groupPlan.groupStart, groupRows: groupPlan.groupRows, asyncColumns }
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
          // collect subcolumn data
          /** @type {Map<string, DecodedArray>} */
          const subcolumnData = new Map()
          let minLength = Infinity
          for (const column of childColumns) {
            const { data } = await column.data
            const flat = flatten(data)
            subcolumnData.set(column.pathInSchema.join('.'), flat)
            minLength = Math.min(minLength, flat.length)
          }
          // trim sub-columns to same length (offset index may read different pages per column)
          for (const [key, value] of subcolumnData) {
            if (value.length > minLength) {
              subcolumnData.set(key, value.slice(0, minLength))
            }
          }
          // assemble the column
          assembleNested(subcolumnData, child, parsers)
          const assembled = subcolumnData.get(child.element.name)
          if (!assembled) throw new Error('parquet column data not assembled')
          return { data: [assembled], skipped: 0 }
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
