import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { DEFAULT_PARSERS } from './convert.js'
import { readOffsetIndex } from './indexes.js'
import { getSchemaPath } from './schema.js'
import { flatten, flattenAsync } from './utils.js'

/**
 * @import {AsyncColumn, AsyncPages, AsyncSubColumn, AsyncRowGroup, DecodedArray, GroupPlan, ParquetParsers, ParquetReadOptions, QueryPlan, ResolvedPages, SchemaTree, ColumnData, SubColumnData} from '../src/types.js'
 */
/**
 * Read a row group from a file-like object.
 *
 * @param {ParquetReadOptions} options
 * @param {QueryPlan} plan
 * @param {GroupPlan} groupPlan
 * @returns {AsyncRowGroup} resolves to column data
 */
export function readRowGroup(options, { metadata }, groupPlan) {
  const { file, compressors, utf8 } = options

  /** @type {AsyncSubColumn[]} */
  const asyncColumns = []
  /** @type {ParquetParsers} */
  const parsers = { ...DEFAULT_PARSERS, ...options.parsers }

  // read column data
  for (const chunkPlan of groupPlan.chunks) {
    const { columnMetadata } = chunkPlan
    const schemaPath = getSchemaPath(metadata.schema, columnMetadata.path_in_schema)
    const columnDecoder = {
      pathInSchema: columnMetadata.path_in_schema,
      type: columnMetadata.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      codec: columnMetadata.codec,
      parsers,
      compressors,
      utf8,
    }

    // non-offset-index case
    if (!('offsetIndex' in chunkPlan)) {
      asyncColumns.push({
        pathInSchema: columnMetadata.path_in_schema,
        data: Promise.resolve(file.slice(chunkPlan.range.startByte, chunkPlan.range.endByte))
          .then(buffer => {
            const reader = { view: new DataView(buffer), offset: 0 }
            return {
              pageSkip: 0,
              data: readColumn(reader, groupPlan, columnDecoder, options.onPage),
            }
          }),
      })
      continue
    }

    // offset-index case
    asyncColumns.push({
      pathInSchema: columnMetadata.path_in_schema,
      // fetch offset index
      data: Promise.resolve(file.slice(chunkPlan.offsetIndex.startByte, chunkPlan.offsetIndex.endByte))
        .then(async arrayBuffer => {
          const offsetIndex = readOffsetIndex({ view: new DataView(arrayBuffer), offset: 0 })
          // use offset index to read only necessary pages
          const { selectStart, selectEnd } = groupPlan
          const pages = offsetIndex.page_locations
          let startByte = NaN
          let endByte = NaN
          let pageSkip = 0
          for (let i = 0; i < pages.length; i++) {
            const page = pages[i]
            const pageStart = Number(page.first_row_index)
            const pageEnd = i + 1 < pages.length
              ? Number(pages[i + 1].first_row_index)
              : groupPlan.groupRows // last page extends to end of row group
            // check if page overlaps with [selectStart, selectEnd)
            if (pageStart < selectEnd && pageEnd > selectStart) {
              if (Number.isNaN(startByte)) {
                startByte = Number(page.offset)
                pageSkip = pageStart
              }
              endByte = Number(page.offset) + page.compressed_page_size
            }
          }
          const buffer = await file.slice(startByte, endByte)
          const reader = { view: new DataView(buffer), offset: 0 }
          // adjust row selection for skipped pages
          const adjustedGroupPlan = pageSkip ? {
            ...groupPlan,
            groupStart: groupPlan.groupStart + pageSkip,
            selectStart: groupPlan.selectStart - pageSkip,
            selectEnd: groupPlan.selectEnd - pageSkip,
          } : groupPlan
          return {
            data: readColumn(reader, adjustedGroupPlan, columnDecoder, options.onPage),
            pageSkip,
          }
        }),
    })
  }

  return { groupStart: groupPlan.groupStart, groupRows: groupPlan.groupRows, asyncColumns }
}

/**
 * Assemble a row group into row objects.
 *
 * @param {object} options
 * @param {AsyncRowGroup} options.rowGroup
 * @param {SchemaTree} options.schemaTree
 * @param {number} options.selectStart
 * @param {number} options.selectEnd
 * @param {string[]} [options.columns]
 * @param {((page: SubColumnData) => void)} [options.onPage]
 * @param {((chunk: ColumnData) => void)} [options.onChunk]
 * @returns {Promise<Record<string, any>[]>}
 */
export async function assembleRows({ rowGroup, schemaTree, selectStart, selectEnd, columns, onPage, onChunk }) {
  const { asyncColumns } = rowGroup

  // Assemble physical columns into top-level logical columns
  /** @type {AsyncColumn[]} */
  const assembled = []
  for (const child of schemaTree.children) {
    if (child.children.length) {
      // nested column (struct, list, map)
      const childColumns = asyncColumns.filter(column => column.pathInSchema[0] === child.element.name)
      if (!childColumns.length) continue

      /** @type {Map<string, DecodedArray>} */
      const flatData = new Map()
      /** @type {Promise<AsyncPages>} */
      const data = Promise.all(childColumns.map(async column => {
        const pages = await column.data
        const columnData = []
        for await (const chunk of pages.data) {
          onPage?.({
            pathInSchema: column.pathInSchema,
            columnData: chunk,
            rowStart: rowGroup.groupStart,
            rowEnd: rowGroup.groupStart + rowGroup.groupRows,
          })
          columnData.push(chunk)
        }
        flatData.set(column.pathInSchema.join('.'), flatten(columnData))
      })).then(async function* () {
        assembleNested(flatData, child)
        const flatColumn = flatData.get(child.path[0])
        if (!flatColumn) throw new Error('parquet column data not assembled')
        onChunk?.({
          columnName: child.path[0],
          columnData: flatColumn,
          rowStart: rowGroup.groupStart,
          rowEnd: rowGroup.groupStart + flatColumn.length,
        })
        yield flatColumn
      }).then(data => ({ data, pageSkip: 0 }))

      assembled.push({ columnName: child.path[0], data })
    } else {
      // leaf column
      const asyncColumn = asyncColumns.find(column => column.pathInSchema[0] === child.element.name)
      if (asyncColumn) {
        const data = asyncColumn.data.then(async ({ data: pageData, pageSkip }) => {
          const columnData = []
          for await (const chunk of pageData) {
            onPage?.({
              pathInSchema: asyncColumn.pathInSchema,
              columnData: chunk,
              rowStart: rowGroup.groupStart,
              rowEnd: rowGroup.groupStart + rowGroup.groupRows,
            })
            columnData.push(chunk)
          }
          return { data: (async function* () { yield flatten(columnData) })(), pageSkip }
        })
        assembled.push({ columnName: child.path[0], data })
      }
    }
  }

  // Flatten async column data
  /** @type {ResolvedPages[]} */
  const resolvedPages = await Promise.all(assembled.map(flattenAsync))

  // Filter columns
  const filteredColumns = columns
    ? assembled.filter(column => columns.includes(column.columnName))
    : assembled

  // Transpose columns to rows
  const selectCount = selectEnd - selectStart
  /** @type {Record<string, any>[]} */
  const rows = new Array(selectCount)
  for (let i = 0; i < selectCount; i++) {
    /** @type {Record<string, any>} */
    const row = {}
    for (let j = 0; j < filteredColumns.length; j++) {
      const columnIndex = assembled.indexOf(filteredColumns[j])
      const { data, pageSkip } = resolvedPages[columnIndex]
      row[filteredColumns[j].columnName] = data[selectStart + i - pageSkip]
    }
    rows[i] = row
  }
  return rows
}
