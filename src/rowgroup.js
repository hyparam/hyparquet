import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { DEFAULT_PARSERS } from './convert.js'
import { readOffsetIndex } from './indexes.js'
import { getSchemaPath } from './schema.js'
import { flatten } from './utils.js'

/**
 * @import {AsyncColumn, AsyncRowGroup, DecodedArray, GroupPlan, ParquetParsers, ParquetReadOptions, QueryPlan, SchemaTree} from './types.js'
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

  /** @type {AsyncColumn[]} */
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
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @returns {Promise<Record<string, any>[]>} resolves to row data
 */
export async function asyncGroupToRows({ asyncColumns }, selectStart, selectEnd, columns) {
  // columnData[i] for asyncColumns[i]
  // TODO: do it without flatten
  const asyncPages = await Promise.all(asyncColumns.map(async ({ data }) => {
    const pages = await data
    return {
      ...pages,
      data: flatten(pages.data),
    }
  }))

  // filter columns
  const filteredColumns = columns
    ? asyncColumns.filter(column => columns.includes(column.pathInSchema[0]))
    : asyncColumns

  // transpose columns into rows
  const selectCount = selectEnd - selectStart
  /** @type {Record<string, any>[]} */
  const groupData = new Array(selectCount)
  for (let selectRow = 0; selectRow < selectCount; selectRow++) {
    const row = selectStart + selectRow
    /** @type {Record<string, any>} */
    const rowData = {}
    for (let i = 0; i < filteredColumns.length; i++) {
      const columnIndex = asyncColumns.indexOf(filteredColumns[i])
      const { data, pageSkip } = asyncPages[columnIndex]
      rowData[filteredColumns[i].pathInSchema[0]] = data[row - pageSkip]
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
 * @returns {AsyncRowGroup}
 */
export function assembleAsync(asyncRowGroup, schemaTree) {
  const { asyncColumns } = asyncRowGroup
  /** @type {AsyncColumn[]} */
  const assembled = []
  for (const child of schemaTree.children) {
    if (child.children.length) {
      const childColumns = asyncColumns.filter(column => column.pathInSchema[0] === child.element.name)
      if (!childColumns.length) continue

      // wait for all child columns to be read
      /** @type {Map<string, DecodedArray>} */
      const flatData = new Map()
      const data = Promise.all(childColumns.map(column => {
        return column.data.then(({ data }) => {
          flatData.set(column.pathInSchema.join('.'), flatten(data))
        })
      })).then(() => {
        // assemble the column
        assembleNested(flatData, child)
        const flatColumn = flatData.get(child.path.join('.'))
        if (!flatColumn) throw new Error('parquet column data not assembled')
        return { data: [flatColumn], pageSkip: 0 }
      })

      assembled.push({ pathInSchema: child.path, data })
    } else {
      // leaf node, return the column
      const asyncColumn = asyncColumns.find(column => column.pathInSchema[0] === child.element.name)
      if (asyncColumn) {
        assembled.push(asyncColumn)
      }
    }
  }
  return { ...asyncRowGroup, asyncColumns: assembled }
}
