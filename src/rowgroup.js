import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { DEFAULT_PARSERS } from './convert.js'
import { getColumnRange } from './plan.js'
import { getSchemaPath } from './schema.js'
import { flatten } from './utils.js'

/**
 * @import {AsyncColumn, AsyncRowGroup, DecodedArray, GroupPlan, ParquetParsers, ParquetReadOptions, QueryPlan, RowGroup, SchemaTree} from './types.js'
 */
/**
 * Read a row group from a file-like object.
 *
 * @param {ParquetReadOptions} options
 * @param {QueryPlan} plan
 * @param {GroupPlan} groupPlan
 * @returns {AsyncRowGroup} resolves to column data
 */
export function readRowGroup(options, { metadata, columns }, groupPlan) {
  const { file, compressors, utf8 } = options

  /** @type {AsyncColumn[]} */
  const asyncColumns = []
  /** @type {ParquetParsers} */
  const parsers = { ...DEFAULT_PARSERS, ...options.parsers }

  // read column data
  for (const { file_path, meta_data } of groupPlan.rowGroup.columns) {
    if (file_path) throw new Error('parquet file_path not supported')
    if (!meta_data) throw new Error('parquet column metadata is undefined')

    // skip columns that are not requested
    const columnName = meta_data.path_in_schema[0]
    if (columns && !columns.includes(columnName)) continue

    const { startByte, endByte } = getColumnRange(meta_data)
    const columnBytes = endByte - startByte

    // skip columns larger than 1gb
    // TODO: stream process the data, returning only the requested rows
    if (columnBytes > 1 << 30) {
      console.warn(`parquet skipping huge column "${meta_data.path_in_schema}" ${columnBytes} bytes`)
      // TODO: set column to new Error('parquet column too large')
      continue
    }

    // wrap awaitable to ensure it's a promise
    /** @type {Promise<ArrayBuffer>} */
    const buffer = Promise.resolve(file.slice(startByte, endByte))

    // read column data async
    asyncColumns.push({
      pathInSchema: meta_data.path_in_schema,
      data: buffer.then(arrayBuffer => {
        const schemaPath = getSchemaPath(metadata.schema, meta_data.path_in_schema)
        const reader = { view: new DataView(arrayBuffer), offset: 0 }
        const columnDecoder = {
          pathInSchema: meta_data.path_in_schema,
          type: meta_data.type,
          element: schemaPath[schemaPath.length - 1].element,
          schemaPath,
          codec: meta_data.codec,
          parsers,
          compressors,
          utf8,
        }
        return readColumn(reader, groupPlan, columnDecoder, options.onPage)
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
  const columnDatas = await Promise.all(asyncColumns.map(({ data }) => data.then(flatten)))

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
      rowData[filteredColumns[i].pathInSchema[0]] = columnDatas[columnIndex][row]
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
        return column.data.then(columnData => {
          flatData.set(column.pathInSchema.join('.'), flatten(columnData))
        })
      })).then(() => {
        // assemble the column
        assembleNested(flatData, child)
        const flatColumn = flatData.get(child.path.join('.'))
        if (!flatColumn) throw new Error('parquet column data not assembled')
        return [flatColumn]
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
