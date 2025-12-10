import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { DEFAULT_PARSERS } from './convert.js'
import { getColumnRange } from './plan.js'
import { getSchemaPath } from './schema.js'
import { flatten } from './utils.js'

/**
 * @import {AsyncColumn, AsyncRowGroupAssembled, AsyncSubColumn, AsyncRowGroup, DecodedArray, GroupPlan, ParquetParsers, ParquetReadOptions, QueryPlan, RowGroup, SchemaTree, ColumnData} from '../src/types.js'
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

  /** @type {AsyncSubColumn[]} */
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
      data: (async function* () {
        const arrayBuffer = await buffer
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
        yield* readColumn(reader, groupPlan, columnDecoder, options.onPage)
      })(),
    })
  }

  return { groupStart: groupPlan.groupStart, groupRows: groupPlan.groupRows, asyncColumns }
}

/**
 * @param {AsyncColumn[]} asyncColumns
 * @param {DecodedArray[]} columnDatas
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @returns {Record<string, any>[]} row data
 */
export function transposeColumnsToRows(asyncColumns, columnDatas, selectStart, selectEnd, columns) {
  // filter columns
  const filteredColumns = columns
    ? asyncColumns.filter(column => columns.includes(column.columnName))
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
      rowData[filteredColumns[i].columnName] = columnDatas[columnIndex][row]
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
 * @param {((page: ColumnData) => void) | undefined} onChunk
 * @returns {AsyncRowGroupAssembled}
 */
export function assembleAsync(asyncRowGroup, schemaTree, onChunk) {
  const { asyncColumns } = asyncRowGroup
  /** @type {AsyncColumn[]} */
  const assembled = []
  for (const child of schemaTree.children) {
    if (child.children.length) {
      const childColumns = asyncColumns.filter(column => column.pathInSchema[0] === child.element.name)
      if (!childColumns.length) continue

      /** @type {Map<string, DecodedArray>} */
      const flatData = new Map()
      /** @type {AsyncGenerator<DecodedArray>} */
      const data = (async function* () {
        // wait for all child columns to be read
        await Promise.all(childColumns.map(async column => {
          const columnData = new Array()
          for await (const chunk of column.data) {
            columnData.push(chunk)
          }
          flatData.set(column.pathInSchema.join('.'), flatten(columnData))
        }))
        // assemble the column
        assembleNested(flatData, child)
        const flatColumn = flatData.get(child.path[0])
        if (!flatColumn) throw new Error('parquet column data not assembled')

        // emit chunks
        onChunk?.({
          columnName: child.path[0],
          columnData: flatColumn,
          rowStart: asyncRowGroup.groupStart,
          rowEnd: asyncRowGroup.groupStart + flatColumn.length,
        })

        yield flatColumn
      })()

      assembled.push({ columnName: child.path[0], data })
    } else {
      // leaf node, return the column
      const asyncColumn = asyncColumns.find(column => column.pathInSchema[0] === child.element.name)
      if (asyncColumn) {
        assembled.push({ columnName: child.path[0], data: asyncColumn.data })
      }
    }
  }
  return { ...asyncRowGroup, asyncColumns: assembled }
}
