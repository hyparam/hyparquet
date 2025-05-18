import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { getColumnRange, parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { getSchemaPath } from './schema.js'
import { concat } from './utils.js'

/**
 * Read parquet data rows from a file-like object.
 * Reads the minimal number of row groups and columns to satisfy the request.
 *
 * Returns a void promise when complete.
 * Errors are thrown on the returned promise.
 * Data is returned in callbacks onComplete, onChunk, onPage, NOT the return promise.
 * See parquetReadObjects for a more convenient API.
 *
 * @param {ParquetReadOptions} options read options
 * @returns {Promise<void>} resolves when all requested rows and columns are parsed, all errors are thrown here
 */
export async function parquetRead(options) {
  if (!options.file || !(options.file.byteLength >= 0)) {
    throw new Error('parquetRead expected file AsyncBuffer')
  }

  // load metadata if not provided
  options.metadata ??= await parquetMetadataAsync(options.file)
  const { metadata, onComplete, rowStart = 0, rowEnd } = options
  if (rowStart < 0) throw new Error('parquetRead rowStart must be positive')

  // prefetch byte ranges
  const plan = parquetPlan(options)
  options.file = prefetchAsyncBuffer(options.file, plan)

  /** @type {any[][]} */
  const rowData = []

  // read row groups
  let groupStart = 0 // first row index of the current group
  for (const rowGroup of metadata.row_groups) {
    // number of rows in this row group
    const groupRows = Number(rowGroup.num_rows)
    // if row group overlaps with row range, read it
    if (groupStart + groupRows >= rowStart && (rowEnd === undefined || groupStart < rowEnd)) {
      // read row group
      const groupData = await readRowGroup(options, rowGroup, groupStart)
      if (onComplete) {
        if (!groupData) continue
        // filter to rows in range
        const start = Math.max(rowStart - groupStart, 0)
        const end = rowEnd === undefined ? undefined : rowEnd - groupStart
        concat(rowData, groupData.slice(start, end))
      }
    }
    groupStart += groupRows
  }

  if (onComplete) onComplete(rowData)
}

/**
 * Read a row group from a file-like object.
 *
 * @param {ParquetReadOptions} options read options
 * @param {RowGroup} rowGroup row group to read
 * @param {number} groupStart row index of the first row in the group
 * @returns {Promise<any[][] | undefined>} resolves to row data
 */
export async function readRowGroup(options, rowGroup, groupStart) {
  const { metadata, columns, rowStart = 0, rowEnd, onComplete, onChunk } = options
  if (!metadata) throw new Error('parquet metadata not found')
  const groupRows = Number(rowGroup.num_rows)
  // indexes within the group to read:
  const selectStart = Math.max(rowStart - groupStart, 0)
  const selectEnd = Math.min((rowEnd ?? Infinity) - groupStart, groupRows)
  /** @type {RowGroupSelect} */
  const rowGroupSelect = { groupStart, selectStart, selectEnd, groupRows }

  // read row group
  const { asyncColumns } = readRowGroupAsync(options, rowGroup, rowGroupSelect)

  // skip assembly if no onComplete or onChunk, but wait for reading to finish
  if (!onComplete && !onChunk) {
    for (const { data } of asyncColumns) await data
    return
  }

  const schemaTree = parquetSchema(metadata)
  const assembled = assembleAsync(asyncColumns, schemaTree)

  if (onChunk) {
    // if onChunk is defined, emit all chunks
    for (const asyncColumn of assembled) {
      // don't await, emit as soon as possible
      asyncColumn.data.then(columnDatas => {
        for (const columnData of columnDatas) {
          onChunk({
            columnName: asyncColumn.pathInSchema.join('.'),
            columnData,
            rowStart: groupStart,
            rowEnd: groupStart + columnData.length,
          })
        }
      })
    }
  }

  // Wait for all columns to be read
  const columnData = await Promise.all(assembled.map(column => column.data))

  if (onComplete) {
    // if onComplete is defined, assemble and emit all rows
    const includedColumnNames = schemaTree.children
      .map(child => child.element.name)
      .filter(name => !columns || columns.includes(name))
    const columnOrder = columns ?? includedColumnNames
    const includedColumns = columnOrder.map(columnName => {
      if (includedColumnNames.includes(columnName)) {
        const columnDataIndex = assembled.findIndex(column => column.pathInSchema[0] === columnName)
        if (columnDataIndex < 0) throw new Error('parquet assembly failed')
        return flatten(columnData[columnDataIndex])
      }
    })

    // transpose columns into rows
    const groupData = new Array(selectEnd)
    for (let row = selectStart; row < selectEnd; row++) {
      if (options.rowFormat === 'object') {
        // return each row as an object
        /** @type {Record<string, any>} */
        const rowData = {}
        for (let i = 0; i < columnOrder.length; i++) {
          rowData[columnOrder[i]] = includedColumns[i]?.[row]
        }
        groupData[row] = rowData
      } else {
        // return each row as an array
        groupData[row] = includedColumns.map(column => column?.[row])
      }
    }
    return groupData
  }
}

/**
 * @param {ParquetReadOptions} options read options
 * @param {RowGroup} rowGroup row group to read
 * @param {RowGroupSelect} rowGroupSelect row group selection
 * @returns {AsyncRowGroup} resolves to column data
 */
export function readRowGroupAsync(options, rowGroup, rowGroupSelect) {
  const { file, metadata, columns } = options
  if (!metadata) throw new Error('parquet metadata expected')

  /** @type {AsyncColumn[]} */
  const asyncColumns = []

  // read column data
  for (const { file_path, meta_data } of rowGroup.columns) {
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
        const subcolumn = meta_data.path_in_schema.join('.')
        const columnDecoder = {
          columnName: subcolumn,
          type: meta_data.type,
          element: schemaPath[schemaPath.length - 1].element,
          schemaPath,
          codec: meta_data.codec,
          compressors: options.compressors,
          utf8: options.utf8,
        }
        return readColumn(reader, rowGroupSelect, columnDecoder, options.onPage)
      }),
    })
  }

  return { groupStart: rowGroupSelect.groupStart, asyncColumns }
}

/**
 * Assemble physical columns into top-level columns asynchronously.
 *
 * @param {AsyncColumn[]} asyncColumns
 * @param {SchemaTree} schemaTree
 * @returns {AsyncColumn[]}
 */
function assembleAsync(asyncColumns, schemaTree) {
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
  return assembled
}

/**
 * Flatten a list of lists into a single list.
 *
 * @import {AsyncColumn, AsyncRowGroup, DecodedArray, ParquetReadOptions, RowGroup, RowGroupSelect, SchemaTree} from '../src/types.d.ts'
 * @param {DecodedArray[] | undefined} chunks
 * @returns {DecodedArray}
 */
function flatten(chunks) {
  if (!chunks) return []
  if (chunks.length === 1) return chunks[0]
  /** @type {any[]} */
  const output = []
  for (const chunk of chunks) {
    concat(output, chunk)
  }
  return output
}
