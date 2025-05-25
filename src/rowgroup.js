import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { getColumnRange } from './plan.js'
import { getSchemaPath } from './schema.js'
import { flatten } from './utils.js'

/**
 * Read a row group from a file-like object.
 *
 * @param {ParquetReadOptions} options read options
 * @param {RowGroup} rowGroup row group to read
 * @param {number} groupStart row index of the first row in the group
 * @returns {Promise<any[][]>} resolves to row data
 */
export async function readRowGroup(options, rowGroup, groupStart) {
  const { file, metadata, columns, rowStart = 0, rowEnd } = options
  if (!metadata) throw new Error('parquet metadata not found')
  const numRows = Number(rowGroup.num_rows)
  // indexes within the group to read:
  const selectStart = Math.max(rowStart - groupStart, 0)
  const selectEnd = Math.min((rowEnd ?? Infinity) - groupStart, numRows)
  /** @type {RowGroupSelect} */
  const rowGroupSelect = { groupStart, selectStart, selectEnd, numRows }

  /** @type {Promise<void>[]} */
  const promises = []
  // top-level columns to assemble
  const { children } = getSchemaPath(metadata.schema, [])[0]
  const subcolumnNames = new Map(children.map(child => [child.element.name, getSubcolumns(child)]))
  /** @type {Map<string, DecodedArray[]>} */
  const subcolumnData = new Map() // columns to assemble as maps
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
    promises.push(buffer.then(arrayBuffer => {
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
      /** @type {DecodedArray[] | undefined} */
      let chunks = readColumn(reader, rowGroupSelect, columnDecoder, options.onPage)

      // skip assembly if no onComplete or onChunk
      if (!options.onComplete && !options.onChunk) return

      // TODO: fast path for non-nested columns
      // save column data for assembly
      subcolumnData.set(subcolumn, chunks)
      chunks = undefined

      const subcolumns = subcolumnNames.get(columnName)
      if (subcolumns?.every(name => subcolumnData.has(name))) {
        // For every subcolumn, flatten and assemble the column
        const flatData = new Map(subcolumns.map(name => [name, flatten(subcolumnData.get(name))]))
        assembleNested(flatData, schemaPath[1])
        const flatColumn = flatData.get(columnName)
        if (!flatColumn) throw new Error(`parquet column data not assembled: ${columnName}`)
        chunks = [flatColumn]
        subcolumns.forEach(name => subcolumnData.delete(name))
        subcolumnData.set(columnName, chunks)
      }

      // do not emit column data until structs are fully parsed
      if (!chunks) return
      // notify caller of column data
      if (options.onChunk) {
        for (const columnData of chunks) {
          options.onChunk({
            columnName,
            columnData,
            rowStart: groupStart,
            rowEnd: groupStart + columnData.length,
          })
        }
      }
    }))
  }
  await Promise.all(promises)
  if (options.onComplete) {
    const includedColumnNames = children
      .map(child => child.element.name)
      .filter(name => !columns || columns.includes(name))
    const columnOrder = columns || includedColumnNames
    const includedColumns = columnOrder
      .map(name => includedColumnNames.includes(name) ? flatten(subcolumnData.get(name)) : undefined)

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
  return []
}

/**
 * Return a list of sub-columns needed to construct a top-level column.
 *
 * @import {DecodedArray, ParquetReadOptions, RowGroup, RowGroupSelect, SchemaTree} from '../src/types.d.ts'
 * @param {SchemaTree} schema
 * @param {string[]} output
 * @returns {string[]}
 */
function getSubcolumns(schema, output = []) {
  if (schema.children.length) {
    for (const child of schema.children) {
      getSubcolumns(child, output)
    }
  } else {
    output.push(schema.path.join('.'))
  }
  return output
}
