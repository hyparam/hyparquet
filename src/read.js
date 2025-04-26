import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { parquetMetadataAsync } from './metadata.js'
import { getColumnRange, parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { getSchemaPath } from './schema.js'
import { concat } from './utils.js'

/**
 * Read parquet data rows from a file-like object.
 * Reads the minimal number of row groups and columns to satisfy the request.
 *
 * Returns a void promise when complete, and to throw errors.
 * Data is returned in onComplete, not the return promise, because
 * if onComplete is undefined, we parse the data, and emit chunks, but skip
 * computing the row view directly. This saves on allocation if the caller
 * wants to cache the full chunks, and make their own view of the data from
 * the chunks.
 *
 * @param {ParquetReadOptions} options read options
 * @returns {Promise<void>} resolves when all requested rows and columns are parsed, all errors are thrown here
 */
export async function parquetRead(options) {
  if (!options.file || !(options.file.byteLength >= 0)) {
    throw new Error('parquetRead expected file AsyncBuffer')
  }

  // load metadata if not provided
  options.metadata ||= await parquetMetadataAsync(options.file)
  const { metadata, onComplete, rowStart = 0, rowEnd } = options
  if (rowStart < 0) throw new Error('parquetRead rowStart must be postive')

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
  for (let i = 0; i < rowGroup.columns.length; i++) {
    const columnMetadata = rowGroup.columns[i].meta_data
    if (!columnMetadata) throw new Error('parquet column metadata is undefined')

    // skip columns that are not requested
    const columnName = columnMetadata.path_in_schema[0]
    if (columns && !columns.includes(columnName)) continue

    const { startByte, endByte } = getColumnRange(columnMetadata)
    const columnBytes = endByte - startByte

    // skip columns larger than 1gb
    // TODO: stream process the data, returning only the requested rows
    if (columnBytes > 1 << 30) {
      console.warn(`parquet skipping huge column "${columnMetadata.path_in_schema}" ${columnBytes} bytes`)
      // TODO: set column to new Error('parquet column too large')
      continue
    }

    // wrap awaitable to ensure it's a promise
    /** @type {Promise<ArrayBuffer>} */
    const buffer = Promise.resolve(file.slice(startByte, endByte))

    // read column data async
    promises.push(buffer.then(arrayBuffer => {
      const schemaPath = getSchemaPath(metadata.schema, columnMetadata.path_in_schema)
      const reader = { view: new DataView(arrayBuffer), offset: 0 }
      const columnDecoder = {
        columnName: columnMetadata.path_in_schema.join('.'),
        type: columnMetadata.type,
        element: schemaPath[schemaPath.length - 1].element,
        schemaPath,
        codec: columnMetadata.codec,
        compressors: options.compressors,
        utf8: options.utf8,
      }
      /** @type {DecodedArray[] | undefined} */
      let chunks = readColumn(reader, rowGroupSelect, columnDecoder, options.onPage)

      // skip assembly if no onComplete or onChunk
      if (!options.onComplete && !options.onChunk) return

      // TODO: fast path for non-nested columns
      // save column data for assembly
      const subcolumn = columnMetadata.path_in_schema.join('.')
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
 * Flatten a list of lists into a single list.
 *
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
