import { assembleNested } from './assemble.js'
import { getColumnRange, readColumn } from './column.js'
import { parquetMetadataAsync } from './metadata.js'
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
  const rowStart = options.rowStart || 0
  if (rowStart < 0) throw new Error('parquetRead rowStart must be postive')

  // load metadata if not provided
  options.metadata ||= await parquetMetadataAsync(options.file)
  if (!options.metadata) throw new Error('parquet metadata not found')

  const { metadata, onComplete, rowEnd } = options
  /** @type {any[][]} */
  const rowData = []

  // find which row groups to read
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
  const { file, metadata, columns, rowStart, rowEnd } = options
  if (!metadata) throw new Error('parquet metadata not found')
  const numRows = Number(rowGroup.num_rows)
  const rowGroupStart = Math.max((rowStart || 0) - groupStart, 0)
  const rowGroupEnd = rowEnd === undefined ? numRows : Math.min(rowEnd - groupStart, numRows)

  // loop through metadata to find min/max bytes to read
  let [groupStartByte, groupEndByte] = [file.byteLength, 0]
  for (const { meta_data } of rowGroup.columns) {
    if (!meta_data) throw new Error('parquet column metadata is undefined')
    // skip columns that are not requested
    if (columns && !columns.includes(meta_data.path_in_schema[0])) continue

    const [columnStartByte, columnEndByte] = getColumnRange(meta_data).map(Number)
    groupStartByte = Math.min(groupStartByte, columnStartByte)
    groupEndByte = Math.max(groupEndByte, columnEndByte)
  }
  if (groupStartByte >= groupEndByte && columns?.length) {
    throw new Error(`parquet columns not found: ${columns.join(', ')}`)
  }
  // if row group size is less than 32mb, pre-load in one read
  let groupBuffer
  if (groupEndByte - groupStartByte <= 1 << 25) {
    // pre-load row group byte data in one big read,
    // otherwise read column data individually
    groupBuffer = await file.slice(groupStartByte, groupEndByte)
  }

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

    const [columnStartByte, columnEndByte] = getColumnRange(columnMetadata).map(Number)
    const columnBytes = columnEndByte - columnStartByte

    // skip columns larger than 1gb
    // TODO: stream process the data, returning only the requested rows
    if (columnBytes > 1 << 30) {
      console.warn(`parquet skipping huge column "${columnMetadata.path_in_schema}" ${columnBytes.toLocaleString()} bytes`)
      // TODO: set column to new Error('parquet column too large')
      continue
    }

    // use pre-loaded row group byte data if available, else read column data
    /** @type {Promise<ArrayBuffer>} */
    let buffer
    let bufferOffset = 0
    if (groupBuffer) {
      buffer = Promise.resolve(groupBuffer)
      bufferOffset = columnStartByte - groupStartByte
    } else {
      // wrap awaitable to ensure it's a promise
      buffer = Promise.resolve(file.slice(columnStartByte, columnEndByte))
    }

    // read column data async
    promises.push(buffer.then(arrayBuffer => {
      const schemaPath = getSchemaPath(metadata.schema, columnMetadata.path_in_schema)
      const reader = { view: new DataView(arrayBuffer), offset: bufferOffset }
      const columnData = readColumn(reader, rowGroupStart, rowGroupEnd, columnMetadata, schemaPath, options)
      /** @type {DecodedArray[] | undefined} */
      let chunks = columnData

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
      for (const chunk of chunks) {
        options.onChunk?.({
          columnName,
          columnData: chunk,
          rowStart: groupStart,
          rowEnd: groupStart + chunk.length,
        })
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
    const groupData = new Array(rowGroupEnd)
    for (let row = rowGroupStart; row < rowGroupEnd; row++) {
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
 * @import {DecodedArray, ParquetReadOptions, RowGroup, SchemaTree} from '../src/types.d.ts'
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
