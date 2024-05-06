
import { assembleNested } from './assemble.js'
import { getColumnOffset, readColumn } from './column.js'
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
 * @typedef {import('./hyparquet.js').ColumnData} ColumnData
 * @typedef {import('./types.js').Compressors} Compressors
 * @typedef {import('./types.js').AsyncBuffer} AsyncBuffer
 * @typedef {import('./types.js').FileMetaData} FileMetaData
 * @param {object} options read options
 * @param {AsyncBuffer} options.file file-like object containing parquet data
 * @param {FileMetaData} [options.metadata] parquet file metadata
 * @param {string[]} [options.columns] columns to read, all columns if undefined
 * @param {number} [options.rowStart] first requested row index (inclusive)
 * @param {number} [options.rowEnd] last requested row index (exclusive)
 * @param {(chunk: ColumnData) => void} [options.onChunk] called when a column chunk is parsed. chunks may include row data outside the requested range.
 * @param {(rows: any[][]) => void} [options.onComplete] called when all requested rows and columns are parsed
 * @param {Compressors} [options.compressors] custom decompressors
 * @returns {Promise<void>} resolves when all requested rows and columns are parsed
 */
export async function parquetRead(options) {
  if (!options.file) throw new Error('parquet file is required')

  // load metadata if not provided
  options.metadata ||= await parquetMetadataAsync(options.file)
  if (!options.metadata) throw new Error('parquet metadata not found')

  const { metadata, onComplete } = options
  const rowStart = options.rowStart || 0
  const rowEnd = options.rowEnd || Number(metadata.num_rows)
  /** @type {any[][]} */
  const rowData = []

  // find which row groups to read
  let groupStart = 0 // first row index of the current group
  for (const rowGroup of metadata.row_groups) {
    // number of rows in this row group
    const groupRows = Number(rowGroup.num_rows)
    // if row group overlaps with row range, read it
    if (groupStart + groupRows >= rowStart && groupStart < rowEnd) {
      // read row group
      const groupData = await readRowGroup(options, rowGroup, groupStart)
      if (onComplete) {
        // filter to rows in range
        const start = Math.max(rowStart - groupStart, 0)
        const end = Math.min(rowEnd - groupStart, groupRows)
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
 * @typedef {import('./types.js').RowGroup} RowGroup
 * @param {object} options read options
 * @param {AsyncBuffer} options.file file-like object containing parquet data
 * @param {FileMetaData} [options.metadata] parquet file metadata
 * @param {string[]} [options.columns] columns to read, all columns if undefined
 * @param {(chunk: ColumnData) => void} [options.onChunk] called when a column chunk is parsed. chunks may include row data outside the requested range.
 * @param {(rows: any[][]) => void} [options.onComplete] called when all requested rows and columns are parsed
 * @param {Compressors} [options.compressors]
 * @param {RowGroup} rowGroup row group to read
 * @param {number} groupStart row index of the first row in the group
 * @returns {Promise<any[][]>} resolves to row data
 */
async function readRowGroup(options, rowGroup, groupStart) {
  const { file, metadata, columns, compressors } = options
  if (!metadata) throw new Error('parquet metadata not found')

  // loop through metadata to find min/max bytes to read
  let [groupStartByte, groupEndByte] = [file.byteLength, 0]
  rowGroup.columns.forEach(({ meta_data: columnMetadata }) => {
    if (!columnMetadata) throw new Error('parquet column metadata is undefined')
    // skip columns that are not requested
    if (columns && !columns.includes(columnMetadata.path_in_schema[0])) return

    const startByte = getColumnOffset(columnMetadata)
    const endByte = startByte + Number(columnMetadata.total_compressed_size)
    groupStartByte = Math.min(groupStartByte, startByte)
    groupEndByte = Math.max(groupEndByte, endByte)
  })
  if (groupStartByte >= groupEndByte && columns?.length) {
    // TODO: should throw if any column is missing
    throw new Error(`parquet columns not found: ${columns.join(', ')}`)
  }
  // if row group size is less than 32mb, pre-load in one read
  let groupBuffer
  if (groupEndByte - groupStartByte <= 1 << 25) {
    // pre-load row group byte data in one big read,
    // otherwise read column data individually
    groupBuffer = await file.slice(groupStartByte, groupEndByte)
  }

  /** @type {any[][]} */
  const groupColumnData = []
  const promises = []
  // Top-level columns to assemble
  const { children } = getSchemaPath(metadata.schema, [])[0]
  const subcolumnNames = new Map(children.map(child => [child.element.name, getSubcolumns(child)]))
  const subcolumnData = new Map() // columns to assemble as maps
  // read column data
  for (let columnIndex = 0; columnIndex < rowGroup.columns.length; columnIndex++) {
    const columnMetadata = rowGroup.columns[columnIndex].meta_data
    if (!columnMetadata) throw new Error('parquet column metadata is undefined')

    // skip columns that are not requested
    const columnName = columnMetadata.path_in_schema[0]
    if (columns && !columns.includes(columnName)) continue

    const columnStartByte = getColumnOffset(columnMetadata)
    const columnEndByte = columnStartByte + Number(columnMetadata.total_compressed_size)
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
      /** @type {any[] | undefined} */
      let columnData = readColumn(
        arrayBuffer, bufferOffset, rowGroup, columnMetadata, schemaPath, compressors
      )
      // assert(columnData.length === Number(rowGroup.num_rows)

      // TODO: fast path for non-nested columns
      // Save column data for assembly
      const subcolumn = columnMetadata.path_in_schema.join('.')
      subcolumnData.set(subcolumn, columnData)
      columnData = undefined

      const subcolumns = subcolumnNames.get(columnName)
      if (subcolumns?.every(name => subcolumnData.has(name))) {
        // We have all data needed to assemble a top level column
        assembleNested(subcolumnData, schemaPath[1])
        columnData = subcolumnData.get(columnName)
        if (!columnData) {
          throw new Error(`parquet column data not assembled: ${columnName}`)
        }
      }

      // do not emit column data until structs are fully parsed
      if (!columnData) return
      // notify caller of column data
      options.onChunk?.({
        columnName,
        columnData,
        rowStart: groupStart,
        rowEnd: groupStart + columnData.length,
      })
      // save column data only if onComplete is defined
      if (options.onComplete) groupColumnData.push(columnData)
    }))
  }
  await Promise.all(promises)
  if (options.onComplete) {
    // transpose columns into rows
    return groupColumnData[0].map((_, row) => groupColumnData.map(col => col[row]))
  }
  return []
}


/**
 * Return a list of sub-columns needed to construct a top-level column.
 *
 * @param {import('./types.js').SchemaTree} schema
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
