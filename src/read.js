
import { getColumnOffset, readColumn } from './column.js'
import { parquetMetadataAsync } from './metadata.js'
import { getColumnName, isMapLike } from './schema.js'

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
  // load metadata if not provided
  options.metadata ||= await parquetMetadataAsync(options.file)
  if (!options.metadata) throw new Error('parquet metadata not found')

  const { metadata, onComplete } = options
  /** @type {any[][]} */
  const rowData = []
  const rowStart = options.rowStart || 0
  const rowEnd = options.rowEnd || Number(metadata.num_rows)

  // find which row groups to read
  let groupStart = 0 // first row index of the current group
  for (const rowGroup of metadata.row_groups) {
    // number of rows in this row group
    const groupRows = Number(rowGroup.num_rows)
    // if row group overlaps with row range, read it
    if (groupStart + groupRows >= rowStart && groupStart < rowEnd) {
      // read row group
      const groupData = await readRowGroup(options, rowGroup)
      if (onComplete) {
        // filter to rows in range
        const start = Math.max(rowStart - groupStart, 0)
        const end = Math.min(rowEnd - groupStart, groupRows)
        rowData.push(...groupData.slice(start, end))
      }
    }
    groupStart += groupRows
  }

  if (onComplete) onComplete(rowData)
}

/**
 * Read a row group from a file-like object.
 * Reads the minimal number of columns to satisfy the request.
 *
 * @typedef {import('./types.js').RowGroup} RowGroup
 * @param {object} options read options
 * @param {AsyncBuffer} options.file file-like object containing parquet data
 * @param {FileMetaData} [options.metadata] parquet file metadata
 * @param {string[]} [options.columns] columns to read, all columns if undefined
 * @param {(chunk: ColumnData) => void} [options.onChunk] called when a column chunk is parsed. chunks may include row data outside the requested range.
 * @param {(rows: any[][]) => void} [options.onComplete] called when all requested rows and columns are parsed
 * @param {Compressors} [options.compressors] custom decompressors
 * @param {RowGroup} rowGroup row group to read
 * @returns {Promise<any[][]>} resolves to row data
 */
async function readRowGroup(options, rowGroup) {
  const { file, metadata, columns, compressors } = options
  if (!metadata) throw new Error('parquet metadata not found')

  // loop through metadata to find min/max bytes to read
  let [groupStartByte, groupEndByte] = [file.byteLength, 0]
  rowGroup.columns.forEach(({ meta_data: columnMetadata }) => {
    if (!columnMetadata) throw new Error('parquet column metadata is undefined')
    const columnName = getColumnName(metadata.schema, columnMetadata.path_in_schema)
    // skip columns that are not requested
    if (columns && !columns.includes(columnName)) return

    const startByte = getColumnOffset(columnMetadata)
    const endByte = startByte + Number(columnMetadata.total_compressed_size)
    groupStartByte = Math.min(groupStartByte, startByte)
    groupEndByte = Math.max(groupEndByte, endByte)
  })
  if (groupStartByte >= groupEndByte && columns?.length) {
    // TODO: should throw if any column is missing
    throw new Error(`parquet columns not found: ${columns.join(', ')}`)
  }
  // if row group size is less than 128mb, pre-load in one read
  let groupBuffer
  if (groupEndByte - groupStartByte <= 1 << 27) {
    // pre-load row group byte data in one big read,
    // otherwise read column data individually
    groupBuffer = await file.slice(groupStartByte, groupEndByte)
  }

  /** @type {any[][]} */
  const groupData = []
  const promises = []
  const maps = new Map()
  let outputColumnIndex = 0
  // read column data
  for (let columnIndex = 0; columnIndex < rowGroup.columns.length; columnIndex++) {
    const columnMetadata = rowGroup.columns[columnIndex].meta_data
    if (!columnMetadata) throw new Error('parquet column metadata is undefined')

    // skip columns that are not requested
    const columnName = getColumnName(metadata.schema, columnMetadata.path_in_schema)
    // skip columns that are not requested
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
    let buffer
    let bufferOffset = 0
    if (groupBuffer) {
      buffer = Promise.resolve(groupBuffer)
      bufferOffset = columnStartByte - groupStartByte
    } else {
      buffer = file.slice(columnStartByte, columnEndByte)
    }

    // read column data async
    promises.push(buffer.then(arrayBuffer => {
      // TODO: extract SchemaElement for this column
      /** @type {ArrayLike<any> | undefined} */
      let columnData = readColumn(
        arrayBuffer, bufferOffset, rowGroup, columnMetadata, metadata.schema, compressors
      )
      if (columnData.length !== Number(rowGroup.num_rows)) {
        throw new Error(`parquet column length ${columnData.length} does not match row group length ${rowGroup.num_rows}`)
      }

      if (isMapLike(metadata.schema, columnMetadata.path_in_schema)) {
        const name = columnMetadata.path_in_schema.slice(0, -2).join('.')
        if (!maps.has(name)) {
          maps.set(name, columnData)
          columnData = undefined // do not emit column data until both key and value are read
        } else {
          if (columnMetadata.path_in_schema[0] === 'key') {
            throw new Error('parquet map-like column key is not first') // TODO: support value-first
          } else {
            const values = columnData
            const keys = maps.get(name)
            const out = []
            if (keys.length !== values.length) {
              throw new Error('parquet map-like column key/value length mismatch')
            }
            // assemble map-like column data
            for (let i = 0; i < keys.length; i++) {
              /** @type {Record<string, any>} */
              const obj = {}
              for (let j = 0; j < keys[i].length; j++) {
                obj[keys[i][j]] = values[i][j]
              }
              out.push(obj)
            }
            columnData = out
          }
          maps.delete(name)
        }
      }

      // do not emit column data until structs are fully parsed
      if (!columnData) return
      // notify caller of column data
      if (options.onChunk) options.onChunk({ columnName, columnData, rowStart: 0, rowEnd: columnData.length })
      // add column data to group data only if onComplete is defined
      if (options.onComplete) addColumn(groupData, outputColumnIndex, columnData)
      outputColumnIndex++
    }))
  }
  await Promise.all(promises)
  return groupData
}

/**
 * Add a column to rows.
 *
 * @param {any[][]} rows rows to add column data to
 * @param {number} columnIndex column index to add
 * @param {ArrayLike<any>} columnData column data to add
 */
function addColumn(rows, columnIndex, columnData) {
  for (let i = 0; i < columnData.length; i++) {
    if (!rows[i]) rows[i] = []
    rows[i][columnIndex] = columnData[i]
  }
}
