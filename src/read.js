import { parquetMetadata, parquetSchema } from './metadata.js'
import { parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { assembleAsync, readRowGroup, transposeColumnsToRows } from './rowgroup.js'
import { concat, flatten, flattenAsync } from './utils.js'

/**
 * @import {AsyncRowGroup, DecodedArray, ParquetReadOptions, BaseParquetReadOptions, SubColumnData, AsyncRowGroupAssembled, ColumnData} from '../src/types.js'
 */

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
  // load metadata if not provided
  options.metadata ??= await parquetMetadata(options)
  const { rowStart = 0, rowEnd = Infinity, onComplete } = options

  // read row groups
  const asyncGroups = parquetReadAsync(options)

  // assemble struct columns
  const schemaTree = parquetSchema(options.metadata)

  // onComplete transpose column chunks to rows
  if (onComplete) {
    /** @type {Record<string, any>[]} */
    const rows = []
    for await (const rowGroup of asyncGroups) {
      // filter to rows in range
      const selectStart = Math.max(rowStart - rowGroup.groupStart, 0)
      const selectEnd = Math.min(rowEnd - rowGroup.groupStart, rowGroup.groupRows)
      const emitted = emitPages(rowGroup, options.onPage)
      const assembled = await assembleAsync(emitted, schemaTree, options.onChunk)
      const columnDatas = await Promise.all(assembled.asyncColumns.map(flattenAsync))
      const transposed = transposeColumnsToRows(assembled.asyncColumns, columnDatas, selectStart, selectEnd, options.columns)
      concat(rows, transposed)
    }
    onComplete(rows)
  }
}

/**
 * Asynchronously read parquet data according to options.
 *
 * @param {ParquetReadOptions} options read options
 * @yields {AsyncRowGroup}
 */
export async function* parquetReadAsync(options) {
  // load metadata if not provided
  options.metadata ??= await parquetMetadata(options)
  // TODO: validate options (start, end, columns, etc)

  // prefetch byte ranges
  const plan = parquetPlan(options)
  if (options.prefetch !== false) {
    options.file = prefetchAsyncBuffer(options.file, plan)
  }

  // read row groups
  for (const groupPlan of plan.groups) {
    yield readRowGroup(options, plan, groupPlan)
  }
}

/**
 * Reads a single column from a parquet file.
 *
 * @param {BaseParquetReadOptions} options
 * @returns {Promise<DecodedArray>}
 */
export async function parquetReadColumn(options) {
  if (options.columns?.length !== 1) {
    throw new Error('parquetReadColumn expected columns: [columnName]')
  }

  /** @type {DecodedArray[]} */
  const columnData = []
  for await (const rg of parquetReadAsync(options)) {
    columnData.push(await flattenAsync(rg.asyncColumns[0]))
  }
  return flatten(columnData)
}

/**
 * This is a helper function to read parquet row data as a promise.
 * It is a wrapper around the more configurable parquetRead function.
 *
 * @param {ParquetReadOptions} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export function parquetReadObjects(options) {
  return new Promise((onComplete, reject) => {
    parquetRead({
      ...options,
      onComplete,
    }).catch(reject)
  })
}

/**
 * @param {AsyncRowGroup} rowGroup
 * @param {((page: SubColumnData) => void) | undefined} onPage
 * @returns {AsyncRowGroup}
 */
function emitPages(rowGroup, onPage) {
  if (!onPage) return rowGroup
  return {
    ...rowGroup,
    asyncColumns: rowGroup.asyncColumns.map(col => ({
      ...col,
      data: (async function* () {
        for await (const columnData of col.data) {
          onPage({
            pathInSchema: col.pathInSchema,
            columnData,
            rowStart: rowGroup.groupStart,
            rowEnd: rowGroup.groupStart + rowGroup.groupRows,
          })
          yield columnData
        }
      })(),
    })),
  }
}
