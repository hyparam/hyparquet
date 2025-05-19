import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { assembleAsync, asyncGroupToRows, readRowGroup } from './rowgroup.js'
import { concat } from './utils.js'

/**
 * @import {ParquetReadOptions} from '../src/types.js'
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
  if (!options.file || !(options.file.byteLength >= 0)) {
    throw new Error('parquetRead expected file AsyncBuffer')
  }

  // load metadata if not provided
  options.metadata ??= await parquetMetadataAsync(options.file)
  const schemaTree = parquetSchema(options.metadata)
  const {
    rowStart = 0,
    rowEnd,
    columns,
    onChunk,
    onComplete,
    rowFormat,
  } = options

  // TODO: validate options (start, end, columns, etc)

  // prefetch byte ranges
  const plan = parquetPlan(options)
  options.file = prefetchAsyncBuffer(options.file, plan)

  // read row groups
  const asyncGroups = plan.groups.map(groupPlan => readRowGroup(options, plan, groupPlan))

  // skip assembly if no onComplete or onChunk, but wait for reading to finish
  if (!onComplete && !onChunk) {
    for (const { asyncColumns } of asyncGroups) {
      for (const { data } of asyncColumns) await data
    }
    return
  }

  // assemble struct columns
  const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree))

  // onChunk emit all chunks (don't await)
  if (onChunk) {
    for (const asyncGroup of assembled) {
      for (const asyncColumn of asyncGroup.asyncColumns) {
        asyncColumn.data.then(columnDatas => {
          let rowStart = asyncGroup.groupStart
          for (const columnData of columnDatas) {
            onChunk({
              columnName: asyncColumn.pathInSchema[0],
              columnData,
              rowStart,
              rowEnd: rowStart + columnData.length,
            })
            rowStart += columnData.length
          }
        })
      }
    }
  }

  // onComplete transpose column chunks to rows
  if (onComplete) {
    /** @type {any[][]} */
    const rows = []
    for (const asyncGroup of assembled) {
      // filter to rows in range
      const selectStart = Math.max(rowStart - asyncGroup.groupStart, 0)
      const selectEnd = Math.min((rowEnd ?? Infinity) - asyncGroup.groupStart, asyncGroup.groupRows)
      // transpose column chunks to rows in output
      const groupData = await asyncGroupToRows(asyncGroup, selectStart, selectEnd, columns, rowFormat)
      concat(rows, groupData.slice(selectStart, selectEnd))
    }
    onComplete(rows)
  } else {
    // wait for all async groups to finish (complete takes care of this)
    for (const { asyncColumns } of assembled) {
      for (const { data } of asyncColumns) await data
    }
  }
}
