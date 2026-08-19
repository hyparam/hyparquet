/**
 * @import {AsyncRowGroup, BaseParquetReadOptions, DecodedArray, ParquetReadOptions} from '../src/types.js'
 */

import { columnsNeededForFilter, matchFilter } from './filter.js'
import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { parquetPlan } from './plan.js'
import { assembleAsync, asyncGroupToRows } from './rowgroup.js'
import { prepareParquetRead, readParquetPlan } from './scan.js'
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
  // load metadata if not provided
  options.metadata ??= await parquetMetadataAsync(options.file, options)

  const { rowStart = 0, rowEnd, columns, onChunk, onComplete, rowFormat, filter, filterStrict = true } = options

  // Filter requires object format to match column names
  if (filter && rowFormat !== 'object') {
    throw new Error('parquet filter requires rowFormat: "object"')
  }

  const filterColumns = columnsNeededForFilter(filter)
  let readColumns = columns
  if (columns && filterColumns.length) {
    const selectedColumns = new Set(columns)
    const extraColumns = filterColumns.filter(column => !selectedColumns.has(column))
    if (extraColumns.length) readColumns = [...columns, ...extraColumns]
  }
  const readOptions = readColumns === columns ? options : { ...options, columns: readColumns }
  const prepared = await prepareParquetRead(readOptions)
  const preparedOptions = prepared.options
  const requiresProjection = readColumns !== columns
  const asyncGroups = readParquetPlan(preparedOptions, prepared.plan)

  // skip assembly if no onComplete or onChunk, but wait for reading to finish
  if (!onComplete && !onChunk) {
    await awaitAllColumns(asyncGroups)
    return
  }

  // assemble struct columns
  if (!preparedOptions.metadata) throw new Error('parquet requires metadata')
  const schemaTree = parquetSchema(preparedOptions.metadata)
  const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree, options.parsers))

  // onChunk emit all chunks (don't await). Rejection is surfaced by awaitAllColumns below.
  if (onChunk) {
    for (const asyncGroup of assembled) {
      for (const asyncColumn of asyncGroup.asyncColumns) {
        asyncColumn.data.then(({ data, skipped }) => {
          let rowStart = asyncGroup.groupStart + skipped
          for (const columnData of data) {
            onChunk({
              columnName: asyncColumn.pathInSchema[0],
              columnData,
              rowStart,
              rowEnd: rowStart + columnData.length,
            })
            rowStart += columnData.length
          }
        }, () => {})
      }
    }
  }

  // onComplete transpose column chunks to rows
  if (onComplete) {
    // wait for all reads to settle so a sibling rejection cannot leak
    await awaitAllColumns(assembled)
    // loosen the types to avoid duplicate code
    /** @type {any[]} */
    const rows = []
    for (const asyncGroup of assembled) {
      // filter to rows in range (the plan may have narrowed the selection to
      // a sub-range of the group via page index pushdown)
      const selectStart = asyncGroup.selectStart ?? Math.max(rowStart - asyncGroup.groupStart, 0)
      const selectEnd = asyncGroup.selectEnd ?? Math.min((rowEnd ?? Infinity) - asyncGroup.groupStart, asyncGroup.groupRows)
      // transpose column chunks to rows in output
      const groupData = rowFormat === 'object' ?
        await asyncGroupToRows(asyncGroup, selectStart, selectEnd, readColumns, 'object') :
        await asyncGroupToRows(asyncGroup, selectStart, selectEnd, columns, 'array')

      // Apply filter and projection
      if (filter) {
        // eslint-disable-next-line no-extra-parens
        for (const row of /** @type {Record<string, any>[]} */ (groupData)) {
          if (matchFilter(row, filter, filterStrict)) {
            if (requiresProjection && columns) {
              for (const col of filterColumns) {
                if (!columns.includes(col)) delete row[col]
              }
            }
            rows.push(row)
          }
        }
      } else {
        concat(rows, groupData)
      }
    }
    onComplete(rows)
  } else {
    // wait for all async groups to finish (complete takes care of this)
    await awaitAllColumns(assembled)
  }
}

/**
 * Await every column promise across the given row groups via Promise.allSettled
 * so no rejection escapes as an unhandledRejection. Throws the first rejection.
 *
 * @param {AsyncRowGroup[]} asyncGroups
 * @returns {Promise<void>}
 */
async function awaitAllColumns(asyncGroups) {
  const all = asyncGroups.flatMap(g => g.asyncColumns.map(c => c.data))
  const results = await Promise.allSettled(all)
  const failed = results.find(r => r.status === 'rejected')
  if (failed) throw failed.reason
}

/**
 * @param {ParquetReadOptions} options read options
 * @returns {AsyncRowGroup[]}
 */
export function parquetReadAsync(options) {
  if (!options.metadata) throw new Error('parquet requires metadata')
  // TODO: validate options (start, end, columns, etc)
  return readParquetPlan(options, parquetPlan(options))
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
  options.metadata ??= await parquetMetadataAsync(options.file, options)
  const column = options.columns[0]
  const prepared = await prepareParquetRead(options)
  const asyncGroups = readParquetPlan(prepared.options, prepared.plan)
  if (!prepared.options.metadata) throw new Error('parquet requires metadata')
  const schemaTree = parquetSchema(prepared.options.metadata)
  const assembled = asyncGroups.map(group => assembleAsync(group, schemaTree, options.parsers))

  // Keep one plan for the whole column so reads can be prefetched together.
  await awaitAllColumns(assembled)
  /** @type {DecodedArray} */
  const columnData = []
  for (const group of assembled) {
    const asyncColumn = group.asyncColumns.find(candidate => candidate.pathInSchema[0] === column)
    if (!asyncColumn) throw new Error(`parquet column not found: ${column}`)
    const { data } = await asyncColumn.data
    for (const chunk of data) concat(columnData, chunk)
  }
  return columnData
}

/**
 * This is a helper function to read parquet row data as a promise.
 * It is a wrapper around the more configurable parquetRead function.
 *
 * @param {Omit<ParquetReadOptions, 'onComplete'>} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export function parquetReadObjects(options) {
  return new Promise((onComplete, reject) => {
    parquetRead({
      ...options,
      rowFormat: 'object', // force object output
      onComplete,
    }).catch(reject)
  })
}
