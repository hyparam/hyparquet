import { columnsNeededForFilter, matchFilter } from './filter.js'
import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { assembleAsync, asyncGroupToRows, readRowGroup } from './rowgroup.js'
import { concat, flatten } from './utils.js'

/**
 * @import {AsyncRowGroup, DecodedArray, ParquetReadOptions, BaseParquetReadOptions} from '../src/types.js'
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
  options.metadata ??= await parquetMetadataAsync(options.file, options)

  const { rowStart = 0, rowEnd, columns, onChunk, onComplete, rowFormat, filter, filterStrict = true } = options

  // Filter requires object format to match column names
  if (filter && rowFormat !== 'object') {
    throw new Error('parquet filter requires rowFormat: "object"')
  }

  // Include filter columns in the read plan
  const filterColumns = columnsNeededForFilter(filter)
  if (filterColumns.length) {
    const schemaColumns = parquetSchema(options.metadata).children.map(c => c.element.name)
    const missingColumns = filterColumns.filter(c => !schemaColumns.includes(c))
    if (missingColumns.length) {
      throw new Error(`parquet filter columns not found: ${missingColumns.join(', ')}`)
    }
  }
  let readColumns = columns
  let requiresProjection = false
  if (columns && filter) {
    const missingFilterColumns = filterColumns.filter(c => !columns.includes(c))
    if (missingFilterColumns.length) {
      readColumns = [...columns, ...missingFilterColumns]
      requiresProjection = true
    }
  }

  // read row groups with expanded columns
  const readOptions = readColumns !== columns ? { ...options, columns: readColumns } : options
  const asyncGroups = parquetReadAsync(readOptions)

  // skip assembly if no onComplete or onChunk, but wait for reading to finish
  if (!onComplete && !onChunk) {
    for (const { asyncColumns } of asyncGroups) {
      for (const { data } of asyncColumns) await data
    }
    return
  }

  // assemble struct columns
  const schemaTree = parquetSchema(options.metadata)
  const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree))

  // onChunk emit all chunks (don't await)
  if (onChunk) {
    for (const asyncGroup of assembled) {
      for (const asyncColumn of asyncGroup.asyncColumns) {
        asyncColumn.data.then(({ data, pageSkip }) => {
          let rowStart = asyncGroup.groupStart + pageSkip
          for (const columnData of data) {
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
    // loosen the types to avoid duplicate code
    /** @type {any[]} */
    const rows = []
    for (const asyncGroup of assembled) {
      // filter to rows in range
      const selectStart = Math.max(rowStart - asyncGroup.groupStart, 0)
      const selectEnd = Math.min((rowEnd ?? Infinity) - asyncGroup.groupStart, asyncGroup.groupRows)
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
    for (const { asyncColumns } of assembled) {
      for (const { data } of asyncColumns) await data
    }
  }
}

/**
 * @param {ParquetReadOptions} options read options
 * @returns {AsyncRowGroup[]}
 */
export function parquetReadAsync(options) {
  if (!options.metadata) throw new Error('parquet requires metadata')
  // TODO: validate options (start, end, columns, etc)

  // prefetch byte ranges
  const plan = parquetPlan(options)
  options.file = prefetchAsyncBuffer(options.file, plan)

  // read row groups
  return plan.groups.map(groupPlan => readRowGroup(options, plan, groupPlan))
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
  const asyncGroups = parquetReadAsync(options)

  // assemble struct columns
  const schemaTree = parquetSchema(options.metadata)
  const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree))

  /** @type {DecodedArray[]} */
  const columnData = []
  for (const rg of assembled) {
    columnData.push(flatten((await rg.asyncColumns[0].data).data))
  }
  return flatten(columnData)
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
