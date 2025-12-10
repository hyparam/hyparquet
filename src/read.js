import { columnsNeededForFilter, matchFilter } from './filter.js'
import { parquetMetadata, parquetSchema } from './metadata.js'
import { parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { assembleRows, readRowGroup } from './rowgroup.js'
import { concat, flatten, flattenAsync } from './utils.js'

/**
 * @import {AsyncRowGroup, DecodedArray, ParquetReadOptions, BaseParquetReadOptions, FileMetaData} from '../src/types.js'
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
  const { rowStart = 0, rowEnd = Infinity, columns, onComplete, filter, filterStrict = true } = options

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

  // assemble struct columns
  const schemaTree = parquetSchema(options.metadata)

  // onComplete transpose column chunks to rows
  if (onComplete) {
    /** @type {Record<string, any>[]} */
    const rows = []
    for await (const rowGroup of asyncGroups) {
      const selectStart = Math.max(rowStart - rowGroup.groupStart, 0)
      const selectEnd = Math.min(rowEnd - rowGroup.groupStart, rowGroup.groupRows)
      const transposed = await assembleRows({
        rowGroup,
        schemaTree,
        selectStart,
        selectEnd,
        columns: readColumns,
        onPage: options.onPage,
        onChunk: options.onChunk,
      })

      // Apply filter and projection
      if (filter) {
        for (const row of transposed) {
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
        concat(rows, transposed)
      }
    }
    onComplete(rows)
  }
}

/**
 * Asynchronously read parquet data according to options.
 *
 * @param {ParquetReadOptions} options
 * @yields {AsyncRowGroup}
 */
export async function* parquetReadAsync(options) {
  // load metadata if not provided
  options.metadata ??= await parquetMetadata(options)
  yield* parquetReadIter({ ...options, metadata: options.metadata })
}

/**
 * @param {ParquetReadOptions & { metadata: FileMetaData }} options
 * @yields {AsyncRowGroup}
 */
export function* parquetReadIter(options) {
  // TODO: validate options (start, end, columns, etc)
  const plan = parquetPlan(options)

  // prefetch byte ranges
  if (options.prefetch !== false) {
    options.file = prefetchAsyncBuffer(options.file, plan)
  }

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
    const resolved = await flattenAsync(rg.asyncColumns[0])
    columnData.push(resolved.data)
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
