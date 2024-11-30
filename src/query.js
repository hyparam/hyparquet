import { parquetReadObjects } from './hyparquet.js'
import { parquetMetadataAsync } from './metadata.js'

/**
 * Wraps parquetRead with orderBy support.
 * This is a parquet-aware query engine that can read a subset of rows and columns.
 * Accepts an optional orderBy column name to sort the results.
 * Note that using orderBy may SIGNIFICANTLY increase the query time.
 *
 * @param {ParquetReadOptions & { orderBy?: string }} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export async function parquetQuery(options) {
  const { file, rowStart, rowEnd, orderBy } = options
  options.metadata ||= await parquetMetadataAsync(file)

  // TODO: Faster path for: no orderBy, no rowStart/rowEnd, one row group

  if (typeof orderBy === 'string') {
    // Fetch orderBy column first
    const orderColumn = await parquetReadObjects({ ...options, rowStart: undefined, rowEnd: undefined, columns: [orderBy] })

    // Compute row groups to fetch
    const sortedIndices = Array.from(orderColumn, (_, index) => index)
      .sort((a, b) => compare(orderColumn[a][orderBy], orderColumn[b][orderBy]))
      .slice(rowStart, rowEnd)

    const sparseData = await parquetReadRows({ ...options, rows: sortedIndices })
    const data = sortedIndices.map(index => sparseData[index])
    return data
  } else {
    return await parquetReadObjects(options)
  }
}

/**
 * Reads a list rows from a parquet file, reading only the row groups that contain the rows.
 * Returns a sparse array of rows.
 * @import {ParquetReadOptions} from '../src/types.d.ts'
 * @param {ParquetReadOptions & { rows: number[] }} options
 * @returns {Promise<Record<string, any>[]>}
 */
async function parquetReadRows(options) {
  const { file, rows } = options
  options.metadata ||= await parquetMetadataAsync(file)
  const { row_groups: rowGroups } = options.metadata
  // Compute row groups to fetch
  const groupIncluded = Array(rowGroups.length).fill(false)
  let groupStart = 0
  const groupEnds = rowGroups.map(group => groupStart += Number(group.num_rows))
  for (const index of rows) {
    const groupIndex = groupEnds.findIndex(end => index < end)
    groupIncluded[groupIndex] = true
  }

  // Compute row ranges to fetch
  const rowRanges = []
  let rangeStart
  groupStart = 0
  for (let i = 0; i < groupIncluded.length; i++) {
    const groupEnd = groupStart + Number(rowGroups[i].num_rows)
    if (groupIncluded[i]) {
      if (rangeStart === undefined) {
        rangeStart = groupStart
      }
    } else {
      if (rangeStart !== undefined) {
        rowRanges.push([rangeStart, groupEnd])
        rangeStart = undefined
      }
    }
    groupStart = groupEnd
  }
  if (rangeStart !== undefined) {
    rowRanges.push([rangeStart, groupStart])
  }

  // Fetch by row group and map to rows
  const sparseData = new Array(Number(options.metadata.num_rows))
  for (const [rangeStart, rangeEnd] of rowRanges) {
    // TODO: fetch in parallel
    const groupData = await parquetReadObjects({ ...options, rowStart: rangeStart, rowEnd: rangeEnd })
    for (let i = rangeStart; i < rangeEnd; i++) {
      sparseData[i] = groupData[i - rangeStart]
      sparseData[i].__index__ = i
    }
  }
  return sparseData
}

/**
 * @param {any} a
 * @param {any} b
 * @returns {number}
 */
function compare(a, b) {
  if (a < b) return -1
  if (a > b) return 1
  return 1 // TODO: how to handle nulls?
}
