import { parquetReadObjects } from './hyparquet.js'
import { parquetMetadataAsync } from './metadata.js'

/**
 * Wraps parquetRead with filter and orderBy support.
 * This is a parquet-aware query engine that can read a subset of rows and columns.
 * Accepts optional filter object to filter the results and orderBy column name to sort the results.
 * Note that using orderBy may SIGNIFICANTLY increase the query time.
 *
 * @import {ParquetQueryFilter} from '../src/types.d.ts'
 * @param {ParquetReadOptions & { filter?: ParquetQueryFilter, orderBy?: string }} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export async function parquetQuery(options) {
  const { file, rowStart, rowEnd, orderBy, filter } = options
  options.metadata ||= await parquetMetadataAsync(file)

  // TODO: Faster path for: no orderBy, no rowStart/rowEnd, one row group

  if (typeof orderBy === 'string') {
    // Fetch orderBy column first
    const orderColumn = await parquetReadObjects({ ...options, rowStart: undefined, rowEnd: undefined, columns: [orderBy] })

    // Compute row groups to fetch
    const sortedIndices = Array.from(orderColumn, (_, index) => index)
      .sort((a, b) => compare(orderColumn[a][orderBy], orderColumn[b][orderBy]))

    const sparseData = await parquetReadRows({ ...options, rows: sortedIndices })
    return sortedIndices.map(index => sparseData[index])
      .filter((doc) => !filter || matchQuery(doc, filter))
      .slice(rowStart, rowEnd)
  } else {
    // TODO: Move filter to parquetRead for performance
    const results = await parquetReadObjects(options)
    return filter ? results.filter(row => matchQuery(row, filter)) : results
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

/**
 * Match a record against a query filter
 *
 * @param {any} record
 * @param {ParquetQueryFilter} query
 * @returns {boolean}
 * @example matchQuery({ id: 1 }, { id: {$gte: 1} }) // true
 */
export function matchQuery(record, query = {}) {
  if (query.$and) {
    return query.$and.every(subQuery => matchQuery(record, subQuery))
  }

  if (query.$or) {
    return query.$or.some(subQuery => matchQuery(record, subQuery))
  }

  return Object.entries(query).every(([field, condition]) => {
    const value = record[field]

    if (condition !== null && typeof condition !== 'object') {
      return value === condition
    }

    return Object.entries(condition || {}).every(([operator, target]) => {
      switch (operator) {
      case '$gt':
        return value > target
      case '$gte':
        return value >= target
      case '$lt':
        return value < target
      case '$lte':
        return value <= target
      case '$ne':
        return value !== target
      case '$in':
        return Array.isArray(target) && target.includes(value)
      case '$nin':
        return Array.isArray(target) && !target.includes(value)
      case '$not':
        return !matchQuery({ [field]: value }, { [field]: target })
      default:
        return true
      }
    })
  })
}
