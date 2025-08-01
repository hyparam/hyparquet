import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { parquetReadColumn, parquetReadObjects } from './read.js'
import { equals } from './utils.js'

/**
 * Wraps parquetRead with filter and orderBy support.
 * This is a parquet-aware query engine that can read a subset of rows and columns.
 * Accepts optional filter object to filter the results and orderBy column name to sort the results.
 * Note that using orderBy may SIGNIFICANTLY increase the query time.
 * @import {BaseParquetReadOptions} from '../src/types.d.ts'
 * @param {BaseParquetReadOptions & { filter?: ParquetQueryFilter, orderBy?: string }} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export async function parquetQuery(options) {
  if (!options.file || !(options.file.byteLength >= 0)) {
    throw new Error('parquet expected AsyncBuffer')
  }
  options.metadata ??= await parquetMetadataAsync(options.file)

  const { metadata, rowStart = 0, columns, orderBy, filter } = options
  if (rowStart < 0) throw new Error('parquet rowStart must be positive')
  const rowEnd = options.rowEnd ?? Number(metadata.num_rows)

  // Collect columns needed for the query
  const filterColumns = columnsNeededForFilter(filter)
  const allColumns = parquetSchema(options.metadata).children.map(c => c.element.name)
  // Check if all filter columns exist
  const missingColumns = filterColumns.filter(column => !allColumns.includes(column))
  if (missingColumns.length) {
    throw new Error(`parquet filter columns not found: ${missingColumns.join(', ')}`)
  }
  if (orderBy && !allColumns.includes(orderBy)) {
    throw new Error(`parquet orderBy column not found: ${orderBy}`)
  }
  const relevantColumns = columns ? allColumns.filter(column =>
    columns.includes(column) || filterColumns.includes(column) || column === orderBy
  ) : undefined
  // Is the output a subset of the relevant columns?
  const requiresProjection = columns && relevantColumns ? columns.length < relevantColumns.length : false

  if (filter && !orderBy && rowEnd < metadata.num_rows) {
    // iterate through row groups and filter until we have enough rows
    const filteredRows = new Array()
    let groupStart = 0
    for (const group of metadata.row_groups) {
      const groupEnd = groupStart + Number(group.num_rows)
      // TODO: if expected > group size, start fetching next groups
      const groupData = await parquetReadObjects({
        ...options,
        rowStart: groupStart,
        rowEnd: groupEnd,
        columns: relevantColumns,
      })
      for (const row of groupData) {
        if (matchQuery(row, filter)) {
          if (requiresProjection && relevantColumns) {
            for (const column of relevantColumns) {
              if (columns && !columns.includes(column)) {
                delete row[column] // remove columns not in the projection
              }
            }
          }
          filteredRows.push(row)
        }
      }
      if (filteredRows.length >= rowEnd) break
      groupStart = groupEnd
    }
    return filteredRows.slice(rowStart, rowEnd)
  } else if (filter) {
    // read all rows, sort, and filter
    const results = await parquetReadObjects({
      ...options,
      rowStart: undefined,
      rowEnd: undefined,
      columns: relevantColumns,
    })
    if (orderBy) results.sort((a, b) => compare(a[orderBy], b[orderBy]))
    const filteredRows = new Array()
    for (const row of results) {
      if (matchQuery(row, filter)) {
        if (requiresProjection && relevantColumns) {
          for (const column of relevantColumns) {
            if (columns && !columns.includes(column)) {
              delete row[column] // remove columns not in the projection
            }
          }
        }
        filteredRows.push(row)
      }
    }
    return filteredRows.slice(rowStart, rowEnd)
  } else if (typeof orderBy === 'string') {
    // sorted but unfiltered: fetch orderBy column first
    const orderColumn = await parquetReadColumn({ ...options, rowStart: undefined, rowEnd: undefined, columns: [orderBy] })

    // compute row groups to fetch
    const sortedIndices = Array.from(orderColumn, (_, index) => index)
      .sort((a, b) => compare(orderColumn[a], orderColumn[b]))
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
 * @import {ParquetQueryFilter} from '../src/types.d.ts'
 * @param {BaseParquetReadOptions & { rows: number[] }} options
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
  return 0 // TODO: null handling
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
  if ('$and' in query && Array.isArray(query.$and)) {
    return query.$and.every(subQuery => matchQuery(record, subQuery))
  }
  if ('$or' in query && Array.isArray(query.$or)) {
    return query.$or.some(subQuery => matchQuery(record, subQuery))
  }
  if ('$nor' in query && Array.isArray(query.$nor)) {
    return !query.$nor.some(subQuery => matchQuery(record, subQuery))
  }

  return Object.entries(query).every(([field, condition]) => {
    const value = record[field]

    // implicit $eq for non-object conditions
    if (typeof condition !== 'object' || condition === null || Array.isArray(condition)) {
      return equals(value, condition)
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
      case '$eq':
        return equals(value, target)
      case '$ne':
        return !equals(value, target)
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

/**
 * Returns an array of column names that are needed to evaluate the mongo filter.
 *
 * @param {ParquetQueryFilter} [filter]
 * @returns {string[]}
 */
function columnsNeededForFilter(filter) {
  if (!filter) return []
  /** @type {string[]} */
  const columns = []
  if ('$and' in filter && Array.isArray(filter.$and)) {
    columns.push(...filter.$and.flatMap(columnsNeededForFilter))
  } else if ('$or' in filter && Array.isArray(filter.$or)) {
    columns.push(...filter.$or.flatMap(columnsNeededForFilter))
  } else if ('$nor' in filter && Array.isArray(filter.$nor)) {
    columns.push(...filter.$nor.flatMap(columnsNeededForFilter))
  } else {
    // Column filters
    columns.push(...Object.keys(filter))
  }
  return columns
}
