import { matchFilter } from './filter.js'
import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { parquetReadColumn, parquetReadObjects } from './read.js'

/**
 * @import {ParquetQueryFilter, BaseParquetReadOptions} from '../src/types.js'
 */
/**
 * Wraps parquetRead with orderBy support.
 * This is a parquet-aware query engine that can read a subset of rows and columns.
 * Accepts optional orderBy column name to sort the results.
 * Note that using orderBy may SIGNIFICANTLY increase the query time.
 *
 * @param {BaseParquetReadOptions & { orderBy?: string }} options
 * @returns {Promise<Record<string, any>[]>} resolves when all requested rows and columns are parsed
 */
export async function parquetQuery(options) {
  if (!options.file || !(options.file.byteLength >= 0)) {
    throw new Error('parquet expected AsyncBuffer')
  }
  options.metadata ??= await parquetMetadataAsync(options.file, options)

  const { metadata, rowStart = 0, columns, orderBy, filter, filterStrict = true } = options
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
    /** @type {Record<string, any>[]} */
    const filteredRows = new Array()
    let groupStart = 0
    for (const group of metadata.row_groups) {
      const groupEnd = groupStart + Number(group.num_rows)
      // TODO: if expected > group size, start fetching next groups
      const groupData = await parquetReadObjects({
        ...options, rowStart: groupStart, rowEnd: groupEnd, columns: relevantColumns,
      })
      // filter and project rows
      for (const row of groupData) {
        if (matchFilter(row, filter, filterStrict)) {
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
      ...options, rowStart: undefined, rowEnd: undefined, columns: relevantColumns,
    })

    // sort
    if (orderBy) results.sort((a, b) => compare(a[orderBy], b[orderBy]))

    // filter and project rows
    /** @type {Record<string, any>[]} */
    const filteredRows = new Array()
    for (const row of results) {
      if (matchFilter(row, filter, filterStrict)) {
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
    const orderColumn = await parquetReadColumn({
      ...options, rowStart: undefined, rowEnd: undefined, columns: [orderBy],
    })

    // compute row groups to fetch
    const sortedIndices = Array.from(orderColumn, (_, index) => index)
      .sort((a, b) => compare(orderColumn[a], orderColumn[b]))
      .slice(rowStart, rowEnd)

    const sparseData = await parquetReadRows({ ...options, rows: sortedIndices })
    // warning: the type Record<string, any> & {__index__: number})[] is simplified into Record<string, any>[]
    // when returning. The data contains the __index__ property, but it's not exposed as such.
    const data = sortedIndices.map(index => sparseData[index])
    return data
  } else {
    return await parquetReadObjects(options)
  }
}

/**
 * Reads a list rows from a parquet file, reading only the row groups that contain the rows.
 * Returns a sparse array of rows.
 * @param {BaseParquetReadOptions & { rows: number[] }} options
 * @returns {Promise<(Record<string, any> & {__index__: number})[]>}
 */
async function parquetReadRows(options) {
  const { file, rows } = options
  options.metadata ??= await parquetMetadataAsync(file, options)
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
  /** @type {(Record<string, any> & {__index__: number})[]} */
  const sparseData = new Array(Number(options.metadata.num_rows))
  for (const [rangeStart, rangeEnd] of rowRanges) {
    // TODO: fetch in parallel
    const groupData = await parquetReadObjects({ ...options, rowStart: rangeStart, rowEnd: rangeEnd })
    for (let i = rangeStart; i < rangeEnd; i++) {
      // warning: if the row contains a column named __index__, it will overwrite the index.
      sparseData[i] = { __index__: i, ...groupData[i - rangeStart] }
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
