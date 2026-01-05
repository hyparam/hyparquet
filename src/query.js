import { parquetMetadata, parquetSchema } from './metadata.js'
import { parquetReadColumn, parquetReadObjects } from './read.js'

/**
 * @import {BaseParquetReadOptions} from '../src/types.js'
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
  options.metadata ??= await parquetMetadata(options)

  const { metadata, rowStart = 0, columns, orderBy, filter } = options
  if (rowStart < 0) throw new Error('parquet rowStart must be positive')
  const rowEnd = options.rowEnd ?? Number(metadata.num_rows)

  // Validate orderBy column exists
  if (orderBy) {
    const allColumns = parquetSchema(options.metadata).children.map(c => c.element.name)
    if (!allColumns.includes(orderBy)) {
      throw new Error(`parquet orderBy column not found: ${orderBy}`)
    }
  }

  if (filter && !orderBy && rowEnd < metadata.num_rows) {
    // iterate through row groups and filter until we have enough rows
    /** @type {Record<string, any>[]} */
    const filteredRows = []
    let groupStart = 0
    for (const group of metadata.row_groups) {
      const groupEnd = groupStart + Number(group.num_rows)
      // TODO: if expected > group size, start fetching next groups
      const groupData = await parquetReadObjects({
        ...options, rowStart: groupStart, rowEnd: groupEnd,
      })
      filteredRows.push(...groupData)
      if (filteredRows.length >= rowEnd) break
      groupStart = groupEnd
    }
    return filteredRows.slice(rowStart, rowEnd)
  } else if (filter && orderBy) {
    // read all rows with orderBy column included for sorting
    const readColumns = columns && !columns.includes(orderBy)
      ? [...columns, orderBy]
      : columns

    const results = await parquetReadObjects({
      ...options, rowStart: undefined, rowEnd: undefined, columns: readColumns,
    })

    // sort by orderBy column
    results.sort((a, b) => compare(a[orderBy], b[orderBy]))

    // project out orderBy column if not originally requested
    if (readColumns !== columns) {
      for (const row of results) {
        delete row[orderBy]
      }
    }

    return results.slice(rowStart, rowEnd)
  } else if (filter) {
    // filter without orderBy, read all matching rows
    const results = await parquetReadObjects({
      ...options, rowStart: undefined, rowEnd: undefined,
    })
    return results.slice(rowStart, rowEnd)
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
  options.metadata ??= await parquetMetadata(options)
  const { row_groups: rowGroups } = options.metadata
  // Compute row groups to fetch
  const groupIncluded = Array(rowGroups.length).fill(false)
  let groupStart = 0
  const groupEnds = rowGroups.map(group => groupStart += Number(group.num_rows))
  for (const index of options.rows) {
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
  const sparseData = Array(Number(options.metadata.num_rows))
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
