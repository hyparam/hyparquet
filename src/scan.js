/**
 * @import {AsyncRowGroup, BaseParquetReadOptions, BloomFilter, DecodedArray, PageLocation, PageRanges, ParquetScan, ParquetScanOptions, QueryPlan, SchemaElement} from '../src/types.js'
 */

/**
 * @typedef {BaseParquetReadOptions & {
 *   bloomFiltersByGroup?: Record<string, BloomFilter>[],
 *   schemaElements?: Record<string, SchemaElement>,
 *   pageRangesByGroup?: (PageRanges | undefined)[],
 *   pageLocationsByGroup?: Record<string, PageLocation[]>[],
 * }} PreparedParquetReadOptions
 */

import { columnsNeededForFilter } from './filter.js'
import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { parquetPlan, parquetPlanGroup, parquetPlanGroups, prefetchAsyncBuffer, prefetchBloomFilters, prefetchPageIndexes } from './plan.js'
import { assembleAsync, readRowGroup } from './rowgroup.js'
import { flatten } from './utils.js'

/**
 * Prepare a lazy column-oriented scan of a parquet file.
 *
 * The returned ranges are physical, zero-based row ranges in the file. A
 * `pruningFilter` may narrow them using row-group statistics, bloom filters,
 * and page indexes, but does not filter individual values returned by
 * `readColumn`. This makes the scan suitable for query engines that maintain
 * their own row selections while retaining parquet-level I/O pushdown.
 *
 * `columns`, when supplied, limits the columns available to `readColumn`.
 * Columns referenced by `pruningFilter` are included automatically.
 *
 * @param {ParquetScanOptions} options
 * @returns {Promise<ParquetScan>}
 */
export async function parquetScan(options) {
  options.metadata ??= await parquetMetadataAsync(options.file, options)
  const { pruningFilter, ...readOptions } = options
  let { columns } = readOptions
  if (columns && pruningFilter) {
    const selectedColumns = new Set(columns)
    const extraColumns = columnsNeededForFilter(pruningFilter)
      .filter(column => !selectedColumns.has(column))
    if (extraColumns.length) columns = [...columns, ...extraColumns]
  }
  const preparedOptions = await prepareParquetOptions({
    ...readOptions,
    columns,
    filter: pruningFilter,
    useOffsetIndex: readOptions.useOffsetIndex ?? true,
  })
  const { metadata } = preparedOptions
  if (!metadata) throw new Error('parquet requires metadata')

  const schemaTree = parquetSchema(metadata)
  const availableColumns = new Set(columns ?? schemaTree.children.map(child => child.element.name))
  const plan = parquetPlanGroups(preparedOptions)
  const candidates = plan.groups.flatMap(group => group.ranges.map(([selectStart, selectEnd]) => ({
    rowStart: group.groupStart + selectStart,
    rowEnd: group.groupStart + selectEnd,
    group,
  })))
  const ranges = candidates.map(({ rowStart, rowEnd }) => ({ rowStart, rowEnd }))
  /** @type {Map<string, {rowStart: number, rowEnd: number, data: Promise<DecodedArray>}>} */
  const cache = new Map()

  /**
   * @param {import('../src/types.js').ParquetScanColumnOptions} columnOptions
   * @returns {Promise<DecodedArray>}
   */
  function readColumn({ column, rowStart, rowEnd }) {
    if (!availableColumns.has(column)) {
      throw new Error(`parquet column not found in scan: ${column}`)
    }
    validateRange(rowStart, rowEnd)
    if (rowStart === rowEnd) return Promise.resolve([])
    const candidateIndex = findCandidate(candidates, rowStart, rowEnd)
    if (candidateIndex < 0) {
      throw new RangeError(`parquet row range [${rowStart}, ${rowEnd}) is outside scan ranges`)
    }
    const { group } = candidates[candidateIndex]

    const cached = cache.get(column)
    if (cached && rowStart >= cached.rowStart && rowEnd <= cached.rowEnd) {
      return cached.data.then(data => sliceDecodedArray(
        data,
        rowStart - cached.rowStart,
        rowEnd - cached.rowStart
      ))
    }

    const data = readColumnRange({
      options: preparedOptions,
      schemaTree,
      group,
      column,
      rowStart,
      rowEnd,
    })
    cache.set(column, { rowStart, rowEnd, data })
    return data
  }

  return {
    metadata,
    ranges,
    readColumn,
  }
}

/**
 * Load optional indexes and build the canonical physical read plan.
 * Shared by row-oriented reads and lazy scans so pruning behavior cannot
 * diverge between APIs.
 *
 * @param {BaseParquetReadOptions} options
 * @returns {Promise<{options: PreparedParquetReadOptions, plan: QueryPlan}>}
 */
export async function prepareParquetRead(options) {
  const prepared = await prepareParquetOptions(options)
  return {
    options: prepared,
    plan: parquetPlan(prepared),
  }
}

/**
 * Load optional indexes and validate read options without planning data reads.
 *
 * @param {BaseParquetReadOptions} options
 * @returns {Promise<PreparedParquetReadOptions>}
 */
async function prepareParquetOptions(options) {
  const metadata = options.metadata ?? await parquetMetadataAsync(options.file, options)
  const schemaColumns = parquetSchema(metadata).children.map(child => child.element.name)
  const filterColumns = columnsNeededForFilter(options.filter)
  const missingFilterColumns = filterColumns.filter(column => !schemaColumns.includes(column))
  if (missingFilterColumns.length) {
    throw new Error(`parquet filter columns not found: ${missingFilterColumns.join(', ')}`)
  }

  if (options.columns) {
    const missingColumns = options.columns.filter(column => !schemaColumns.includes(column))
    if (missingColumns.length) throw new Error(`parquet column not found: ${missingColumns[0]}`)
  }

  /** @type {PreparedParquetReadOptions} */
  let prepared = { ...options, metadata }
  prepared = await withBloomFilters(prepared)
  prepared = await withPageIndexes(prepared)
  return prepared
}

/**
 * Read all planned row groups, prefetching their coalesced byte ranges.
 *
 * @param {BaseParquetReadOptions} options
 * @param {QueryPlan} plan
 * @returns {AsyncRowGroup[]}
 */
export function readParquetPlan(options, plan) {
  const readOptions = { ...options, file: prefetchAsyncBuffer(options.file, plan) }
  return plan.groups.map(group => readRowGroup(readOptions, plan, group))
}

/**
 * @param {object} input
 * @param {PreparedParquetReadOptions} input.options
 * @param {import('../src/types.js').SchemaTree} input.schemaTree
 * @param {ReturnType<typeof parquetPlanGroups>['groups'][number]} input.group
 * @param {string} input.column
 * @param {number} input.rowStart
 * @param {number} input.rowEnd
 * @returns {Promise<DecodedArray>}
 */
async function readColumnRange({ options, schemaTree, group, column, rowStart, rowEnd }) {
  const columnOptions = {
    ...options,
    columns: [column],
    filter: undefined,
    rowStart,
    rowEnd,
    useBloomFilters: false,
    useOffsetIndex: options.useOffsetIndex ?? true,
    usePageIndex: false,
    pageRangesByGroup: undefined,
  }
  const columnPlan = planCandidateColumn({ options, group, column, rowStart, rowEnd })
  const [asyncGroup] = readParquetPlan(columnOptions, columnPlan)
  if (!asyncGroup) throw new Error('parquet scan range not planned')
  const assembled = assembleAsync(asyncGroup, schemaTree, options.parsers)
  const asyncColumn = assembled.asyncColumns.find(child => child.pathInSchema[0] === column)
  if (!asyncColumn) throw new Error(`parquet column not found: ${column}`)
  const result = await asyncColumn.data
  const data = flatten(result.data)
  const selectStart = assembled.selectStart ?? rowStart - assembled.groupStart
  const selectEnd = assembled.selectEnd ?? rowEnd - assembled.groupStart
  return sliceDecodedArray(data, selectStart - result.skipped, selectEnd - result.skipped)
}

/**
 * Build a one-column plan from a retained candidate without scanning the
 * file's row groups again.
 *
 * @param {object} input
 * @param {PreparedParquetReadOptions} input.options
 * @param {ReturnType<typeof parquetPlanGroups>['groups'][number]} input.group
 * @param {string} input.column
 * @param {number} input.rowStart
 * @param {number} input.rowEnd
 * @returns {QueryPlan}
 */
function planCandidateColumn({ options, group, column, rowStart, rowEnd }) {
  if (!options.metadata) throw new Error('parquet requires metadata')
  const localPlan = parquetPlanGroup({
    rowGroup: group.rowGroup,
    groupStart: group.groupStart,
    groupRows: group.groupRows,
    ranges: [[rowStart - group.groupStart, rowEnd - group.groupStart]],
    columns: [column],
    useOffsetIndex: options.useOffsetIndex ?? true,
    pageLocations: group.pageLocations,
  })
  const [localGroupPlan] = localPlan.groups
  if (!localGroupPlan) throw new Error(`parquet column not found: ${column}`)
  return {
    metadata: options.metadata,
    rowStart,
    rowEnd,
    columns: [column],
    fetches: [...localPlan.fetches, ...localPlan.indexes],
    groups: [localGroupPlan],
  }
}

/**
 * Find the one candidate that fully contains a requested non-empty range.
 * Candidates are sorted and disjoint, so lookup is logarithmic.
 *
 * @param {{rowStart: number, rowEnd: number}[]} candidates
 * @param {number} rowStart
 * @param {number} rowEnd
 * @returns {number}
 */
function findCandidate(candidates, rowStart, rowEnd) {
  let low = 0
  let high = candidates.length - 1
  while (low <= high) {
    const middle = Math.floor((low + high) / 2)
    const candidate = candidates[middle]
    if (rowStart < candidate.rowStart) {
      high = middle - 1
    } else if (rowStart >= candidate.rowEnd) {
      low = middle + 1
    } else {
      return rowEnd <= candidate.rowEnd ? middle : -1
    }
  }
  return -1
}

/**
 * @param {number} rowStart
 * @param {number} rowEnd
 */
function validateRange(rowStart, rowEnd) {
  if (!Number.isSafeInteger(rowStart) || rowStart < 0) {
    throw new RangeError('parquet rowStart must be a non-negative safe integer')
  }
  if (!Number.isSafeInteger(rowEnd) || rowEnd < rowStart) {
    throw new RangeError('parquet rowEnd must be a safe integer greater than or equal to rowStart')
  }
}

/**
 * Return an exact view over decoded values. Typed arrays retain a zero-copy
 * subarray while ordinary arrays use their native slice.
 *
 * @param {DecodedArray} data
 * @param {number} start
 * @param {number} end
 * @returns {DecodedArray}
 */
function sliceDecodedArray(data, start, end) {
  if (start === 0 && end === data.length) return data
  return Array.isArray(data) ? data.slice(start, end) : data.subarray(start, end)
}

/**
 * Conditionally fetch bloom filters and attach them to planner options.
 *
 * @param {PreparedParquetReadOptions} options
 * @returns {Promise<PreparedParquetReadOptions>}
 */
async function withBloomFilters(options) {
  if (!options.useBloomFilters || !options.filter || !options.metadata) return options
  const schemaTree = parquetSchema(options.metadata)
  /** @type {Record<string, SchemaElement>} */
  const schemaElements = {}
  for (const child of schemaTree.children) schemaElements[child.element.name] = child.element
  const bloomFiltersByGroup = await prefetchBloomFilters({
    file: options.file,
    metadata: options.metadata,
    filter: options.filter,
    filterStrict: options.filterStrict,
  })
  return { ...options, bloomFiltersByGroup, schemaElements }
}

/**
 * Conditionally fetch page indexes and attach candidate ranges and layouts.
 *
 * @param {PreparedParquetReadOptions} options
 * @returns {Promise<PreparedParquetReadOptions>}
 */
async function withPageIndexes(options) {
  if (!options.usePageIndex || !options.filter || !options.metadata) return options
  const { pageRangesByGroup, pageLocationsByGroup } = await prefetchPageIndexes({
    file: options.file,
    metadata: options.metadata,
    filter: options.filter,
    filterStrict: options.filterStrict,
    rowStart: options.rowStart,
    rowEnd: options.rowEnd,
    columns: options.columns,
    bloomFiltersByGroup: options.bloomFiltersByGroup,
    schemaElements: options.schemaElements,
    parsers: options.parsers,
  })
  return { ...options, pageRangesByGroup, pageLocationsByGroup }
}
