import { concat } from './utils.js'

// Threshold for combining adjacent column chunks into a single read
const columnChunkAggregation = 1 << 25 // 32mb

/**
 * @import {AsyncBuffer, ByteRange, RowGroup, ColumnMetaData, GroupPlan, ParquetReadOptions, QueryPlan} from '../src/types.js'
 */
/**
 * Plan which byte ranges to read to satisfy a read request.
 * Metadata must be non-null.
 *
 * @param {ParquetReadOptions} options
 * @returns {QueryPlan}
 */
export function parquetPlan({ metadata, rowStart = 0, rowEnd = Infinity, columns }) {
  if (!metadata) throw new Error('parquetPlan requires metadata')
  /** @type {GroupPlan[]} */
  const groups = []
  /** @type {ByteRange[]} */
  const fetches = []

  // find which row groups to read
  let groupStart = 0 // first row index of the current group
  for (const rowGroup of metadata.row_groups) {
    const groupRows = Number(rowGroup.num_rows)
    const groupEnd = groupStart + groupRows
    // if row group overlaps with row range, add it to the plan
    if (groupRows > 0 && groupEnd >= rowStart && groupStart < rowEnd) {
      /** @type {ByteRange[]} */
      const ranges = []
      // loop through each column chunk
      for (const { file_path, meta_data } of rowGroup.columns) {
        if (file_path) throw new Error('parquet file_path not supported')
        if (!meta_data) throw new Error('parquet column metadata is undefined')
        // add included columns to the plan
        if (!columns || columns.includes(meta_data.path_in_schema[0])) {
          ranges.push(getColumnRange(meta_data))
        }
      }
      const selectStart = Math.max(rowStart - groupStart, 0)
      const selectEnd = Math.min(rowEnd - groupStart, groupRows)
      groups.push({ ranges, rowGroup, groupStart, groupRows, selectStart, selectEnd })

      // map group plan to ranges
      const groupSize = ranges[ranges.length - 1]?.endByte - ranges[0]?.startByte
      if (!columns && groupSize < columnChunkAggregation) {
        // full row group
        fetches.push({
          startByte: ranges[0].startByte,
          endByte: ranges[ranges.length - 1].endByte,
        })
      } else if (ranges.length) {
        concat(fetches, ranges)
      } else if (columns?.length) {
        throw new Error(`parquet columns not found: ${columns.join(', ')}`)
      }
    }

    groupStart = groupEnd
  }
  if (!isFinite(rowEnd)) rowEnd = groupStart

  return { metadata, rowStart, rowEnd, columns, fetches, groups }
}

/**
 * @param {ColumnMetaData} columnMetadata
 * @returns {ByteRange}
 */
export function getColumnRange({ dictionary_page_offset, data_page_offset, total_compressed_size }) {
  const columnOffset = dictionary_page_offset || data_page_offset
  return {
    startByte: Number(columnOffset),
    endByte: Number(columnOffset + total_compressed_size),
  }
}

/**
 * Prefetch byte ranges from an AsyncBuffer.
 *
 * @param {AsyncBuffer} file
 * @param {QueryPlan} plan
 * @returns {AsyncBuffer}
 */
export function prefetchAsyncBuffer(file, { fetches }) {
  // fetch byte ranges from the file
  const promises = fetches.map(({ startByte, endByte }) => file.slice(startByte, endByte))
  return {
    byteLength: file.byteLength,
    slice(start, end = file.byteLength) {
      // find matching slice
      const index = fetches.findIndex(({ startByte, endByte }) => startByte <= start && end <= endByte)
      if (index < 0) throw new Error(`no prefetch for range [${start}, ${end}]`)
      if (fetches[index].startByte !== start || fetches[index].endByte !== end) {
        // slice a subrange of the prefetch
        const startOffset = start - fetches[index].startByte
        const endOffset = end - fetches[index].startByte
        if (promises[index] instanceof Promise) {
          return promises[index].then(buffer => buffer.slice(startOffset, endOffset))
        } else {
          return promises[index].slice(startOffset, endOffset)
        }
      } else {
        return promises[index]
      }
    },
  }
}

/**
 * Calculate the total byte range of a row group including indexes.
 * Includes column data and any page indexes for complete row group reads.
 *
 * @param {RowGroup} rowGroup - row group metadata
 * @returns {{start: number, end: number, size: number}} byte range and size
 */
export function getRowGroupFullRange(rowGroup) {
  let start = Infinity
  let end = 0

  for (const col of rowGroup.columns) {
    if (col.meta_data) {
      // Column data range
      const colStart = Number(col.meta_data.dictionary_page_offset || col.meta_data.data_page_offset)
      const colEnd = colStart + Number(col.meta_data.total_compressed_size)
      start = Math.min(start, colStart)
      end = Math.max(end, colEnd)

      // Include column index if present
      if (col.column_index_offset) {
        const indexEnd = Number(col.column_index_offset) + Number(col.column_index_length)
        end = Math.max(end, indexEnd)
      }

      // Include offset index if present
      if (col.offset_index_offset) {
        const offsetEnd = Number(col.offset_index_offset) + Number(col.offset_index_length)
        end = Math.max(end, offsetEnd)
      }
    }
  }

  return { start, end, size: end - start }
}

/**
 * Create column name to index mapping for a row group.
 * Enables lookup by name since row groups store columns by index.
 *
 * @param {RowGroup} rowGroup - row group metadata
 * @returns {Map<string, number>} map from column name to index
 */
export function createColumnIndexMap(rowGroup) {
  const map = new Map()
  rowGroup.columns.forEach((column, index) => {
    if (column.meta_data?.path_in_schema?.length && column.meta_data.path_in_schema.length > 0) {
      map.set(column.meta_data.path_in_schema[0], index)
    }
  })
  return map
}

/**
 * Extract column names from filter.
 * Needed to read filter columns that may not be in the output.
 *
 * @param {object} filter - MongoDB-style filter object
 * @returns {string[]} array of column names referenced in filter
 */
export function extractFilterColumns(filter) {
  const columns = new Set()

  /**
   * @param {any} f
   */
  function extract(f) {
    if (f.$and || f.$or || f.$nor) {
      (f.$and || f.$or || f.$nor).forEach(extract)
    } else if (f.$not) {
      extract(f.$not)
    } else {
      Object.keys(f).forEach((k) => {
        if (!k.startsWith('$')) columns.add(k)
      })
    }
  }

  extract(filter)
  return [...columns]
}

/**
 * Create predicates from MongoDB-style filter.
 * Converts filters to functions that test against min/max statistics.
 *
 * @param {object} filter - MongoDB-style filter object
 * @returns {Map<string, (min: any, max: any) => boolean>} map from column to predicate function
 */
export function createPredicates(filter) {
  const predicates = new Map()

  /**
   * @param {any} f
   */
  function processFilter(f) {
    if (f.$and) {
      f.$and.forEach(processFilter)
    } else if (f.$or) {
      // OR predicates across different columns can't use statistics effectively
    } else {
      // Process column-level conditions
      for (const [col, cond] of Object.entries(f)) {
        if (!col.startsWith('$')) {
          const pred = createRangePredicate(cond)
          if (pred) {
            predicates.set(col, pred)
          }
        }
      }
    }
  }

  processFilter(filter)
  return predicates
}

/**
 * Create range predicate from condition.
 * Returns a function that tests if a [min,max] range could contain matching values.
 *
 * @param {any} condition - filter condition (value or operators object)
 * @returns {((min: any, max: any) => boolean)|null} predicate function or null
 */
export function createRangePredicate(condition) {
  // Handle direct value comparison
  if (typeof condition !== 'object' || condition === null) {
    return (min, max) => min <= condition && condition <= max
  }

  const { $eq, $gt, $gte, $lt, $lte, $in } = condition

  // Test if statistics range could contain values matching the condition
  return (min, max) => {
    if ($eq !== undefined) {
      return min <= $eq && $eq <= max
    }

    if ($in && Array.isArray($in)) {
      return $in.some((v) => min <= v && v <= max)
    }

    let possible = true

    if ($gt !== undefined) {
      possible = possible && max > $gt
    }
    if ($gte !== undefined) {
      possible = possible && max >= $gte
    }
    if ($lt !== undefined) {
      possible = possible && min < $lt
    }
    if ($lte !== undefined) {
      possible = possible && min <= $lte
    }

    return possible
  }
}
