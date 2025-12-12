import { canSkipRowGroup } from './filter.js'
import { parquetSchema } from './metadata.js'
import { getPhysicalColumns } from './schema.js'
import { concat } from './utils.js'

// Combine column chunks into a single byte range if less than 32mb
const columnChunkAggregation = 1 << 25 // 32mb

/**
 * @import {AsyncBuffer, ByteRange, ColumnMetaData, GroupPlan, ParquetReadOptions, QueryPlan} from '../src/types.js'
 */
/**
 * Plan which byte ranges to read to satisfy a read request.
 * Metadata must be non-null.
 *
 * @param {ParquetReadOptions} options
 * @returns {QueryPlan}
 */
export function parquetPlan({ metadata, rowStart = 0, rowEnd = Infinity, columns, filter, filterStrict = true }) {
  if (!metadata) throw new Error('parquetPlan requires metadata')
  /** @type {GroupPlan[]} */
  const groups = []
  /** @type {ByteRange[]} */
  const fetches = []
  const physicalColumns = getPhysicalColumns(parquetSchema(metadata))

  // find which row groups to read
  let groupStart = 0 // first row index of the current group
  for (const rowGroup of metadata.row_groups) {
    const groupRows = Number(rowGroup.num_rows)
    const groupEnd = groupStart + groupRows
    // if row group overlaps with row range, add it to the plan
    if (groupRows > 0 && groupEnd > rowStart && groupStart < rowEnd && !canSkipRowGroup({ rowGroup, physicalColumns, filter, strict: filterStrict })) {
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
