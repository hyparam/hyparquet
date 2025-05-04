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
export function parquetPlan({ metadata, rowStart = 0, rowEnd = Infinity, columns }) {
  if (!metadata) throw new Error('parquetPlan requires metadata')
  /** @type {GroupPlan[]} */
  const groups = []
  /** @type {ByteRange[]} */
  const ranges = []

  // find which row groups to read
  let groupStart = 0 // first row index of the current group
  for (const rowGroup of metadata.row_groups) {
    const groupEnd = groupStart + Number(rowGroup.num_rows)
    // if row group overlaps with row range, add it to the plan
    if (groupEnd >= rowStart && groupStart < rowEnd) {
      /** @type {ByteRange[]} */
      const plan = []
      // loop through each column chunk
      for (const { file_path, meta_data } of rowGroup.columns) {
        if (file_path) throw new Error('parquet file_path not supported')
        if (!meta_data) throw new Error('parquet column metadata is undefined')
        // add included columns to the plan
        if (!columns || columns.includes(meta_data.path_in_schema[0])) {
          plan.push(getColumnRange(meta_data))
        }
      }
      groups.push({ plan })

      // map group plan to ranges
      const groupSize = plan[plan.length - 1]?.endByte - plan[0]?.startByte
      if (!columns && groupSize < columnChunkAggregation) {
        // full row group
        ranges.push({
          startByte: plan[0].startByte,
          endByte: plan[plan.length - 1].endByte,
        })
      } else if (plan.length) {
        concat(ranges, plan)
      } else if (columns?.length) {
        throw new Error(`parquet columns not found: ${columns.join(', ')}`)
      }
    }

    groupStart = groupEnd
  }

  return { ranges, groups }
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
export function prefetchAsyncBuffer(file, { ranges }) {
  // fetch byte ranges from the file
  const promises = ranges.map(({ startByte, endByte }) => file.slice(startByte, endByte))
  return {
    byteLength: file.byteLength,
    slice(start, end = file.byteLength) {
      // find matching slice
      const index = ranges.findIndex(({ startByte, endByte }) => startByte <= start && end <= endByte)
      if (index < 0) throw new Error(`no prefetch for range [${start}, ${end}]`)
      if (ranges[index].startByte !== start || ranges[index].endByte !== end) {
        // slice a subrange of the prefetch
        const startOffset = start - ranges[index].startByte
        const endOffset = end - ranges[index].startByte
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
