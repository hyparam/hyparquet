import { canSkipRowGroup } from './filter.js'
import { parquetSchema } from './metadata.js'
import { getPhysicalColumns } from './schema.js'

// Combine column chunks if less than 2mb
const runLimit = 1 << 21 // 2mb

/**
 * @import {AsyncBuffer, ByteRange, ColumnMetaData, ChunkPlan, GroupPlan, ParquetReadOptions, QueryPlan} from '../src/types.js'
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
      /** @type {ChunkPlan[]} */
      const chunks = []
      // loop through each column chunk
      for (const chunk of rowGroup.columns) {
        const meta = chunk.meta_data
        if (chunk.file_path) throw new Error('parquet file_path not supported')
        if (!meta) throw new Error('parquet column metadata is undefined')
        // add included column chunks to the plan
        if (!columns || columns.includes(meta.path_in_schema[0])) {
          const columnOffset = meta.dictionary_page_offset || meta.data_page_offset
          chunks.push({
            columnMetadata: meta,
            range: {
              startByte: Number(columnOffset),
              endByte: Number(columnOffset + meta.total_compressed_size),
            },
          })
        }
      }
      const selectStart = Math.max(rowStart - groupStart, 0)
      const selectEnd = Math.min(rowEnd - groupStart, groupRows)
      groups.push({ chunks, rowGroup, groupStart, groupRows, selectStart, selectEnd })

      // combine runs of column chunks
      /** @type {ByteRange | undefined} */
      let run
      for (const { range } of chunks) {
        if (columns) {
          fetches.push(range)
        } else if (run && range.endByte - run.startByte <= runLimit) {
          // extend range
          run.endByte = range.endByte
        } else {
          // new range
          if (run) fetches.push(run)
          run = { ...range }
        }
      }
      if (run) fetches.push(run)
    }

    groupStart = groupEnd
  }
  if (!isFinite(rowEnd)) rowEnd = groupStart

  return { metadata, rowStart, rowEnd, columns, fetches, groups }
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
