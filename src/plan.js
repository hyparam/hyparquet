import { bloomEligibleColumns, readBloomFilter } from './bloom.js'
import { canSkipRowGroup } from './filter.js'
import { parquetSchema } from './metadata.js'
import { getPhysicalColumns } from './schema.js'

/**
 * @import {AsyncBuffer, BloomFilter, ByteRange, ChunkPlan, FileMetaData, GroupPlan, ParquetQueryFilter, ParquetReadOptions, QueryPlan, SchemaElement} from '../src/types.js'
 */

// Combine column chunks if less than 2mb
const runLimit = 1 << 21 // 2mb

/**
 * Plan which byte ranges to read to satisfy a read request.
 * Metadata must be non-null.
 *
 * @param {ParquetReadOptions & { bloomFiltersByGroup?: Record<string, BloomFilter>[], schemaElements?: Record<string, SchemaElement> }} options
 * @returns {QueryPlan}
 */
export function parquetPlan({ metadata, rowStart = 0, rowEnd = Infinity, columns, filter, filterStrict = true, useOffsetIndex = false, bloomFiltersByGroup, schemaElements }) {
  if (!metadata) throw new Error('parquetPlan requires metadata')
  /** @type {GroupPlan[]} */
  const groups = []
  /** @type {ByteRange[]} */
  const fetches = []
  /** @type {ByteRange[]} */
  const indexes = []
  const physicalColumns = getPhysicalColumns(parquetSchema(metadata))

  // find which row groups to read
  let groupStart = 0 // first row index of the current group
  let rgIdx = 0
  for (const rowGroup of metadata.row_groups) {
    const groupRows = Number(rowGroup.num_rows)
    const groupEnd = groupStart + groupRows
    const bloomFilters = bloomFiltersByGroup?.[rgIdx]
    // if row group overlaps with row range, add it to the plan
    if (groupRows > 0 && groupEnd > rowStart && groupStart < rowEnd && !canSkipRowGroup({ rowGroup, physicalColumns, filter, strict: filterStrict, bloomFilters, schemaElements })) {
      /** @type {ChunkPlan[]} */
      const chunks = []
      let groupStartByte = Infinity
      let groupEndByte = -Infinity
      // loop through each column chunk
      for (const chunk of rowGroup.columns) {
        const meta = chunk.meta_data
        if (chunk.file_path) throw new Error('parquet file_path not supported')
        if (!meta) throw new Error('parquet column metadata is undefined')
        // add included column chunks to the plan
        if (!columns || columns.includes(meta.path_in_schema[0])) {
          // full column chunk
          const columnOffset = meta.dictionary_page_offset || meta.data_page_offset
          const startByte = Number(columnOffset)
          const endByte = Number(columnOffset + meta.total_compressed_size)
          // update group byte range
          if (startByte < groupStartByte) groupStartByte = startByte
          if (endByte > groupEndByte) groupEndByte = endByte

          if (useOffsetIndex && chunk.offset_index_offset && chunk.offset_index_length && (rowStart > groupStart || rowEnd < groupEnd)) {
            const offsetIndexStart = Number(chunk.offset_index_offset)
            chunks.push({
              columnMetadata: meta,
              offsetIndex: {
                startByte: offsetIndexStart,
                endByte: offsetIndexStart + chunk.offset_index_length,
              },
              range: { startByte, endByte },
            })
          } else {
            chunks.push({
              columnMetadata: meta,
              range: { startByte, endByte },
            })
          }

        }
      }
      const selectStart = Math.max(rowStart - groupStart, 0)
      const selectEnd = Math.min(rowEnd - groupStart, groupRows)
      groups.push({ chunks, rowGroup, groupStart, groupRows, selectStart, selectEnd })

      // combine runs of column chunks
      /** @type {ByteRange | undefined} */
      let run
      for (const chunk of chunks) {
        if ('offsetIndex' in chunk) {
          indexes.push(chunk.offsetIndex)
        } else {
          const { range } = chunk
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
      }
      if (run) fetches.push(run)
    }

    groupStart = groupEnd
    rgIdx++
  }
  if (!isFinite(rowEnd)) rowEnd = groupStart
  fetches.push(...indexes)

  return { metadata, rowStart, rowEnd, columns, fetches, groups }
}

/**
 * Fetch bloom filters for $eq / $in columns of row groups not already provably
 * skippable by statistics alone. Returns an array indexed by row-group ordinal;
 * each entry maps top-level column name → BloomFilter for any chunk whose
 * bloom filter we were able to parse. Adds one round-trip when at least one
 * bloom filter is fetched; otherwise returns synchronously.
 *
 * @param {object} options
 * @param {AsyncBuffer} options.file
 * @param {FileMetaData} options.metadata
 * @param {ParquetQueryFilter} options.filter
 * @param {boolean} [options.filterStrict]
 * @returns {Promise<Record<string, BloomFilter>[]>}
 */
export async function prefetchBloomFilters({ file, metadata, filter, filterStrict = true }) {
  const result = metadata.row_groups.map(() => /** @type {Record<string, BloomFilter>} */ ({}))
  const eligibleCols = bloomEligibleColumns(filter)
  if (eligibleCols.size === 0) return result
  const physicalColumns = getPhysicalColumns(parquetSchema(metadata))

  /** @type {Promise<void>[]} */
  const tasks = []
  metadata.row_groups.forEach((rowGroup, rgIdx) => {
    if (canSkipRowGroup({ rowGroup, physicalColumns, filter, strict: filterStrict })) return
    for (const colName of eligibleCols) {
      const columnIdx = physicalColumns.indexOf(colName)
      if (columnIdx === -1) continue
      const meta = rowGroup.columns[columnIdx]?.meta_data
      if (!meta?.bloom_filter_offset || !meta.bloom_filter_length) continue
      const start = Number(meta.bloom_filter_offset)
      const end = start + meta.bloom_filter_length
      tasks.push((async () => {
        const buffer = await file.slice(start, end)
        const bloom = readBloomFilter({ view: new DataView(buffer), offset: 0 })
        if (bloom) result[rgIdx][colName] = bloom
      })())
    }
  })

  if (tasks.length) await Promise.all(tasks)
  return result
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
      if (index < 0) {
        // fallback to direct read
        return file.slice(start, end)
      }
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
