import { bloomEligibleColumns, readBloomFilter } from './bloom.js'
import { canSkipRowGroup, filterPageRanges, pathsNeededForFilter } from './filter.js'
import { readColumnIndex, readOffsetIndex } from './indexes.js'
import { parquetSchema } from './metadata.js'
import { getPhysicalColumns } from './schema.js'

/**
 * @import {AsyncBuffer, BloomFilter, ByteRange, ChunkPlan, ColumnPageStats, FileMetaData, GroupPlan, PageLocation, PageRanges, ParquetParsers, ParquetQueryFilter, ParquetReadOptions, QueryPlan, RowGroup, SchemaElement, SchemaTree} from '../src/types.js'
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
  const schemaTree = parquetSchema(metadata)
  const physicalColumns = getPhysicalColumns(schemaTree)
  const elementsByPath = filter ? {
    ...physicalSchemaElements(schemaTree),
    ...schemaElements,
  } : schemaElements

  // find which row groups to read
  let groupStart = 0 // first row index of the current group
  let rgIdx = 0
  for (const rowGroup of metadata.row_groups) {
    const groupRows = Number(rowGroup.num_rows)
    const groupEnd = groupStart + groupRows
    const bloomFilters = bloomFiltersByGroup?.[rgIdx]
    // if row group overlaps with row range, add it to the plan
    if (groupRows > 0 && groupEnd > rowStart && groupStart < rowEnd && !canSkipRowGroup({ rowGroup, physicalColumns, filter, strict: filterStrict, bloomFilters, schemaElements: elementsByPath })) {
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
 * Fetch page indexes (column index + offset index) for filter columns of row
 * groups that survive row-group-level pruning, and compute candidate row
 * ranges per group from the per-page min/max statistics.
 *
 * Returns pageRangesByGroup indexed by row-group ordinal: sorted disjoint
 * [start, end) row ranges (relative to the group) that could match the filter.
 * An empty array means the group provably contains no matching rows; undefined
 * means no page-level information was available for that group.
 * Also returns pageLocationsByGroup, mapping row-group ordinals and physical
 * leaf paths to page locations, so the read path can reuse the parsed offset
 * indexes without refetching them.
 *
 * @param {object} options
 * @param {AsyncBuffer} options.file
 * @param {FileMetaData} options.metadata
 * @param {ParquetQueryFilter} [options.filter]
 * @param {boolean} [options.filterStrict]
 * @param {number} [options.rowStart]
 * @param {number} [options.rowEnd]
 * @param {string[]} [options.columns]
 * @param {Record<string, BloomFilter>[]} [options.bloomFiltersByGroup]
 * @param {Record<string, SchemaElement>} [options.schemaElements]
 * @param {ParquetParsers} [options.parsers]
 * @returns {Promise<{pageRangesByGroup: (PageRanges | undefined)[], pageLocationsByGroup: Record<string, PageLocation[]>[]}>}
 */
export async function prefetchPageIndexes({ file, metadata, filter, filterStrict = true, rowStart = 0, rowEnd = Infinity, columns, bloomFiltersByGroup, schemaElements, parsers }) {
  /** @type {(PageRanges | undefined)[]} */
  const pageRangesByGroup = metadata.row_groups.map(() => undefined)
  const pageLocationsByGroup = metadata.row_groups.map(() => /** @type {Record<string, PageLocation[]>} */ ({}))
  if (filter && '$nor' in filter && Array.isArray(filter.$nor)) {
    return { pageRangesByGroup, pageLocationsByGroup }
  }
  const filterColumns = pathsNeededForFilter(filter)
  if (!filterColumns.length) return { pageRangesByGroup, pageLocationsByGroup }
  const schemaTree = parquetSchema(metadata)
  const physicalColumns = getPhysicalColumns(schemaTree)
  const elementsByPath = {
    ...physicalSchemaElements(schemaTree),
    ...schemaElements,
  }

  /** @type {Promise<void>[]} */
  const tasks = []
  let groupStart = 0
  metadata.row_groups.forEach((rowGroup, rgIdx) => {
    const groupRows = Number(rowGroup.num_rows)
    const groupEnd = groupStart + groupRows
    const overlaps = groupRows > 0 && groupEnd > rowStart && groupStart < rowEnd
    groupStart = groupEnd
    if (!overlaps) return
    if (canSkipRowGroup({ rowGroup, physicalColumns, filter, strict: filterStrict, bloomFilters: bloomFiltersByGroup?.[rgIdx], schemaElements: elementsByPath })) return

    /** @type {Record<string, ColumnPageStats>} */
    const columnPages = {}
    /** @type {Promise<void>[]} */
    const columnTasks = []
    const scheduledOffsetPaths = new Set()
    let hasFilterIndex = false
    for (const columnName of filterColumns) {
      const columnIdx = physicalColumns.indexOf(columnName)
      if (columnIdx === -1) continue
      const chunk = rowGroup.columns[columnIdx]
      const meta = chunk?.meta_data
      if (!meta) continue
      if (!chunk.column_index_offset || !chunk.column_index_length) continue
      if (!chunk.offset_index_offset || !chunk.offset_index_length) continue
      const element = elementsByPath[columnName]
      if (!element) continue
      hasFilterIndex = true
      scheduledOffsetPaths.add(columnName)
      const columnIndexStart = Number(chunk.column_index_offset)
      const offsetIndexStart = Number(chunk.offset_index_offset)
      columnTasks.push(Promise.all([
        file.slice(columnIndexStart, columnIndexStart + chunk.column_index_length),
        file.slice(offsetIndexStart, offsetIndexStart + chunk.offset_index_length),
      ]).then(([columnIndexBuffer, offsetIndexBuffer]) => {
        const columnIndex = readColumnIndex({ view: new DataView(columnIndexBuffer), offset: 0 }, element, parsers)
        const offsetIndex = readOffsetIndex({ view: new DataView(offsetIndexBuffer), offset: 0 })
        pageLocationsByGroup[rgIdx][columnName] = offsetIndex.page_locations
        columnPages[columnName] = {
          minValues: columnIndex.min_values,
          maxValues: columnIndex.max_values,
          nullPages: columnIndex.null_pages,
          nullCounts: columnIndex.null_counts,
          pageStarts: offsetIndex.page_locations.map(page => Number(page.first_row_index)),
          element,
        }
      }))
    }
    // Candidate ranges are shared by every selected column. Load their offset
    // indexes too so the planner can merge ranges that map to the same coarse
    // page instead of fetching and decoding that page more than once.
    if (hasFilterIndex) {
      for (const chunk of rowGroup.columns) {
        const meta = chunk.meta_data
        if (!meta) continue
        const columnName = meta.path_in_schema[0]
        const columnPath = meta.path_in_schema.join('.')
        if (columns && !columns.includes(columnName)) continue
        if (scheduledOffsetPaths.has(columnPath)) continue
        if (!chunk.offset_index_offset || !chunk.offset_index_length) continue
        scheduledOffsetPaths.add(columnPath)
        const offsetIndexStart = Number(chunk.offset_index_offset)
        columnTasks.push(Promise.resolve(
          file.slice(offsetIndexStart, offsetIndexStart + chunk.offset_index_length)
        ).then(offsetIndexBuffer => {
          const offsetIndex = readOffsetIndex({ view: new DataView(offsetIndexBuffer), offset: 0 })
          pageLocationsByGroup[rgIdx][columnPath] = offsetIndex.page_locations
        }))
      }
    }
    if (columnTasks.length) {
      tasks.push(Promise.all(columnTasks).then(() => {
        pageRangesByGroup[rgIdx] = filterPageRanges(filter, columnPages, groupRows, filterStrict)
      }))
    }
  })

  if (tasks.length) await Promise.all(tasks)
  return { pageRangesByGroup, pageLocationsByGroup }
}

/**
 * Build a lookup from physical paths to leaf schema elements.
 *
 * @param {SchemaTree} schemaTree
 * @returns {Record<string, SchemaElement>}
 */
function physicalSchemaElements(schemaTree) {
  /** @type {Record<string, SchemaElement>} */
  const elements = {}
  /** @param {SchemaTree} node */
  function traverse(node) {
    if (node.children.length) {
      for (const child of node.children) traverse(child)
    } else {
      elements[node.path.join('.')] = node.element
    }
  }
  traverse(schemaTree)
  return elements
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
