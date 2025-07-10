import { readColumnIndex, readOffsetIndex } from './indexes.js'
import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { getSchemaPath } from './schema.js'
import { readColumn } from './column.js'
import { parquetReadObjects } from './index.js'
import { DEFAULT_PARSERS } from './convert.js'
import { concat, equals } from './utils.js'
import { createColumnIndexMap, createPredicates, extractFilterColumns, getRowGroupFullRange } from './plan.js'

/**
 * @import {AsyncBuffer, FileMetaData, ColumnChunk, SchemaElement, ColumnIndex, OffsetIndex, CompressionCodec, Compressors, ParquetParsers, RowGroup, DecodedArray} from './types.js'
 */

/**
 * Query parquet file with predicate pushdown.
 * Uses Parquet statistics to skip data that doesn't match filters.
 * @param {object} options - Query options
 * @param {AsyncBuffer} options.file - Parquet file buffer
 * @param {FileMetaData} [options.metadata] - Parquet metadata (will be loaded if not provided)
 * @param {object} [options.filter] - MongoDB-style filter
 * @param {string[]} [options.columns] - Columns to return
 * @param {string} [options.orderBy] - Column to sort by
 * @param {boolean} [options.desc] - Sort descending
 * @param {number} [options.rowStart] - First row to return (inclusive)
 * @param {number} [options.rowEnd] - Last row to return (exclusive)
 * @param {number} [options.offset] - Skip this many rows (alternative to rowStart)
 * @param {number} [options.limit] - Return at most this many rows (alternative to rowEnd)
 * @param {Compressors} [options.compressors] - Custom decompressors
 * @param {boolean} [options.utf8] - Decode byte arrays as utf8 strings
 * @param {ParquetParsers} [options.parsers] - Custom parsers
 * @returns {Promise<object[]>} Array of row objects
 */
export async function parquetQuery(options) {
  const { file, filter, columns, orderBy, desc = false } = options
  const metadata = options.metadata || await parquetMetadataAsync(file)

  // Support both APIs since users might use either style
  const offset = options.offset ?? options.rowStart ?? 0
  if (offset < 0) throw new Error('parquet rowStart must be positive')
  const limit = options.limit ?? (options.rowEnd !== undefined ? options.rowEnd - offset : undefined)

  // Get schema once since we'll reference it multiple times
  const schema = parquetSchema(metadata)
  const allColumns = schema.children.map((c) => c.element.name)

  // Need both output columns and filter columns for evaluation
  const filterColumns = filter ? extractFilterColumns(filter) : []
  const outputColumns = columns || allColumns
  const requiredColumns = [...new Set([...outputColumns, ...filterColumns, orderBy].filter(Boolean))]

  // Convert filter to predicates that can test min/max statistics
  const predicates = filter ? createPredicates(filter) : new Map()

  // Validate columns exist
  if (filter) {
    const missingColumns = filterColumns.filter((col) => !allColumns.includes(col))
    if (missingColumns.length) {
      throw new Error(`parquet filter columns not found: ${missingColumns.join(', ')}`)
    }
  }
  if (orderBy && !allColumns.includes(orderBy)) {
    throw new Error(`parquet orderBy column not found: ${orderBy}`)
  }

  // Main processing loop
  const rows = []
  let groupStart = 0

  for (let rgIndex = 0; rgIndex < metadata.row_groups.length; rgIndex++) {
    const rowGroup = metadata.row_groups[rgIndex]
    const groupRows = Number(rowGroup.num_rows)

    // Skip row groups that can't contain matches based on min/max statistics
    if (!canRowGroupMatch(rowGroup, predicates)) {
      groupStart += groupRows
      continue
    }

    // Apply 4MB optimization
    const { size } = getRowGroupFullRange(rowGroup)
    const useSmallGroupOptimization = size < 4 * 1024 * 1024

    let groupData
    if (useSmallGroupOptimization) {
      groupData = await readSmallRowGroup(file, metadata, rgIndex, predicates, requiredColumns, options, groupStart)
    } else {
      groupData = await readLargeRowGroup(file, metadata, rgIndex, predicates, requiredColumns, options, groupStart)
    }

    // Apply filter if needed
    if (filter) {
      groupData = groupData.filter((row) => matchesFilter(row, filter))
    }

    rows.push(...groupData)

    // Early exit for limited queries without sorting
    if (!orderBy && limit !== undefined && rows.length >= offset + limit) {
      const sliced = rows.slice(offset, offset + limit)
      return columns ? sliced.map((row) => projectRow(row, columns)) : sliced
    }

    groupStart += groupRows
  }

  // Handle sorting
  if (orderBy) {
    if (!filter) {
      // Add stable sort indices
      rows.forEach((row, idx) => {
        row.__index__ = idx
      })
    }
    sortRows(rows, orderBy, desc)
  }

  // Final slice and projection
  const sliced = filter || orderBy ? rows.slice(offset, limit ? offset + limit : undefined) : rows
  return columns ? sliced.map((row) => projectRow(row, columns)) : sliced
}

/**
 * Read small row group with buffering optimization
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {number} rgIndex
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @param {string[]} columns
 * @param {object} options
 * @param {number} groupStart
 * @returns {Promise<object[]>}
 */
export async function readSmallRowGroup(file, metadata, rgIndex, predicates, columns, options, groupStart) {
  const rowGroup = metadata.row_groups[rgIndex]
  const { start, end } = getRowGroupFullRange(rowGroup)

  // Read entire row group at once
  const rgBuffer = await file.slice(start, end)

  // Create buffered file
  const bufferedFile = {
    byteLength: file.byteLength,
    slice(sliceStart, sliceEnd) {
      if (sliceStart >= start && sliceEnd <= end) {
        return Promise.resolve(rgBuffer.slice(sliceStart - start, sliceEnd - start))
      }
      return file.slice(sliceStart, sliceEnd)
    },
    sliceAll(ranges) {
      const allInBuffer = ranges.every((range) => !range || range[0] >= start && range[1] <= end)
      if (allInBuffer) {
        return Promise.resolve(
          ranges.map((range) => range ? rgBuffer.slice(range[0] - start, range[1] - start) : new ArrayBuffer(0))
        )
      }
      return sliceAll(file, ranges)
    },
  }

  // Use page filtering if available
  const hasIndices = rowGroup.columns.some((col) => col.column_index_offset)
  if (hasIndices && predicates.size > 0) {
    return readRowGroupWithPageFilter(bufferedFile, metadata, rgIndex, predicates, columns, options)
  }

  // Otherwise read normally
  return parquetReadObjects({
    ...options,
    file: bufferedFile,
    metadata,
    columns,
    rowStart: groupStart,
    rowEnd: groupStart + Number(rowGroup.num_rows),
  })
}

/**
 * Read large row group without buffering
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {number} rgIndex
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @param {string[]} columns
 * @param {object} options
 * @param {number} groupStart
 * @returns {Promise<object[]>}
 */
export async function readLargeRowGroup(file, metadata, rgIndex, predicates, columns, options, groupStart) {
  const rowGroup = metadata.row_groups[rgIndex]
  const hasIndices = rowGroup.columns.some((col) => col.column_index_offset)

  if (hasIndices && predicates.size > 0) {
    return readRowGroupWithPageFilter(file, metadata, rgIndex, predicates, columns, options)
  }

  return await parquetReadObjects({
    ...options,
    file,
    metadata,
    columns,
    rowStart: groupStart,
    rowEnd: groupStart + Number(rowGroup.num_rows),
  })
}

/**
 * Check if row group can contain matching rows based on statistics
 * @param {RowGroup} rowGroup
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @returns {boolean}
 */
function canRowGroupMatch(rowGroup, predicates) {
  const columnIndexMap = createColumnIndexMap(rowGroup)

  for (const [columnName, predicate] of predicates) {
    const colIndex = columnIndexMap.get(columnName)
    if (colIndex === undefined) continue

    const column = rowGroup.columns[colIndex]
    const stats = column?.meta_data?.statistics

    if (stats?.min_value !== undefined && stats?.max_value !== undefined) {
      if (!predicate(stats.min_value, stats.max_value)) {
        return false
      }
    }
  }

  return true
}

/**
 * Read row group with page-level filtering
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {number} rgIndex
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @param {string[]} columns
 * @param {object} options
 * @returns {Promise<object[]>}
 */
export async function readRowGroupWithPageFilter(file, metadata, rgIndex, predicates, columns, options) {
  const rowGroup = metadata.row_groups[rgIndex]
  const columnIndexMap = createColumnIndexMap(rowGroup)

  // Find pages that might contain matching data
  const selectedPages = await selectPages(file, metadata, rowGroup, predicates, columnIndexMap)
  if (!selectedPages || selectedPages.size === 0) return []

  // Read column data from selected pages
  const columnData = await readSelectedPages(
    file, metadata, rowGroup, columns, selectedPages, columnIndexMap, options
  )

  // Assemble into rows
  return assembleRows(columnData, columns)
}

/**
 * Select pages that might contain matching rows
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {RowGroup} rowGroup
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @param {Map<string, number>} columnIndexMap
 * @returns {Promise<Set<number>|null>}
 */
export async function selectPages(file, metadata, rowGroup, predicates, columnIndexMap) {
  const pageSelections = new Map()
  let hasAnyPages = false

  for (const [columnName, predicate] of predicates) {
    const colIndex = columnIndexMap.get(columnName)
    if (colIndex === undefined) continue

    const column = rowGroup.columns[colIndex]
    if (!column.column_index_offset) continue

    // Read indices
    const indices = await readIndices(file, column, metadata.schema)

    // Find matching pages
    const matchingPages = new Set()
    for (let i = 0; i < indices.columnIndex.min_values.length; i++) {
      if (predicate(indices.columnIndex.min_values[i], indices.columnIndex.max_values[i])) {
        matchingPages.add(i)
      }
    }

    if (matchingPages.size > 0) {
      hasAnyPages = true
      pageSelections.set(colIndex, matchingPages)
    }
  }

  if (!hasAnyPages) return null

  // AND semantics: intersect all column selections
  let selectedPages = null
  for (const pages of pageSelections.values()) {
    if (selectedPages === null) {
      selectedPages = new Set(pages)
    } else {
      selectedPages = new Set([...selectedPages].filter((p) => pages.has(p)))
    }
  }

  return selectedPages
}

/**
 * Read indices for a column
 * @param {AsyncBuffer} file
 * @param {ColumnChunk} column
 * @param {SchemaElement[]} schema
 * @returns {Promise<{columnIndex: ColumnIndex, offsetIndex: OffsetIndex}>}
 */
export async function readIndices(file, column, schema) {
  const ranges = [
    [Number(column.column_index_offset), Number(column.column_index_offset) + Number(column.column_index_length)],
    [Number(column.offset_index_offset), Number(column.offset_index_offset) + Number(column.offset_index_length)],
  ]

  const [colIndexData, offsetIndexData] = await sliceAll(file, ranges)

  const schemaPath = getSchemaPath(schema, column.meta_data.path_in_schema)
  const element = schemaPath[schemaPath.length - 1]?.element

  return {
    columnIndex: readColumnIndex({ view: new DataView(colIndexData), offset: 0 }, element),
    offsetIndex: readOffsetIndex({ view: new DataView(offsetIndexData), offset: 0 }),
  }
}

/**
 * Read selected pages for columns
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {RowGroup} rowGroup
 * @param {string[]} columns
 * @param {Set<number>} selectedPages
 * @param {Map<string, number>} columnIndexMap
 * @param {object} options
 * @returns {Promise<Map<string, any[]>>}
 */
export async function readSelectedPages(file, metadata, rowGroup, columns, selectedPages, columnIndexMap, options) {
  const columnData = new Map()

  for (const columnName of columns) {
    const colIndex = columnIndexMap.get(columnName)
    if (colIndex === undefined) continue

    const column = rowGroup.columns[colIndex]
    if (!column.meta_data) continue

    // Read full column if no page selection
    if (!selectedPages || !column.offset_index_offset) {
      const data = await readFullColumn(file, metadata, rowGroup, column, options)
      columnData.set(columnName, data)
      continue
    }

    // Read indices
    const indices = await readIndices(file, column, metadata.schema)
    const selectedPagesList = Array.from(selectedPages).sort((a, b) => a - b)

    // Collect page ranges
    const pageRanges = []
    let needsDictionary = false

    if (column.meta_data.dictionary_page_offset) {
      needsDictionary = true
      pageRanges.push([
        Number(column.meta_data.dictionary_page_offset),
        Number(column.meta_data.data_page_offset),
      ])
    }

    for (const pageIdx of selectedPagesList) {
      const location = indices.offsetIndex.page_locations[pageIdx]
      pageRanges.push([
        Number(location.offset),
        Number(location.offset) + location.compressed_page_size,
      ])
    }

    // Read all pages
    const pageBuffers = await sliceAll(file, pageRanges)
    const dictionary = needsDictionary ? pageBuffers.shift() : null

    // Decode pages
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data.path_in_schema)
    const columnDecoder = {
      columnName: column.meta_data.path_in_schema.join('.'),
      type: column.meta_data.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      codec: column.meta_data.codec,
      parsers: options.parsers || DEFAULT_PARSERS,
      compressors: options.compressors,
      utf8: options.utf8 !== false,
    }

    const allValues = []
    for (let i = 0; i < selectedPagesList.length; i++) {
      const pageIdx = selectedPagesList[i]
      const pageBuffer = pageBuffers[i]
      const location = indices.offsetIndex.page_locations[pageIdx]

      // Combine dictionary and page if needed
      let fullBuffer = pageBuffer
      if (dictionary) {
        const combined = new ArrayBuffer(dictionary.byteLength + pageBuffer.byteLength)
        new Uint8Array(combined).set(new Uint8Array(dictionary), 0)
        new Uint8Array(combined).set(new Uint8Array(pageBuffer), dictionary.byteLength)
        fullBuffer = combined
      }

      // Calculate page row count
      const pageFirstRow = Number(location.first_row_index)
      const nextLocation = indices.offsetIndex.page_locations[pageIdx + 1]
      const pageRowCount = nextLocation
        ? Number(nextLocation.first_row_index) - pageFirstRow
        : Number(column.meta_data.num_values) - pageFirstRow

      // Read page
      const reader = { view: new DataView(fullBuffer), offset: 0 }
      const pageValues = readColumn(
        reader,
        {
          groupStart: 0,
          selectStart: 0,
          selectEnd: pageRowCount,
          groupRows: pageRowCount,
        },
        columnDecoder
      )

      // Flatten values
      if (Array.isArray(pageValues)) {
        for (const chunk of pageValues) {
          concat(allValues, chunk)
        }
      } else {
        allValues.push(pageValues)
      }
    }

    columnData.set(columnName, allValues)
  }

  return columnData
}

/**
 * Read full column without page filtering
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {RowGroup} rowGroup
 * @param {ColumnChunk} column
 * @param {object} options
 * @returns {Promise<DecodedArray>}
 */
export async function readFullColumn(file, metadata, rowGroup, column, options) {
  const start = Number(column.meta_data?.dictionary_page_offset || column.meta_data?.data_page_offset)
  const size = Number(column.meta_data?.total_compressed_size)
  const buffer = await file.slice(start, start + size)

  const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema || [])
  const reader = { view: new DataView(buffer), offset: 0 }

  const values = readColumn(
    reader,
    {
      groupStart: 0,
      selectStart: 0,
      selectEnd: Number(column.meta_data?.num_values),
      groupRows: Number(rowGroup.num_rows),
    },
    {
      columnName: column.meta_data?.path_in_schema.join('.'),
      type: column.meta_data?.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      codec: column.meta_data?.codec,
      parsers: options.parsers || DEFAULT_PARSERS,
      compressors: options.compressors,
      utf8: options.utf8 !== false,
    }
  )

  /** @type {DecodedArray} Flattened values if needed */
  const flatValues = []
  if (Array.isArray(values)) {
    for (const chunk of values) {
      concat(flatValues, chunk)
    }
  }
  return flatValues
}

/**
 * Batch read multiple byte ranges
 * @param {AsyncBuffer} file
 * @param {Array<[number, number]|null>} ranges
 * @returns {Promise<ArrayBuffer[]>}
 */
export function sliceAll(file, ranges) {
  return (
    file.sliceAll?.(ranges) ??
    Promise.all(ranges.map((range) => range ? file.slice(range[0], range[1]) : Promise.resolve(new ArrayBuffer(0))))
  )
}

/**
 * Assemble column data into rows
 * @param {Map<string, any[]>} columnData
 * @param {string[]} columns
 * @returns {object[]}
 */
export function assembleRows(columnData, columns) {
  if (columnData.size === 0) return []

  const numRows = Math.max(...[...columnData.values()].map((d) => d.length))
  const rows = []

  for (let i = 0; i < numRows; i++) {
    const row = {}
    for (const col of columns) {
      const data = columnData.get(col)
      row[col] = data?.[i] ?? null
    }
    rows.push(row)
  }

  return rows
}

/**
 * Check if row matches filter
 * @param {object} row
 * @param {any} filter
 * @returns {boolean}
 */
export function matchesFilter(row, filter) {
  if (filter.$and) {
    return filter.$and.every((f) => matchesFilter(row, f))
  }

  if (filter.$or) {
    return filter.$or.some((f) => matchesFilter(row, f))
  }

  if (filter.$nor) {
    return !filter.$nor.some((f) => matchesFilter(row, f))
  }

  if (filter.$not) {
    return !matchesFilter(row, filter.$not)
  }

  // Evaluate each column's condition
  for (const [col, cond] of Object.entries(filter)) {
    if (col.startsWith('$')) continue

    const value = row[col]
    if (!matchesCondition(value, cond)) {
      return false
    }
  }

  return true
}

/**
 * Check if value matches condition
 * @param {any} value
 * @param {any} condition
 * @returns {boolean}
 */
export function matchesCondition(value, condition) {
  // Handle direct value comparison
  if (typeof condition !== 'object' || condition === null || Array.isArray(condition)) {
    return equals(value, condition)
  }

  // All operators must be satisfied
  for (const [op, target] of Object.entries(condition)) {
    switch (op) {
    case '$eq':
      if (!equals(value, target)) return false
      break
    case '$ne':
      if (equals(value, target)) return false
      break
    case '$gt':
      if (!(value > target)) return false
      break
    case '$gte':
      if (!(value >= target)) return false
      break
    case '$lt':
      if (!(value < target)) return false
      break
    case '$lte':
      if (!(value <= target)) return false
      break
    case '$in':
      if (!Array.isArray(target) || !target.includes(value)) return false
      break
    case '$nin':
      if (!Array.isArray(target) || target.includes(value)) return false
      break
    case '$not':
      if (matchesCondition(value, target)) return false
      break
    }
  }

  return true
}

/**
 * Sort rows by column
 * @param {any[]} rows
 * @param {string} orderBy
 * @param {boolean} desc
 */
export function sortRows(rows, orderBy, desc) {
  rows.sort((a, b) => {
    const aVal = a[orderBy]
    const bVal = b[orderBy]

    if (aVal === bVal) {
      // Use __index__ for stable sort
      if (a.__index__ !== undefined && b.__index__ !== undefined) {
        return a.__index__ - b.__index__
      }
      return 0
    }
    if (aVal === null || aVal === undefined) return desc ? -1 : 1
    if (bVal === null || bVal === undefined) return desc ? 1 : -1

    const cmp = aVal < bVal ? -1 : 1
    return desc ? -cmp : cmp
  })
}

/**
 * Project row to selected columns
 * @param {object} row
 * @param {string[]} columns
 * @returns {object}
 */
export function projectRow(row, columns) {
  const projected = {}
  for (const col of columns) {
    projected[col] = row[col]
  }
  return projected
}
