/**
 * Query implementation with predicate pushdown.
 * Uses Parquet statistics to skip data that doesn't match filters.
 */

import { readColumnIndex, readOffsetIndex } from './indexes.js'
import { parquetMetadataAsync, parquetSchema } from './metadata.js'
import { getSchemaPath } from './schema.js'
import { readColumn } from './column.js'
import { parquetReadObjects } from './index.js'
import { DEFAULT_PARSERS } from './convert.js'
import { concat, equals } from './utils.js'

/**
 * Batch read multiple byte ranges from the file in a single operation.
 * This reduces network round trips when reading from remote storage
 * and allows the underlying implementation to optimize concurrent reads.
 * @param {AsyncBuffer} file - File buffer
 * @param {Array<[number, number] | null>} ranges - Array of byte ranges
 * @returns {Promise<ArrayBuffer[]>} Array of buffers
 */
function sliceAll(file, ranges) {
  return (
    file.sliceAll?.(ranges) ??
    Promise.all(ranges.map((range) => range ? file.slice(range[0], range[1]) : Promise.resolve(new ArrayBuffer(0))))
  )
}

/**
 * Calculate the total byte range of a row group including indices
 * @param {object} rowGroup - Row group metadata
 * @returns {{start: number, end: number, size: number}} - Byte range and size
 */
function getRowGroupFullRange(rowGroup) {
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
 * Query parquet file with predicate pushdown optimization
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
  const requiredColumns = new Set([...outputColumns, ...filterColumns, orderBy].filter(Boolean))

  // Convert filter to predicates that can test min/max statistics
  const predicates = filter ? createPredicates(filter) : new Map()

  // Validate columns exist
  if (filter) {
    const filterColumns = extractFilterColumns(filter)
    const missingColumns = filterColumns.filter((col) => !allColumns.includes(col))
    if (missingColumns.length) {
      throw new Error(`parquet filter columns not found: ${missingColumns.join(', ')}`)
    }
  }
  if (orderBy && !allColumns.includes(orderBy)) {
    throw new Error(`parquet orderBy column not found: ${orderBy}`)
  }

  // Can stream results when filtering without sorting and we know the limit
  if (filter && !orderBy && limit !== undefined && offset + limit < Number(metadata.num_rows)) {
    const filteredRows = []
    let groupStart = 0

    for (let rgIndex = 0; rgIndex < metadata.row_groups.length; rgIndex++) {
      const rowGroup = metadata.row_groups[rgIndex]
      const groupRows = Number(rowGroup.num_rows)

      // Row group statistics let us skip entire groups without reading any data
      if (!canRowGroupMatch(rowGroup, predicates)) {
        groupStart += groupRows
        continue
      }

      // Small row groups are more efficient to read in one shot than multiple reads
      const { start: rgStart, end: rgEnd, size: rgSize } = getRowGroupFullRange(rowGroup)
      const ROW_GROUP_SIZE_THRESHOLD = 4 * 1024 * 1024 // 4MB

      let groupData

      if (rgSize < ROW_GROUP_SIZE_THRESHOLD) {
        // Read entire row group in one shot
        const rgBuffer = await file.slice(rgStart, rgEnd)

        // All subsequent reads will be served from this prefetched buffer
        const bufferedFile = {
          byteLength: file.byteLength,
          slice(start, end) {
            if (start >= rgStart && end <= rgEnd) {
              return Promise.resolve(rgBuffer.slice(start - rgStart, end - rgStart))
            }
            return file.slice(start, end)
          },
          sliceAll(ranges) {
            // All ranges in buffer means we can serve from memory
            const allInBuffer = ranges.every((range) => !range || range[0] >= rgStart && range[1] <= rgEnd)

            if (allInBuffer) {
              return Promise.resolve(
                ranges.map((range) => range ? rgBuffer.slice(range[0] - rgStart, range[1] - rgStart) : new ArrayBuffer(0))
              )
            }

            // Otherwise delegate to original file
            return sliceAll(file, ranges)
          },
        }

        // Now read with the buffered file
        const hasIndices = rowGroup.columns.some((col) => col.column_index_offset)

        if (hasIndices && predicates.size > 0) {
          groupData = await readRowGroupWithPageFilter(bufferedFile, metadata, rgIndex, predicates, [...requiredColumns], options)
        } else {
          groupData = await parquetReadObjects({
            ...options,
            file: bufferedFile,
            metadata,
            columns: [...requiredColumns],
            rowStart: groupStart,
            rowEnd: groupStart + groupRows,
          })
        }
      } else {
        // Large row groups read normally to avoid memory bloat
        const hasIndices = rowGroup.columns.some((col) => col.column_index_offset)

        if (hasIndices && predicates.size > 0) {
          groupData = await readRowGroupWithPageFilter(file, metadata, rgIndex, predicates, [...requiredColumns], options)
        } else {
          groupData = await parquetReadObjects({
            ...options,
            file,
            metadata,
            columns: [...requiredColumns],
            rowStart: groupStart,
            rowEnd: groupStart + groupRows,
          })
        }
      }

      // Apply filter to each row as we read it
      for (const row of groupData) {
        if (matchesFilter(row, filter)) {
          filteredRows.push(row)
          // Early exit if we have enough rows
          if (filteredRows.length >= offset + limit) {
            const sliced = filteredRows.slice(offset, offset + limit)
            return columns ? sliced.map((row) => projectRow(row, columns)) : sliced
          }
        }
      }

      groupStart += groupRows
    }

    // May have fewer rows than limit if we ran out of data
    const sliced = filteredRows.slice(offset, offset + limit)
    return columns ? sliced.map((row) => projectRow(row, columns)) : sliced
  }

  // Can't stream when we need to sort or don't know how many rows we need
  const readOptions = filter || orderBy ? { ...options, rowStart: undefined, rowEnd: undefined } : options
  const rows = await readWithPushdown(file, metadata, predicates, [...requiredColumns], readOptions)

  // Need original positions to maintain stable sort for equal values
  if (orderBy && !filter) {
    rows.forEach((row, idx) => {
      row.__index__ = idx
    })
  }

  const filtered = filter ? rows.filter((row) => matchesFilter(row, filter)) : rows

  // Order matters: filter first (reduce data), then sort, then slice, finally project
  const sorted = orderBy ? sortRows(filtered, orderBy, desc) : filtered

  // Slicing already done during read unless we filtered or sorted
  const sliced = filter || orderBy ? sorted.slice(offset, limit ? offset + limit : undefined) : sorted

  return columns ? sliced.map((row) => projectRow(row, columns)) : sliced
}

/**
 * Read data with predicate pushdown
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {Map<string, Function>} predicates
 * @param {string[]} columns
 * @param {object} options
 * @returns {Promise<object[]>}
 */
async function readWithPushdown(file, metadata, predicates, columns, options) {
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

    // Page indices provide finer-grained statistics than row groups
    const hasIndices = rowGroup.columns.some((col) => col.column_index_offset)

    if (hasIndices && predicates.size > 0) {
      // Use page-level filtering
      const groupData = await readRowGroupWithPageFilter(file, metadata, rgIndex, predicates, columns, options)
      rows.push(...groupData)
    } else {
      // No indices available, read entire row group
      const groupData = await readRowGroup(file, metadata, rgIndex, columns, { ...options, columns }, groupStart)
      rows.push(...groupData)
    }

    groupStart += groupRows
  }

  return rows
}

/**
 * Check if row group can contain matching rows based on statistics
 * @param {object} rowGroup
 * @param {Map<string, Function>} predicates
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
        return false // This row group cannot contain matches
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
 * @param {Map<string, Function>} predicates
 * @param {string[]} columns
 * @param {object} options
 * @returns {Promise<object[]>}
 */
async function readRowGroupWithPageFilter(file, metadata, rgIndex, predicates, columns, options) {
  const rowGroup = metadata.row_groups[rgIndex]
  const columnIndexMap = createColumnIndexMap(rowGroup)

  // Page statistics are more precise than row group statistics
  const pageSelections = await selectPages(file, metadata, rowGroup, predicates, columnIndexMap)

  // No matching pages means no matching rows
  if (!pageSelections || pageSelections.size === 0) return []

  // Read all indices together instead of one by one
  const columnIndices = new Map()
  const indexRanges = []
  const indexColumns = []

  for (const columnName of columns) {
    const colIndex = columnIndexMap.get(columnName)
    if (colIndex === undefined) continue

    const column = rowGroup.columns[colIndex]
    if (!column.meta_data?.path_in_schema?.length || !column.offset_index_offset) continue

    // Add index ranges to batch
    indexRanges.push([Number(column.column_index_offset), Number(column.column_index_offset) + Number(column.column_index_length)])
    indexRanges.push([Number(column.offset_index_offset), Number(column.offset_index_offset) + Number(column.offset_index_length)])
    indexColumns.push(column)
  }

  // Batch read all indices
  if (indexRanges.length > 0) {
    const indexBuffers = await sliceAll(file, indexRanges)

    // Parse indices
    for (let i = 0; i < indexColumns.length; i++) {
      const column = indexColumns[i]
      const colIndexData = indexBuffers[i * 2]
      const offsetIndexData = indexBuffers[i * 2 + 1]

      const schemaPath = getSchemaPath(metadata.schema, column.meta_data.path_in_schema)
      const element = schemaPath[schemaPath.length - 1]?.element

      columnIndices.set(column, {
        columnIndex: readColumnIndex({ view: new DataView(colIndexData), offset: 0 }, element),
        offsetIndex: readOffsetIndex({ view: new DataView(offsetIndexData), offset: 0 }),
      })
    }
  }

  // Now read the actual data pages we selected
  const columnData = await readSelectedPages(file, metadata, rowGroup, columns, pageSelections, columnIndexMap, columnIndices, options)

  // Assemble into rows
  return assembleRows(columnData, columns)
}

/**
 * Select pages that might contain matching rows
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {object} rowGroup
 * @param {Map<string, Function>} predicates
 * @param {Map<string, number>} columnIndexMap
 * @returns {Promise<Set<number>|null>}
 */
async function selectPages(file, metadata, rowGroup, predicates, columnIndexMap) {
  const pageSelections = new Map() // column index -> Set of page indices
  let hasAnyPages = false

  // Each column's pages are checked independently
  for (const [columnName, predicate] of predicates) {
    const colIndex = columnIndexMap.get(columnName)
    if (colIndex === undefined) continue

    const column = rowGroup.columns[colIndex]
    if (!column.column_index_offset) continue

    // Read column and offset indices
    const indices = await readIndices(file, column, metadata.schema)

    // Find pages that might match
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

  // AND semantics: a page must satisfy all predicates to be included
  let selectedPages = null
  for (const pages of pageSelections.values()) {
    if (selectedPages === null) {
      selectedPages = new Set(pages)
    } else {
      // Intersect with previous selections
      selectedPages = new Set([...selectedPages].filter((p) => pages.has(p)))
    }
  }

  return selectedPages
}

/**
 * Read indices for a column
 * @param {AsyncBuffer} file
 * @param {object} column
 * @param {object[]} schema
 * @returns {Promise<{columnIndex: object, offsetIndex: object}>}
 */
async function readIndices(file, column, schema) {
  // Read both indices in one operation instead of two
  const ranges = [
    [Number(column.column_index_offset), Number(column.column_index_offset) + Number(column.column_index_length)],
    [Number(column.offset_index_offset), Number(column.offset_index_offset) + Number(column.offset_index_length)],
  ]

  const [colIndexData, offsetIndexData] = await sliceAll(file, ranges)

  // Parse indices
  const pathInSchema = column.meta_data?.path_in_schema || []
  if (!pathInSchema.length) {
    throw new Error('Column missing path_in_schema')
  }
  const schemaPath = getSchemaPath(schema, pathInSchema)
  const element = schemaPath[schemaPath.length - 1]?.element
  if (!element) {
    throw new Error('Schema element not found for column')
  }

  return {
    columnIndex: readColumnIndex({ view: new DataView(colIndexData), offset: 0 }, element),
    offsetIndex: readOffsetIndex({ view: new DataView(offsetIndexData), offset: 0 }),
  }
}

/**
 * Read selected pages for columns with batched I/O
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {object} rowGroup
 * @param {string[]} columns
 * @param {Set<number>|null} selectedPages
 * @param {Map<string, number>} columnIndexMap
 * @param {Map<object, object>} columnIndices
 * @param {object} options
 * @returns {Promise<Map<string, any[]>>}
 */
async function readSelectedPages(file, metadata, rowGroup, columns, selectedPages, columnIndexMap, columnIndices, options) {
  const columnData = new Map()
  const pageReadPlan = []
  const pageReadColumns = []

  // Collect all ranges first so we can batch the reads
  for (const columnName of columns) {
    const colIndex = columnIndexMap.get(columnName)
    if (colIndex === undefined) continue

    const column = rowGroup.columns[colIndex]
    if (!column.meta_data) continue

    // If we have indices and selected pages, plan page-level reads
    if (selectedPages && columnIndices.has(column)) {
      const indices = columnIndices.get(column)
      const selectedPagesList = Array.from(selectedPages).sort((a, b) => a - b)

      // Dictionary needed for decoding dictionary-encoded pages
      if (column.meta_data.dictionary_page_offset) {
        pageReadPlan.push({
          range: [Number(column.meta_data.dictionary_page_offset), Number(column.meta_data.data_page_offset)],
          column,
          columnName,
          isDictionary: true,
        })
      }

      // Add page reads
      for (const pageIdx of selectedPagesList) {
        const location = indices.offsetIndex.page_locations[pageIdx]
        pageReadPlan.push({
          range: [Number(location.offset), Number(location.offset) + location.compressed_page_size],
          column,
          columnName,
          pageIdx,
          location,
          indices,
        })
      }
      pageReadColumns.push({ column, columnName, selectedPagesList, indices })
    } else {
      // No page filtering, read all column data
      const start = Number(column.meta_data.dictionary_page_offset || column.meta_data.data_page_offset)
      const size = Number(column.meta_data.total_compressed_size)
      pageReadPlan.push({
        range: [start, start + size],
        column,
        columnName,
        isFullColumn: true,
      })
    }
  }

  // Execute all reads in one batch operation
  const allRanges = pageReadPlan.map((p) => p.range)
  const allBuffers = await sliceAll(file, allRanges)

  // Process results
  let bufferIdx = 0
  const columnDictionaries = new Map()
  const columnPageData = new Map()

  for (const plan of pageReadPlan) {
    const buffer = allBuffers[bufferIdx++]

    if (plan.isDictionary) {
      columnDictionaries.set(plan.column, buffer)
    } else if (plan.isFullColumn) {
      // Full column read, no page-level filtering
      const schemaPath = getSchemaPath(metadata.schema, plan.column.meta_data.path_in_schema)
      const reader = { view: new DataView(buffer), offset: 0 }

      const values = readColumn(
        reader,
        {
          groupStart: 0,
          selectStart: 0,
          selectEnd: Number(plan.column.meta_data.num_values),
          groupRows: Number(rowGroup.num_rows),
        },
        {
          columnName: plan.column.meta_data.path_in_schema.join('.'),
          type: plan.column.meta_data.type,
          element: schemaPath[schemaPath.length - 1].element,
          schemaPath,
          codec: plan.column.meta_data.codec,
          parsers: options.parsers || DEFAULT_PARSERS,
          compressors: options.compressors,
          utf8: options.utf8 !== false,
        }
      )

      // readColumn may return multiple chunks that need flattening
      const flatValues = []
      if (Array.isArray(values)) {
        for (const chunk of values) {
          concat(flatValues, chunk)
        }
      }
      columnData.set(plan.columnName, flatValues)
    } else {
      // Collect page data for later assembly
      if (!columnPageData.has(plan.column)) {
        columnPageData.set(plan.column, [])
      }
      columnPageData.get(plan.column).push({
        buffer,
        pageIdx: plan.pageIdx,
        location: plan.location,
      })
    }
  }

  // Combine all pages into complete column data
  for (const { column, columnName, indices } of pageReadColumns) {
    if (!columnPageData.has(column)) continue

    const pages = columnPageData.get(column)
    const dictionary = columnDictionaries.get(column)
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data.path_in_schema)
    const allValues = []

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

    for (const { buffer, pageIdx, location } of pages) {
      // Parquet requires dictionary before data for dictionary encoding
      let fullBuffer
      // let readerOffset = 0

      if (dictionary) {
        // Single allocation is more efficient than multiple concatenations
        const dictSize = dictionary.byteLength
        const pageSize = buffer.byteLength
        fullBuffer = new ArrayBuffer(dictSize + pageSize)
        new Uint8Array(fullBuffer).set(new Uint8Array(dictionary), 0)
        new Uint8Array(fullBuffer).set(new Uint8Array(buffer), dictSize)
        // readerOffset = dictSize
      } else {
        fullBuffer = buffer
      }

      const reader = {
        view: new DataView(fullBuffer),
        offset: 0, // Always start at 0 so readColumn can parse the dictionary header
      }

      // Offset index tells us how many rows are in each page
      const pageFirstRow = Number(location.first_row_index)
      const nextLocation = indices.offsetIndex.page_locations[pageIdx + 1]
      const pageRowCount = nextLocation
        ? Number(nextLocation.first_row_index) - pageFirstRow
        : Number(column.meta_data.num_values) - pageFirstRow

      // Read this specific page's data
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

      // readColumn may return multiple chunks that need flattening

      if (Array.isArray(pageValues)) {
        for (const chunk of pageValues) {
          if (Array.isArray(chunk)) {
            // concat handles typed arrays efficiently
            if (chunk.slice && typeof chunk.slice === 'function' && chunk.constructor !== Array) {
              concat(allValues, chunk)
            } else {
              allValues.push(...chunk)
            }
          } else {
            // Single values just get pushed
            allValues.push(chunk)
          }
        }
      } else {
        // Non-array means single value
        allValues.push(pageValues)
      }
    }

    columnData.set(columnName, allValues)
  }

  return columnData
}

/**
 * Read entire row group without filtering
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {number} rgIndex
 * @param {string[]} columns
 * @param {object} options
 * @param {number} groupStart
 * @returns {Promise<object[]>}
 */
async function readRowGroup(file, metadata, rgIndex, columns, options, groupStart) {
  const rowGroup = metadata.row_groups[rgIndex]
  const groupEnd = groupStart + Number(rowGroup.num_rows)

  return await parquetReadObjects({
    file,
    metadata,
    columns,
    rowStart: groupStart,
    rowEnd: groupEnd,
    ...options,
  })
}

/**
 * Assemble column data into rows
 * @param {Map<string, any[]>} columnData
 * @param {string[]} columns
 * @returns {object[]}
 */
function assembleRows(columnData, columns) {
  if (columnData.size === 0) return []

  const numRows = Math.max(...[...columnData.values()].map((d) => d.length))
  const rows = []

  for (let i = 0; i < numRows; i++) {
    const row = {}
    for (const col of columns) {
      const data = columnData.get(col)
      if (data && Array.isArray(data)) {
        row[col] = data[i] !== undefined ? data[i] : null
      } else {
        row[col] = null
      }
    }
    rows.push(row)
  }

  return rows
}

/**
 * Create column name to index mapping for a row group
 * @param {object} rowGroup
 * @returns {Map<string, number>}
 */
function createColumnIndexMap(rowGroup) {
  const map = new Map()
  rowGroup.columns.forEach((column, index) => {
    if (column.meta_data?.path_in_schema) {
      // Only map top-level column names for now
      if (column.meta_data.path_in_schema.length > 0) {
        map.set(column.meta_data.path_in_schema[0], index)
      }
    }
  })
  return map
}

/**
 * Create predicates from MongoDB-style filter
 * @param {object} filter
 * @returns {Map<string, Function>}
 */
function createPredicates(filter) {
  const predicates = new Map()

  function addPredicate(column, condition) {
    const pred = createRangePredicate(condition)
    if (pred) {
      predicates.set(column, pred)
    }
  }

  function processFilter(f) {
    if (f.$and) {
      f.$and.forEach(processFilter)
    } else if (f.$or) {
      // OR predicates across different columns can't use statistics effectively
    } else {
      // Process column-level conditions
      for (const [col, cond] of Object.entries(f)) {
        if (!col.startsWith('$')) {
          addPredicate(col, cond)
        }
      }
    }
  }

  processFilter(filter)
  return predicates
}

/**
 * Create range predicate from condition
 * @param {any} condition
 * @returns {Function|null}
 */
function createRangePredicate(condition) {
  // Handle direct value comparison
  if (typeof condition !== 'object' || condition === null) {
    return (min, max) => min <= condition && condition <= max
  }

  const { $eq, $gt, $gte, $lt, $lte, $in } = condition

  // Create a function that checks if a [min,max] range could contain values
  // that satisfy the condition. Used for row group and page filtering.
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

/**
 * Check if row matches filter exactly
 * @param {object} row
 * @param {object} filter
 * @returns {boolean}
 */
function matchesFilter(row, filter) {
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
    const matches = matchesCondition(value, cond)
    if (!matches) {
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
function matchesCondition(value, condition) {
  // Handle direct value comparison (including arrays)
  if (typeof condition !== 'object' || condition === null || Array.isArray(condition)) {
    return equals(value, condition)
  }

  // MongoDB semantics: all operators on a field must be satisfied
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
      // $not inverts the entire condition
      if (matchesCondition(value, target)) return false
      break
    }
  }

  return true
}

/**
 * Sort rows by column
 * @param {object[]} rows
 * @param {string} orderBy
 * @param {boolean} desc
 * @returns {object[]}
 */
function sortRows(rows, orderBy, desc) {
  return [...rows].sort((a, b) => {
    const aVal = a[orderBy]
    const bVal = b[orderBy]

    if (aVal === bVal) {
      // Use __index__ to ensure stable sorting
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

/**
 * Extract column names from filter
 * @param {object} filter
 * @returns {string[]}
 */
export function extractFilterColumns(filter) {
  const columns = new Set()

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
 * @import {AsyncBuffer, FileMetaData, CompressionCodec, Compressors, ParquetParsers} from './types.d.ts'
 */
