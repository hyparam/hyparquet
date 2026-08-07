/**
 * @import {BloomFilter, ColumnPageStats, PageRanges, ParquetQueryFilter, RowGroup, SchemaElement} from '../src/types.js'
 */

import { hashParquetValue, sbbfContains } from './bloom.js'
import { equals } from './utils.js'

const encoder = new TextEncoder()

/**
 * Returns the physical leaf paths referenced by a filter.
 *
 * @param {ParquetQueryFilter} [filter]
 * @returns {string[]}
 */
export function pathsNeededForFilter(filter) {
  if (!filter) return []
  /** @type {string[]} */
  const paths = []
  if ('$and' in filter && Array.isArray(filter.$and)) {
    paths.push(...filter.$and.flatMap(pathsNeededForFilter))
  } else if ('$or' in filter && Array.isArray(filter.$or)) {
    paths.push(...filter.$or.flatMap(pathsNeededForFilter))
  } else if ('$nor' in filter && Array.isArray(filter.$nor)) {
    paths.push(...filter.$nor.flatMap(pathsNeededForFilter))
  } else {
    paths.push(...Object.keys(filter))
  }
  return [...new Set(paths)]
}

/**
 * Returns an array of top-level column names needed to evaluate the filter.
 *
 * @param {ParquetQueryFilter} [filter]
 * @returns {string[]}
 */
export function columnsNeededForFilter(filter) {
  return [...new Set(pathsNeededForFilter(filter).map(path => path.split('.')[0]))]
}

/**
 * Match a record against a query filter
 *
 * @param {Record<string, any>} record
 * @param {ParquetQueryFilter} filter
 * @param {boolean} [strict]
 * @returns {boolean}
 */
export function matchFilter(record, filter, strict = true) {
  if ('$and' in filter && Array.isArray(filter.$and)) {
    return filter.$and.every(subQuery => matchFilter(record, subQuery, strict))
  }
  if ('$or' in filter && Array.isArray(filter.$or)) {
    return filter.$or.some(subQuery => matchFilter(record, subQuery, strict))
  }
  if ('$nor' in filter && Array.isArray(filter.$nor)) {
    return !filter.$nor.some(subQuery => matchFilter(record, subQuery, strict))
  }

  return Object.entries(filter).every(([field, condition]) => {
    const value = resolve(record, field)

    // implicit $eq for non-object conditions
    if (typeof condition !== 'object' || condition === null || Array.isArray(condition)) {
      return equals(value, condition, strict)
    }

    return Object.entries(condition || {}).every(([operator, target]) => {
      if (operator === '$gt') return value > target
      if (operator === '$gte') return value >= target
      if (operator === '$lt') return value < target
      if (operator === '$lte') return value <= target
      if (operator === '$eq') return equals(value, target, strict)
      if (operator === '$ne') return !equals(value, target, strict)
      if (operator === '$in') return Array.isArray(target) && matchesIn(value, target, strict)
      if (operator === '$nin') return Array.isArray(target) && !matchesIn(value, target, strict)
      if (operator === '$not') return !matchFilter({ [field]: value }, { [field]: target }, strict)
      return true
    })
  })
}

/**
 * Match a value or one of its immediate array elements against a list.
 *
 * @param {any} value
 * @param {any[]} targets
 * @param {boolean} strict
 * @returns {boolean}
 */
function matchesIn(value, targets, strict) {
  return targets.some(target =>
    equals(value, target, strict) ||
    Array.isArray(value) && value.some(element => equals(element, target, strict)))
}

/**
 * Check if a row group can be skipped based on filter and column statistics,
 * optionally consulting per-column bloom filters for equality predicates that
 * statistics can't decide.
 *
 * @param {object} options
 * @param {RowGroup} options.rowGroup
 * @param {string[]} options.physicalColumns
 * @param {ParquetQueryFilter | undefined} options.filter
 * @param {boolean} [options.strict]
 * @param {Record<string, BloomFilter>} [options.bloomFilters] keyed by filter path
 * @param {Record<string, SchemaElement>} [options.schemaElements] keyed by physical leaf path
 * @returns {boolean} true if the row group can be skipped
 */
export function canSkipRowGroup({ rowGroup, physicalColumns, filter, strict = true, bloomFilters, schemaElements }) {
  if (!filter) return false

  // Handle logical operators
  if ('$and' in filter && Array.isArray(filter.$and)) {
    // For AND, we can skip if ANY condition allows skipping
    return filter.$and.some(subFilter => canSkipRowGroup({ rowGroup, physicalColumns, filter: subFilter, strict, bloomFilters, schemaElements }))
  }
  if ('$or' in filter && Array.isArray(filter.$or)) {
    // For OR, we can skip only if ALL conditions allow skipping
    return filter.$or.every(subFilter => canSkipRowGroup({ rowGroup, physicalColumns, filter: subFilter, strict, bloomFilters, schemaElements }))
  }
  if ('$nor' in filter && Array.isArray(filter.$nor)) {
    // For NOR, we can skip if none of the conditions allow skipping
    // This is complex, so we'll be conservative and not skip
    return false
  }

  // Check column filters
  for (const [field, condition] of Object.entries(filter)) {
    // Find the column chunk for this field
    const columnIndex = physicalColumns.indexOf(field)
    if (columnIndex === -1) continue

    const stats = rowGroup.columns[columnIndex].meta_data?.statistics
    const { min, max, min_value, max_value, null_count: nullCount } = stats || {}
    const minVal = min_value !== undefined ? min_value : min
    const maxVal = max_value !== undefined ? max_value : max
    const haveStats = minVal !== undefined && maxVal !== undefined

    const bloom = bloomFilters?.[field]
    const element = schemaElements?.[field]
    const matchingNulls = matchFilter({ value: null }, { value: condition }, strict) &&
      (nullCount === undefined || nullCount > 0)

    if (haveStats && !matchingNulls && canSkipStats(condition, minVal, maxVal, strict, element)) {
      return true
    }

    // Handle operators
    for (const [operator, target] of Object.entries(condition || {})) {
      // Bloom-filter skipping for equality predicates (proves absence, never presence)
      if (bloom && element) {
        if (operator === '$eq') {
          const hash = hashParquetValue(target, element)
          if (hash !== undefined && !sbbfContains(bloom.blocks, hash)) return true
        }
        if (operator === '$in' && Array.isArray(target) && target.length > 0) {
          let allAbsent = true
          for (const v of target) {
            const h = hashParquetValue(v, element)
            if (h === undefined || sbbfContains(bloom.blocks, h)) {
              allAbsent = false
              break
            }
          }
          if (allAbsent) return true
        }
      }
    }
  }

  return false
}

/**
 * Check if a page value range [minVal, maxVal] provably contains no value
 * matching the operator conditions.
 *
 * @param {any} condition operator object like { $gt: 5 }
 * @param {any} minVal lower bound (may be truncated, still a valid bound)
 * @param {any} maxVal upper bound (may be truncated, still a valid bound)
 * @param {boolean} strict
 * @param {SchemaElement} [element] physical schema element for the bounds
 * @returns {boolean} true if no value in the range can match
 */
function canSkipStats(condition, minVal, maxVal, strict, element) {
  if (minVal === undefined || maxVal === undefined) return false
  const mayContainNaN = element?.type === 'FLOAT' || element?.type === 'DOUBLE' || element?.logical_type?.type === 'FLOAT16'
  for (const [operator, target] of Object.entries(condition || {})) {
    const minComparison = compareParquetValues(minVal, target, strict, element)
    const maxComparison = compareParquetValues(maxVal, target, strict, element)
    // Row filtering uses JavaScript relational operators. For non-ASCII
    // strings their UTF-16 order can differ from Parquet's UTF-8 index order,
    // and binary values are string-coerced rather than byte-compared, so those
    // min/max bounds cannot safely prove a relational predicate impossible.
    const binaryBounds = minVal instanceof Uint8Array || maxVal instanceof Uint8Array
    const relationalBoundsAreSafe = !binaryBounds && (element?.type !== 'BYTE_ARRAY' ||
      typeof target === 'string' && [...target].every(character => character.charCodeAt(0) <= 0x7f))
    if (operator === '$gt' && relationalBoundsAreSafe && maxComparison !== undefined && maxComparison <= 0) return true
    if (operator === '$gte' && relationalBoundsAreSafe && maxComparison !== undefined && maxComparison < 0) return true
    if (operator === '$lt' && relationalBoundsAreSafe && minComparison !== undefined && minComparison >= 0) return true
    if (operator === '$lte' && relationalBoundsAreSafe && minComparison !== undefined && minComparison > 0) return true
    if (operator === '$eq') {
      const targetMinComparison = compareParquetValues(target, minVal, strict, element)
      const targetMaxComparison = compareParquetValues(target, maxVal, strict, element)
      if (targetMinComparison !== undefined && targetMinComparison < 0 ||
          targetMaxComparison !== undefined && targetMaxComparison > 0) return true
    }
    if (operator === '$ne' && !mayContainNaN && equals(minVal, maxVal, strict) && equals(minVal, target, strict)) return true
    if (operator === '$in' && Array.isArray(target) && target.every(value => {
      const valueMinComparison = compareParquetValues(value, minVal, strict, element)
      const valueMaxComparison = compareParquetValues(value, maxVal, strict, element)
      return valueMinComparison !== undefined && valueMinComparison < 0 ||
        valueMaxComparison !== undefined && valueMaxComparison > 0
    })) return true
    if (operator === '$nin' && !mayContainNaN && Array.isArray(target) && equals(minVal, maxVal, strict) && target.includes(minVal)) return true
  }
  return false
}

/**
 * Compare values using Parquet's unsigned lexicographic ordering for binary
 * values. Returns undefined when values do not have a safely comparable order.
 *
 * @param {any} a
 * @param {any} b
 * @param {boolean} strict
 * @param {SchemaElement} [element]
 * @returns {-1 | 0 | 1 | undefined}
 */
function compareParquetValues(a, b, strict, element) {
  // BYTE_ARRAY values are decoded as UTF-8 strings by the default parser even
  // without an explicit UTF8/STRING annotation.
  if (element?.type === 'BYTE_ARRAY') {
    if (typeof a !== 'string' || typeof b !== 'string') return undefined
    return compareBytes(encoder.encode(a), encoder.encode(b))
  }
  if (a instanceof Uint8Array || b instanceof Uint8Array) {
    if (!(a instanceof Uint8Array) || !(b instanceof Uint8Array)) return undefined
    return compareBytes(a, b)
  }
  if (a < b) return -1
  if (a > b) return 1
  if (equals(a, b, strict)) return 0
  return undefined
}

/**
 * @param {Uint8Array} a
 * @param {Uint8Array} b
 * @returns {-1 | 0 | 1}
 */
function compareBytes(a, b) {
  const length = Math.min(a.length, b.length)
  for (let i = 0; i < length; i++) {
    if (a[i] < b[i]) return -1
    if (a[i] > b[i]) return 1
  }
  if (a.length < b.length) return -1
  if (a.length > b.length) return 1
  return 0
}

/**
 * Check whether the filter condition accepts a null value.
 *
 * @param {any} condition
 * @param {boolean} strict
 * @returns {boolean}
 */
function matchesNull(condition, strict) {
  return matchFilter({ value: null }, { value: condition }, strict)
}

/**
 * Compute candidate row ranges within a row group that could match the filter,
 * based on per-page column index statistics.
 *
 * Returns sorted disjoint [start, end) row ranges relative to the group.
 * An empty array means the filter provably matches no rows in the group.
 * Returns undefined when the page statistics give no pruning information
 * (the caller must read the whole selection).
 *
 * @param {ParquetQueryFilter | undefined} filter
 * @param {Record<string, ColumnPageStats>} columnPages keyed by physical leaf path
 * @param {number} groupRows number of rows in the row group
 * @param {boolean} [strict]
 * @returns {PageRanges | undefined}
 */
export function filterPageRanges(filter, columnPages, groupRows, strict = true) {
  if (!filter) return undefined
  if ('$and' in filter && Array.isArray(filter.$and)) {
    /** @type {PageRanges | undefined} */
    let ranges
    for (const subFilter of filter.$and) {
      ranges = intersectRanges(ranges, filterPageRanges(subFilter, columnPages, groupRows, strict))
    }
    return ranges
  }
  if ('$or' in filter && Array.isArray(filter.$or)) {
    /** @type {PageRanges} */
    let ranges = []
    for (const subFilter of filter.$or) {
      const subRanges = filterPageRanges(subFilter, columnPages, groupRows, strict)
      // any un-prunable branch could match anywhere
      if (!subRanges) return undefined
      ranges = unionRanges(ranges, subRanges)
    }
    return ranges
  }
  if ('$nor' in filter && Array.isArray(filter.$nor)) {
    // Conservative: negations cannot be pruned by min/max bounds
    return undefined
  }

  // Column conditions: all fields must match, so intersect their ranges
  /** @type {PageRanges | undefined} */
  let result
  for (const [field, condition] of Object.entries(filter)) {
    const pages = columnPages[field]
    if (!pages) continue
    const nullCanMatch = matchesNull(condition, strict)
    /** @type {PageRanges} */
    const keep = []
    for (let i = 0; i < pages.pageStarts.length; i++) {
      const start = pages.pageStarts[i]
      const end = i + 1 < pages.pageStarts.length ? pages.pageStarts[i + 1] : groupRows
      const nullCount = pages.nullCounts?.[i]
      // Keep pages with matching nulls, including pages whose null presence is
      // unknown because the optional null_counts index field was omitted.
      const matchingNulls = nullCanMatch && (nullCount === undefined || nullCount > 0)
      const skip = !pages.nullPages[i] && !matchingNulls &&
        canSkipStats(condition, pages.minValues[i], pages.maxValues[i], strict, pages.element)
      if (!skip) {
        const last = keep[keep.length - 1]
        if (last && last[1] === start) last[1] = end
        else keep.push([start, end])
      }
    }
    result = intersectRanges(result, keep)
  }
  return result
}

/**
 * Intersect two sets of sorted disjoint ranges. Undefined means "everything".
 *
 * @param {PageRanges | undefined} a
 * @param {PageRanges | undefined} b
 * @returns {PageRanges | undefined}
 */
export function intersectRanges(a, b) {
  if (!a) return b
  if (!b) return a
  /** @type {PageRanges} */
  const out = []
  let i = 0
  let j = 0
  while (i < a.length && j < b.length) {
    const start = Math.max(a[i][0], b[j][0])
    const end = Math.min(a[i][1], b[j][1])
    if (start < end) out.push([start, end])
    if (a[i][1] < b[j][1]) i++
    else j++
  }
  return out
}

/**
 * Union two sets of sorted disjoint ranges.
 *
 * @param {PageRanges} a
 * @param {PageRanges} b
 * @returns {PageRanges}
 */
export function unionRanges(a, b) {
  /** @type {PageRanges} */
  const out = []
  let i = 0
  let j = 0
  while (i < a.length || j < b.length) {
    const next = j >= b.length || i < a.length && a[i][0] <= b[j][0] ? a[i++] : b[j++]
    const last = out[out.length - 1]
    if (last && next[0] <= last[1]) last[1] = Math.max(last[1], next[1])
    else out.push([next[0], next[1]])
  }
  return out
}

/**
 * Resolve a dot-notation path to a value in a nested object.
 *
 * @param {Record<string, any>} record
 * @param {string} path
 * @returns {any}
 */
function resolve(record, path) {
  let value = record
  for (const part of path.split('.')) {
    value = value?.[part]
  }
  return value
}
