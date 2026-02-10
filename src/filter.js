import { equals } from './utils.js'

/**
 * @import {ParquetQueryFilter, RowGroup} from '../src/types.js'
 */

/**
 * Returns an array of top-level column names needed to evaluate the filter.
 *
 * @param {ParquetQueryFilter} [filter]
 * @returns {string[]}
 */
export function columnsNeededForFilter(filter) {
  if (!filter) return []
  /** @type {string[]} */
  const columns = []
  if ('$and' in filter && Array.isArray(filter.$and)) {
    columns.push(...filter.$and.flatMap(columnsNeededForFilter))
  } else if ('$or' in filter && Array.isArray(filter.$or)) {
    columns.push(...filter.$or.flatMap(columnsNeededForFilter))
  } else if ('$nor' in filter && Array.isArray(filter.$nor)) {
    columns.push(...filter.$nor.flatMap(columnsNeededForFilter))
  } else {
    // Map dot-notation paths to top-level column names
    columns.push(...Object.keys(filter).map(key => key.split('.')[0]))
  }
  return [...new Set(columns)]
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
      if (operator === '$in') return Array.isArray(target) && target.includes(value)
      if (operator === '$nin') return Array.isArray(target) && !target.includes(value)
      if (operator === '$not') return !matchFilter({ [field]: value }, { [field]: target }, strict)
      return true
    })
  })
}

/**
 * Check if a row group can be skipped based on filter and column statistics.
 *
 * @param {object} options
 * @param {RowGroup} options.rowGroup
 * @param {string[]} options.physicalColumns
 * @param {ParquetQueryFilter | undefined} options.filter
 * @param {boolean} [options.strict]
 * @returns {boolean} true if the row group can be skipped
 */
export function canSkipRowGroup({ rowGroup, physicalColumns, filter, strict = true }) {
  if (!filter) return false

  // Handle logical operators
  if ('$and' in filter && Array.isArray(filter.$and)) {
    // For AND, we can skip if ANY condition allows skipping
    return filter.$and.some(subFilter => canSkipRowGroup({ rowGroup, physicalColumns, filter: subFilter, strict }))
  }
  if ('$or' in filter && Array.isArray(filter.$or)) {
    // For OR, we can skip only if ALL conditions allow skipping
    return filter.$or.every(subFilter => canSkipRowGroup({ rowGroup, physicalColumns, filter: subFilter, strict }))
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
    if (!stats) continue // No statistics available, can't skip

    const { min, max, min_value, max_value } = stats
    const minVal = min_value !== undefined ? min_value : min
    const maxVal = max_value !== undefined ? max_value : max

    if (minVal === undefined || maxVal === undefined) continue

    // Handle operators
    for (const [operator, target] of Object.entries(condition || {})) {
      if (operator === '$gt' && maxVal <= target) return true
      if (operator === '$gte' && maxVal < target) return true
      if (operator === '$lt' && minVal >= target) return true
      if (operator === '$lte' && minVal > target) return true
      if (operator === '$eq' && (target < minVal || target > maxVal)) return true
      if (operator === '$ne' && equals(minVal, maxVal, strict) && equals(minVal, target, strict)) return true
      if (operator === '$in' && Array.isArray(target) && target.every(v => v < minVal || v > maxVal)) return true
      if (operator === '$nin' && Array.isArray(target) && equals(minVal, maxVal, strict) && target.includes(minVal)) return true
    }
  }

  return false
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
