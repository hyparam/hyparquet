import { equals } from './utils.js'

/**
 * Match a record against a query filter
 *
 * @param {Record<string, any>} record
 * @param {ParquetQueryFilter} filter
 * @returns {boolean}
 * @example matchQuery({ id: 1 }, { id: {$gte: 1} }) // true
 */
export function matchFilter(record, filter = {}) {
  if ('$and' in filter && Array.isArray(filter.$and)) {
    return filter.$and.every(subQuery => matchFilter(record, subQuery))
  }
  if ('$or' in filter && Array.isArray(filter.$or)) {
    return filter.$or.some(subQuery => matchFilter(record, subQuery))
  }
  if ('$nor' in filter && Array.isArray(filter.$nor)) {
    return !filter.$nor.some(subQuery => matchFilter(record, subQuery))
  }

  return Object.entries(filter).every(([field, condition]) => {
    const value = record[field]

    // implicit $eq for non-object conditions
    if (typeof condition !== 'object' || condition === null || Array.isArray(condition)) {
      return equals(value, condition)
    }

    return Object.entries(condition || {}).every(([operator, target]) => {
      if (operator === '$gt') return value > target
      if (operator === '$gte') return value >= target
      if (operator === '$lt') return value < target
      if (operator === '$lte') return value <= target
      if (operator === '$eq') return equals(value, target)
      if (operator === '$ne') return !equals(value, target)
      if (operator === '$in') return Array.isArray(target) && target.includes(value)
      if (operator === '$nin') return Array.isArray(target) && !target.includes(value)
      if (operator === '$not') return !matchFilter({ [field]: value }, { [field]: target })
      return true
    })
  })
}

/**
 * Check if a row group can be skipped based on filter and column statistics.
 *
 * @import {ParquetQueryFilter, RowGroup} from '../src/types.js'
 * @param {ParquetQueryFilter | undefined} filter
 * @param {RowGroup} group
 * @param {string[]} physicalColumns
 * @returns {boolean} true if the row group can be skipped
 */
export function canSkipRowGroup(filter, group, physicalColumns) {
  if (!filter) return false

  // Handle logical operators
  if ('$and' in filter && Array.isArray(filter.$and)) {
    // For AND, we can skip if ANY condition allows skipping
    return filter.$and.some(subFilter => canSkipRowGroup(subFilter, group, physicalColumns))
  }
  if ('$or' in filter && Array.isArray(filter.$or)) {
    // For OR, we can skip only if ALL conditions allow skipping
    return filter.$or.every(subFilter => canSkipRowGroup(subFilter, group, physicalColumns))
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

    const columnChunk = group.columns[columnIndex]
    const stats = columnChunk.meta_data?.statistics
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
      if (operator === '$ne' && equals(minVal, maxVal) && equals(minVal, target)) return true
      if (operator === '$in' && Array.isArray(target) && target.every(v => v < minVal || v > maxVal)) return true
      if (operator === '$nin' && Array.isArray(target) && equals(minVal, maxVal) && target.includes(minVal)) return true
    }
  }

  return false
}
