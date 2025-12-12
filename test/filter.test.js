import { describe, expect, it } from 'vitest'
import { canSkipRowGroup, matchFilter } from '../src/filter.js'

/**
 * @import { RowGroup } from '../src/types.js'
 */

describe('matchFilter', () => {
  it('handles logical operators $and, $or, $nor', () => {
    const record = { a: 5, b: 10 }
    expect(matchFilter(record, { $and: [{ a: { $eq: 5 } }, { b: { $eq: 10 } }] })).toBe(true)
    expect(matchFilter(record, { $and: [{ a: { $eq: 5 } }, { b: { $eq: 99 } }] })).toBe(false)
    expect(matchFilter(record, { $or: [{ a: { $eq: 1 } }, { b: { $eq: 10 } }] })).toBe(true)
    expect(matchFilter(record, { $or: [{ a: { $eq: 1 } }, { b: { $eq: 99 } }] })).toBe(false)
    expect(matchFilter(record, { $nor: [{ a: { $eq: 1 } }, { b: { $eq: 99 } }] })).toBe(true)
    expect(matchFilter(record, { $nor: [{ a: { $eq: 5 } }, { b: { $eq: 99 } }] })).toBe(false)
  })

  it('handles comparison operators', () => {
    const record = { x: 10 }
    expect(matchFilter(record, { x: { $gt: 9 } })).toBe(true)
    expect(matchFilter(record, { x: { $gt: 10 } })).toBe(false)
    expect(matchFilter(record, { x: { $gte: 10 } })).toBe(true)
    expect(matchFilter(record, { x: { $lt: 11 } })).toBe(true)
    expect(matchFilter(record, { x: { $lt: 10 } })).toBe(false)
    expect(matchFilter(record, { x: { $lte: 10 } })).toBe(true)
    expect(matchFilter(record, { x: { $eq: 10 } })).toBe(true)
    expect(matchFilter(record, { x: { $ne: 10 } })).toBe(false)
    expect(matchFilter(record, { x: { $ne: 5 } })).toBe(true)
  })

  it('handles $in, $nin, and $not operators', () => {
    const record = { x: 5 }
    expect(matchFilter(record, { x: { $in: [1, 5, 10] } })).toBe(true)
    expect(matchFilter(record, { x: { $in: [1, 2, 3] } })).toBe(false)
    expect(matchFilter(record, { x: { $nin: [1, 2, 3] } })).toBe(true)
    expect(matchFilter(record, { x: { $nin: [5, 6, 7] } })).toBe(false)
    expect(matchFilter(record, { x: { $not: { $gt: 10 } } })).toBe(true)
    expect(matchFilter(record, { x: { $not: { $lt: 10 } } })).toBe(false)
  })

  it('uses strict equality (===) when strict is true', () => {
    expect(matchFilter({ x: 5 }, { x: { $eq: '5' } }, true)).toBe(false)
    expect(matchFilter({ x: 5 }, { x: { $ne: '5' } }, true)).toBe(true)
  })

  it('uses loose equality (==) when strict is false', () => {
    expect(matchFilter({ x: 5 }, { x: { $eq: '5' } }, false)).toBe(true)
    expect(matchFilter({ x: 5 }, { x: { $ne: '5' } }, false)).toBe(false)
  })
})

describe('canSkipRowGroup', () => {
  /**
   * @param {number} min
   * @param {number} max
   * @returns {RowGroup}
   */
  function makeRowGroup(min, max) {
    return {
      columns: [{
        meta_data: {
          type: 'INT32',
          path_in_schema: ['a'],
          codec: 'UNCOMPRESSED',
          num_values: 1000n,
          total_compressed_size: 2048n,
          total_uncompressed_size: 4096n,
          encodings: ['PLAIN'],
          data_page_offset: 4n,
          statistics: { min_value: min, max_value: max },
        },
        file_offset: 4n,
      }],
      total_byte_size: 4096n,
      num_rows: 1000n,
    }
  }

  it('returns false when no filter or column not found', () => {
    expect(canSkipRowGroup({ filter: { unknown: { $gt: 5 } }, rowGroup: makeRowGroup(1, 10), physicalColumns: ['x'] })).toBe(false)
  })

  it('returns false when no statistics available', () => {
    /** @type {any} */
    const rowGroup = { columns: [{ meta_data: {} }] }
    expect(canSkipRowGroup({ filter: { x: { $gt: 5 } }, rowGroup, physicalColumns: ['x'] })).toBe(false)
  })

  it('handles logical operators', () => {
    const rowGroup = makeRowGroup(10, 20)
    const cols = ['x']
    // $and: skip if ANY allows
    expect(canSkipRowGroup({ filter: { $and: [{ x: { $gt: 100 } }] }, rowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { $and: [{ x: { $gt: 5 } }] }, rowGroup, physicalColumns: cols })).toBe(false)
    // $or: skip only if ALL allow
    expect(canSkipRowGroup({ filter: { $or: [{ x: { $gt: 100 } }, { x: { $lt: 5 } }] }, rowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { $or: [{ x: { $gt: 5 } }, { x: { $lt: 5 } }] }, rowGroup, physicalColumns: cols })).toBe(false)
    // $nor: always conservative
    expect(canSkipRowGroup({ filter: { $nor: [{ x: { $gt: 100 } }] }, rowGroup, physicalColumns: cols })).toBe(false)
  })

  it('skips based on comparison operators', () => {
    const rowGroup = makeRowGroup(10, 20)
    const cols = ['x']
    // $gt: skip if max <= target
    expect(canSkipRowGroup({ filter: { x: { $gt: 20 } }, rowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { x: { $gt: 15 } }, rowGroup, physicalColumns: cols })).toBe(false)
    // $gte: skip if max < target
    expect(canSkipRowGroup({ filter: { x: { $gte: 21 } }, rowGroup, physicalColumns: cols })).toBe(true)
    // $lt: skip if min >= target
    expect(canSkipRowGroup({ filter: { x: { $lt: 10 } }, rowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { x: { $lt: 15 } }, rowGroup, physicalColumns: cols })).toBe(false)
    // $lte: skip if min > target
    expect(canSkipRowGroup({ filter: { x: { $lte: 9 } }, rowGroup, physicalColumns: cols })).toBe(true)
    // $eq: skip if target outside range
    expect(canSkipRowGroup({ filter: { x: { $eq: 5 } }, rowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { x: { $eq: 25 } }, rowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { x: { $eq: 15 } }, rowGroup, physicalColumns: cols })).toBe(false)
  })

  it('skips based on $ne, $in, $nin with uniform values', () => {
    const uniformRowGroup = makeRowGroup(5, 5)
    const cols = ['x']
    // $ne: skip only if min === max === target
    expect(canSkipRowGroup({ filter: { x: { $ne: 5 } }, rowGroup: uniformRowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { x: { $ne: 6 } }, rowGroup: uniformRowGroup, physicalColumns: cols })).toBe(false)
    // $in: skip if all values outside range
    expect(canSkipRowGroup({ filter: { x: { $in: [1, 2, 3] } }, rowGroup: uniformRowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { x: { $in: [4, 5, 6] } }, rowGroup: uniformRowGroup, physicalColumns: cols })).toBe(false)
    // $nin: skip only if uniform and value in array
    expect(canSkipRowGroup({ filter: { x: { $nin: [5, 6, 7] } }, rowGroup: uniformRowGroup, physicalColumns: cols })).toBe(true)
    expect(canSkipRowGroup({ filter: { x: { $nin: [1, 2, 3] } }, rowGroup: uniformRowGroup, physicalColumns: cols })).toBe(false)
  })

  it('handles min/max fallback to legacy fields', () => {
    /** @type {any} */
    const rowGroup = { columns: [{ meta_data: { statistics: { min: 10, max: 20 } } }] }
    expect(canSkipRowGroup({ filter: { x: { $gt: 25 } }, rowGroup, physicalColumns: ['x'] })).toBe(true)
  })

  it('continues when min or max undefined', () => {
    /** @type {any} */
    const rowGroup = { columns: [{ meta_data: { statistics: { min_value: 10 } } }] }
    expect(canSkipRowGroup({ filter: { x: { $gt: 5 } }, rowGroup, physicalColumns: ['x'] })).toBe(false)
  })
})
