import { describe, expect, it } from 'vitest'
import { hashParquetValue, sbbfInsert } from '../src/bloom.js'
import { canSkipRowGroup, matchFilter } from '../src/filter.js'

/**
 * @import { BloomFilter, RowGroup, SchemaElement } from '../src/types.js'
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

  it('handles dot-notation for nested struct fields', () => {
    const record = { bbox: { xmin: -73.1, ymin: 40.9, xmax: -73.0, ymax: 41.0 } }
    expect(matchFilter(record, { 'bbox.xmin': { $gte: -74, $lte: -73 } })).toBe(true)
    expect(matchFilter(record, { 'bbox.xmin': { $gte: -72, $lte: -71 } })).toBe(false)
    expect(matchFilter(record, { 'bbox.xmin': { $eq: -73.1 } })).toBe(true)
    expect(matchFilter(record, { 'bbox.xmin': { $eq: -73.2 } })).toBe(false)
  })

  it('handles deeply nested dot-notation', () => {
    const record = { a: { b: { c: 42 } } }
    expect(matchFilter(record, { 'a.b.c': { $eq: 42 } })).toBe(true)
    expect(matchFilter(record, { 'a.b.c': { $gt: 40 } })).toBe(true)
    expect(matchFilter(record, { 'a.b.c': { $lt: 40 } })).toBe(false)
  })

  it('returns false when nested path does not exist', () => {
    const record = { a: { b: 1 } }
    expect(matchFilter(record, { 'a.c': { $eq: 1 } })).toBe(false)
    expect(matchFilter(record, { 'x.y.z': { $eq: 1 } })).toBe(false)
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

describe('canSkipRowGroup with bloom filters', () => {
  /** @type {SchemaElement} */
  const nameSchema = { name: 'name', type: 'BYTE_ARRAY', converted_type: 'UTF8' }

  /**
   * Build a row group whose `name` column has UTF8 stats with the given min/max.
   *
   * @param {string} min
   * @param {string} max
   * @returns {RowGroup}
   */
  function nameRowGroup(min, max) {
    return {
      columns: [{
        meta_data: {
          type: 'BYTE_ARRAY',
          path_in_schema: ['name'],
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

  /**
   * Build a small bloom filter containing the given UTF-8 string values.
   *
   * @param {string[]} present
   * @returns {BloomFilter}
   */
  function bloomOf(present) {
    const blocks = new Uint32Array(8 * 4) // 4 blocks, generous for a few values
    for (const v of present) {
      const h = hashParquetValue(v, nameSchema)
      if (h === undefined) throw new Error('expected hash')
      sbbfInsert(blocks, h)
    }
    return { numBytes: blocks.byteLength, blocks }
  }

  const cols = ['name']
  const rowGroup = nameRowGroup('a', 'z')
  const present = bloomOf(['alice', 'bob'])
  const schemaElements = { name: nameSchema }

  it('skips $eq when the value is provably absent from the bloom filter', () => {
    // 'carol' is lexicographically in [a, z] so stats alone can't skip
    expect(canSkipRowGroup({ filter: { name: { $eq: 'carol' } }, rowGroup, physicalColumns: cols })).toBe(false)
    expect(canSkipRowGroup({
      filter: { name: { $eq: 'carol' } }, rowGroup, physicalColumns: cols,
      bloomFilters: { name: present }, schemaElements,
    })).toBe(true)
  })

  it('does not skip $eq when the bloom filter says the value might be present', () => {
    expect(canSkipRowGroup({
      filter: { name: { $eq: 'alice' } }, rowGroup, physicalColumns: cols,
      bloomFilters: { name: present }, schemaElements,
    })).toBe(false)
  })

  it('does not skip when the filter value cannot be hashed (lossy column type)', () => {
    /** @type {SchemaElement} */
    const dateSchema = { name: 'created', type: 'INT32', converted_type: 'DATE' }
    const dateRowGroup = nameRowGroup('a', 'z') // stats irrelevant; same shape
    // hashParquetValue returns undefined for DATE → bloom should not be consulted
    expect(canSkipRowGroup({
      filter: { name: { $eq: new Date() } }, rowGroup: dateRowGroup, physicalColumns: ['name'],
      bloomFilters: { name: present }, schemaElements: { name: dateSchema },
    })).toBe(false)
  })

  it('still skips via stats when bloom filter is missing', () => {
    // '~' (0x7e) sorts after 'z' lexicographically, so stats prove absence
    expect(canSkipRowGroup({ filter: { name: { $eq: '~' } }, rowGroup, physicalColumns: cols })).toBe(true)
  })

  it('skips via bloom even when statistics are missing entirely', () => {
    /** @type {any} */
    const noStats = { columns: [{ meta_data: { type: 'BYTE_ARRAY' } }] }
    expect(canSkipRowGroup({
      filter: { name: { $eq: 'carol' } }, rowGroup: noStats, physicalColumns: cols,
      bloomFilters: { name: present }, schemaElements,
    })).toBe(true)
  })

  it('$in: skips only when every value is provably absent from the bloom filter', () => {
    // All absent → skip
    expect(canSkipRowGroup({
      filter: { name: { $in: ['carol', 'dave'] } }, rowGroup, physicalColumns: cols,
      bloomFilters: { name: present }, schemaElements,
    })).toBe(true)
    // One present → don't skip
    expect(canSkipRowGroup({
      filter: { name: { $in: ['alice', 'carol'] } }, rowGroup, physicalColumns: cols,
      bloomFilters: { name: present }, schemaElements,
    })).toBe(false)
    // One unhashable target → conservative: don't skip
    expect(canSkipRowGroup({
      filter: { name: { $in: ['carol', 123] } }, rowGroup, physicalColumns: cols,
      bloomFilters: { name: present }, schemaElements,
    })).toBe(false)
  })

  it('ignores bloom filter on a different column', () => {
    expect(canSkipRowGroup({
      filter: { name: { $eq: 'carol' } }, rowGroup, physicalColumns: cols,
      bloomFilters: { other: present }, schemaElements,
    })).toBe(false)
  })

  it('does nothing without schemaElements (cannot hash the filter value)', () => {
    expect(canSkipRowGroup({
      filter: { name: { $eq: 'carol' } }, rowGroup, physicalColumns: cols,
      bloomFilters: { name: present },
    })).toBe(false)
  })

  it('passes bloom filters through $and / $or recursion', () => {
    // $and: skip if ANY clause allows. Bloom proves 'carol' absent → whole $and skips.
    expect(canSkipRowGroup({
      filter: { $and: [{ name: { $eq: 'carol' } }, { name: { $eq: 'alice' } }] },
      rowGroup, physicalColumns: cols, bloomFilters: { name: present }, schemaElements,
    })).toBe(true)
    // $or: skip only if EVERY clause allows. 'alice' is in bloom → cannot skip.
    expect(canSkipRowGroup({
      filter: { $or: [{ name: { $eq: 'carol' } }, { name: { $eq: 'alice' } }] },
      rowGroup, physicalColumns: cols, bloomFilters: { name: present }, schemaElements,
    })).toBe(false)
    // $or with both clauses provably absent → skip
    expect(canSkipRowGroup({
      filter: { $or: [{ name: { $eq: 'carol' } }, { name: { $eq: 'dave' } }] },
      rowGroup, physicalColumns: cols, bloomFilters: { name: present }, schemaElements,
    })).toBe(true)
  })
})
