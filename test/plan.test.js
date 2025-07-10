import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import {
  createColumnIndexMap,
  createPredicates,
  createRangePredicate,
  extractFilterColumns,
  getColumnRange,
  getRowGroupFullRange,
  parquetPlan,
} from '../src/plan.js'

describe('parquetPlan', () => {
  it('generates a query plan', async () => {
    const file = await asyncBufferFromFile('test/files/page_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const plan = parquetPlan({ file, metadata })
    expect(plan).toMatchObject({
      metadata,
      rowStart: 0,
      rowEnd: 200,
      fetches: [
        { startByte: 4, endByte: 1166 },
        { startByte: 1166, endByte: 2326 },
      ],
      groups: [
        {
          groupRows: 100,
          groupStart: 0,
          ranges: [
            { startByte: 4, endByte: 832 },
            { startByte: 832, endByte: 1166 },
          ],
        },
        {
          groupRows: 100,
          groupStart: 100,
          ranges: [
            { startByte: 1166, endByte: 1998 },
            { startByte: 1998, endByte: 2326 },
          ],
        },
      ],
    })
  })
})

describe('getColumnRange', () => {
  it('calculates byte range with dictionary page', () => {
    const range = getColumnRange({
      dictionary_page_offset: 100n,
      data_page_offset: 200n,
      total_compressed_size: 500n,
    })
    expect(range).toEqual({ startByte: 100, endByte: 600 })
  })

  it('calculates byte range without dictionary page', () => {
    const range = getColumnRange({
      data_page_offset: 200n,
      total_compressed_size: 300n,
    })
    expect(range).toEqual({ startByte: 200, endByte: 500 })
  })
})

describe('getRowGroupFullRange', () => {
  it('calculates full range including indexes', () => {
    const rowGroup = {
      columns: [
        {
          meta_data: {
            dictionary_page_offset: 100n,
            total_compressed_size: 200n,
          },
          column_index_offset: 400n,
          column_index_length: 50,
          offset_index_offset: 500n,
          offset_index_length: 60,
        },
        {
          meta_data: {
            data_page_offset: 300n,
            total_compressed_size: 100n,
          },
        },
      ],
    }

    const range = getRowGroupFullRange(rowGroup)
    expect(range).toEqual({
      start: 100,
      end: 560, // 500 + 60
      size: 460,
    })
  })

  it('handles empty row group', () => {
    const range = getRowGroupFullRange({ columns: [] })
    expect(range).toEqual({ start: Infinity, end: 0, size: -Infinity })
  })
})

describe('createColumnIndexMap', () => {
  it('creates mapping from column names to indexes', () => {
    const rowGroup = {
      columns: [
        { meta_data: { path_in_schema: ['name'] } },
        { meta_data: { path_in_schema: ['age'] } },
        { meta_data: { path_in_schema: ['city'] } },
      ],
    }

    const map = createColumnIndexMap(rowGroup)
    expect(map.get('name')).toBe(0)
    expect(map.get('age')).toBe(1)
    expect(map.get('city')).toBe(2)
    expect(map.size).toBe(3)
  })

  it('skips columns without metadata', () => {
    const rowGroup = {
      columns: [
        { meta_data: { path_in_schema: ['name'] } },
        {},
        { meta_data: { path_in_schema: [] } },
      ],
    }

    const map = createColumnIndexMap(rowGroup)
    expect(map.size).toBe(1)
    expect(map.get('name')).toBe(0)
  })
})

describe('extractFilterColumns', () => {
  it('extracts columns from simple filter', () => {
    const columns = extractFilterColumns({ name: 'John', age: 30 })
    expect(columns).toEqual(['name', 'age'])
  })

  it('extracts columns from $and filter', () => {
    const columns = extractFilterColumns({
      $and: [{ name: 'John' }, { age: { $gt: 25 } }],
    })
    expect(columns).toEqual(['name', 'age'])
  })

  it('extracts columns from $or filter', () => {
    const columns = extractFilterColumns({
      $or: [{ name: 'John' }, { name: 'Jane' }],
    })
    expect(columns).toEqual(['name'])
  })

  it('extracts columns from $nor filter', () => {
    const columns = extractFilterColumns({
      $nor: [{ status: 'inactive' }, { deleted: true }],
    })
    expect(columns).toEqual(['status', 'deleted'])
  })

  it('extracts columns from $not filter', () => {
    const columns = extractFilterColumns({
      $not: { age: { $lt: 18 } },
    })
    expect(columns).toEqual(['age'])
  })

  it('handles nested logical operators', () => {
    const columns = extractFilterColumns({
      $and: [
        { $or: [{ name: 'John' }, { name: 'Jane' }] },
        { age: { $gte: 18 } },
      ],
    })
    expect(columns).toEqual(['name', 'age'])
  })

  it('ignores operator keys', () => {
    const columns = extractFilterColumns({
      name: { $in: ['John', 'Jane'] },
      $comment: 'this should be ignored',
    })
    expect(columns).toEqual(['name'])
  })
})

describe('createPredicates', () => {
  it('creates predicates for simple equality', () => {
    const predicates = createPredicates({ age: 30 })
    expect(predicates.size).toBe(1)

    const agePred = predicates.get('age')
    expect(agePred(25, 35)).toBe(true) // 30 is in range
    expect(agePred(35, 40)).toBe(false) // 30 is not in range
  })

  it('creates predicates for $and conditions', () => {
    const predicates = createPredicates({
      $and: [{ age: { $gt: 25 } }, { age: { $lt: 35 } }],
    })
    expect(predicates.size).toBe(1)

    const agePred = predicates.get('age')
    // Only the last condition for 'age' is kept, which is $lt: 35
    expect(agePred(20, 30)).toBe(true) // min < 35
    expect(agePred(40, 50)).toBe(false) // min not < 35
  })

  it('ignores $or conditions', () => {
    const predicates = createPredicates({
      $or: [{ age: 30 }, { name: 'John' }],
    })
    expect(predicates.size).toBe(0)
  })

  it('handles mixed conditions', () => {
    const predicates = createPredicates({
      age: { $gte: 18 },
      status: 'active',
    })
    expect(predicates.size).toBe(2)
  })
})

describe('createRangePredicate', () => {
  it('handles direct value comparison', () => {
    const pred = createRangePredicate(42)
    expect(pred(40, 50)).toBe(true)
    expect(pred(50, 60)).toBe(false)
  })

  it('handles $eq operator', () => {
    const pred = createRangePredicate({ $eq: 42 })
    expect(pred(40, 50)).toBe(true)
    expect(pred(50, 60)).toBe(false)
  })

  it('handles $gt operator', () => {
    const pred = createRangePredicate({ $gt: 30 })
    expect(pred(20, 25)).toBe(false) // max not > 30
    expect(pred(20, 35)).toBe(true) // max > 30
    expect(pred(35, 40)).toBe(true) // all values > 30
  })

  it('handles $gte operator', () => {
    const pred = createRangePredicate({ $gte: 30 })
    expect(pred(20, 25)).toBe(false) // max not >= 30
    expect(pred(20, 30)).toBe(true) // max >= 30
    expect(pred(30, 40)).toBe(true) // all values >= 30
  })

  it('handles $lt operator', () => {
    const pred = createRangePredicate({ $lt: 30 })
    expect(pred(35, 40)).toBe(false) // min not < 30
    expect(pred(25, 35)).toBe(true) // min < 30
    expect(pred(20, 25)).toBe(true) // all values < 30
  })

  it('handles $lte operator', () => {
    const pred = createRangePredicate({ $lte: 30 })
    expect(pred(35, 40)).toBe(false) // min not <= 30
    expect(pred(30, 35)).toBe(true) // min <= 30
    expect(pred(20, 30)).toBe(true) // all values <= 30
  })

  it('handles $in operator', () => {
    const pred = createRangePredicate({ $in: [10, 20, 30] })
    expect(pred(5, 15)).toBe(true) // contains 10
    expect(pred(25, 35)).toBe(true) // contains 30
    expect(pred(35, 45)).toBe(false) // contains none
  })

  it('handles multiple operators', () => {
    const pred = createRangePredicate({ $gte: 20, $lt: 40 })
    expect(pred(10, 15)).toBe(false) // max < 20
    expect(pred(15, 25)).toBe(true) // overlaps [20, 40)
    expect(pred(35, 45)).toBe(true) // overlaps [20, 40)
    expect(pred(45, 50)).toBe(false) // min >= 40
  })

  it('returns null for unsupported operators', () => {
    const pred = createRangePredicate({ $ne: 42 })
    expect(pred).toBeDefined() // still creates a predicate
    expect(pred(40, 50)).toBe(true) // but always returns true for unsupported ops
  })
})
