import { describe, expect, it, vi } from 'vitest'
import { parquetQuery } from '../src/query.js'
import { asyncBufferFromFile } from '../src/node.js'
import { countingBuffer } from './helpers.js'

describe('parquetQuery', () => {
  it('throws error for undefined file', async () => {
    // @ts-expect-error testing invalid input
    await expect(parquetQuery({ file: undefined }))
      .rejects.toThrow('parquet expected AsyncBuffer')
  })

  it('reads data without orderBy', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data with orderBy', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, orderBy: 'c' })
    expect(rows).toEqual([
      { __index__: 0, a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { __index__: 4, a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
      { __index__: 1, a: 'abc', b: 2, c: 3, d: true },
      { __index__: 2, a: 'abc', b: 3, c: 4, d: true },
      { __index__: 3, a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('reads data with orderBy and limits', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, orderBy: 'c', rowStart: 1, rowEnd: 4 })
    expect(rows).toEqual([
      { __index__: 4, a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
      { __index__: 1, a: 'abc', b: 2, c: 3, d: true },
      { __index__: 2, a: 'abc', b: 3, c: 4, d: true },
    ])
  })

  it('reads data with rowStart and rowEnd without orderBy', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, rowStart: 1, rowEnd: 4 })
    expect(rows).toEqual([
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('reads data with filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: { $eq: 2 } } })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [ 1, 2, 3 ] },
      { a: 'abc', b: 5, c: 2, d: true, e: [ 1, 2 ] },
    ])
  })

  it('reads data with filter and rowStart/rowEnd', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: { $eq: 2 } }, rowStart: 1, rowEnd: 5 })
    expect(rows).toEqual([ { a: 'abc', b: 5, c: 2, d: true, e: [ 1, 2 ] } ])
  })

  it('reads data with filter and orderBy', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: { $eq: 2 } }, orderBy: 'b' })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [ 1, 2, 3 ] },
      { a: 'abc', b: 5, c: 2, d: true, e: [ 1, 2 ] },
    ])
  })

  it('reads data with filter, orderBy, and rowStart/rowEnd', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: { $eq: 2 } }, orderBy: 'b', rowStart: 1, rowEnd: 2 })
    expect(rows).toEqual([ { a: 'abc', b: 5, c: 2, d: true, e: [ 1, 2 ] } ])
  })

  it('reads data with multiple column filter operators', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: { $gt: 1, $lt: 4 }, d: { $eq: true } } })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data with $and filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { $and: [{ c: { $eq: 2 } }, { e: { $eq: [1, 2, 3] } }] } })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
    ])
  })

  it('reads data with $or filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { $or: [{ c: { $eq: 2 } }, { d: { $eq: false } }] } })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data with $nor filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { $nor: [{ c: { $eq: 2 } }, { d: { $eq: true } }] } })
    expect(rows).toEqual([
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('reads data with $not filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: { $not: { $eq: 2 } } } })
    expect(rows).toEqual([
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('reads data with $gt filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $gt: 3 } } })
    expect(rows).toEqual([
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })


  it('reads data with $gte filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $gte: 3 } } })
    expect(rows).toEqual([
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data with $lt filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $lt: 3 } } })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
    ])
  })

  it('reads data with $lte filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $lte: 3 } } })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
    ])
  })

  it('reads data with $ne filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $ne: 3 } } })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data with $in filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $in: [2, 4] } } })
    expect(rows).toEqual([
      { a: 'abc', b: 2, c: 3, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('reads data with $nin filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $nin: [2, 4] } } })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data efficiently with filter', async () => {
    const file = countingBuffer(await asyncBufferFromFile('test/files/page_indexed.parquet'))
    const rows = await parquetQuery({ file, filter: { quality: { $eq: 'good' } }, rowStart: 1, rowEnd: 5 } )
    expect(rows).toEqual([
      { row: 10n, quality: 'good' },
      { row: 29n, quality: 'good' },
      { row: 32n, quality: 'good' },
      { row: 37n, quality: 'good' },
    ])
    // if we weren't streaming row groups, this would be 3:
    expect(file.fetches).toBe(2) // 1 metadata, 1 row group
    expect(file.bytes).toBe(7253) // 4099 (metadata) + 3154 (row group 0 with indexes)
  })

  it('filter on columns that are not selected', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, columns: ['a', 'b'], filter: { c: { $eq: 2 } } })
    expect(rows).toEqual([
      { a: 'abc', b: 1 },
      { a: 'abc', b: 5 },
    ])
  })

  it('throws on non-existent column in filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await expect(parquetQuery({ file, filter: { nonExistent: { $eq: 1 } } }))
      .rejects.toThrow('parquet filter columns not found: nonExistent')
  })

  it('throws on non-existent column in orderBy', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await expect(parquetQuery({ file, orderBy: 'nonExistent' }))
      .rejects.toThrow('parquet orderBy column not found: nonExistent')
  })
})

// Import functions we want to test directly
import { assembleRows, matchesCondition, matchesFilter, projectRow, sliceAll, sortRows } from '../src/query.js'

describe('matchesFilter', () => {
  it('matches simple equality', () => {
    expect(matchesFilter({ name: 'John', age: 30 }, { name: 'John' })).toBe(true)
    expect(matchesFilter({ name: 'Jane', age: 30 }, { name: 'John' })).toBe(false)
  })

  it('matches multiple conditions (implicit AND)', () => {
    const row = { name: 'John', age: 30, city: 'NYC' }
    expect(matchesFilter(row, { name: 'John', age: 30 })).toBe(true)
    expect(matchesFilter(row, { name: 'John', age: 25 })).toBe(false)
  })

  it('matches $and conditions', () => {
    const row = { name: 'John', age: 30 }
    expect(matchesFilter(row, { $and: [{ name: 'John' }, { age: 30 }] })).toBe(true)
    expect(matchesFilter(row, { $and: [{ name: 'John' }, { age: 25 }] })).toBe(false)
  })

  it('matches $or conditions', () => {
    const row = { name: 'John', age: 30 }
    expect(matchesFilter(row, { $or: [{ name: 'Jane' }, { age: 30 }] })).toBe(true)
    expect(matchesFilter(row, { $or: [{ name: 'Jane' }, { age: 25 }] })).toBe(false)
  })

  it('matches $nor conditions', () => {
    const row = { name: 'John', age: 30 }
    expect(matchesFilter(row, { $nor: [{ name: 'Jane' }, { age: 25 }] })).toBe(true)
    expect(matchesFilter(row, { $nor: [{ name: 'John' }, { age: 25 }] })).toBe(false)
  })

  it('matches $not conditions', () => {
    const row = { name: 'John', age: 30 }
    expect(matchesFilter(row, { $not: { name: 'Jane' } })).toBe(true)
    expect(matchesFilter(row, { $not: { name: 'John' } })).toBe(false)
  })

  it('handles nested logical operators', () => {
    const row = { name: 'John', age: 30, status: 'active' }
    const filter = {
      $and: [
        { $or: [{ name: 'John' }, { name: 'Jane' }] },
        { age: { $gte: 25 } },
        { status: 'active' },
      ],
    }
    expect(matchesFilter(row, filter)).toBe(true)
  })

  it('ignores $ prefixed keys at top level', () => {
    const row = { name: 'John' }
    expect(matchesFilter(row, { name: 'John', $comment: 'ignored' })).toBe(true)
  })
})

describe('matchesCondition', () => {
  it('matches direct value equality', () => {
    expect(matchesCondition('John', 'John')).toBe(true)
    expect(matchesCondition(30, 30)).toBe(true)
    expect(matchesCondition(true, true)).toBe(true)
    expect(matchesCondition('John', 'Jane')).toBe(false)
  })

  it('matches null values', () => {
    expect(matchesCondition(null, null)).toBe(true)
    expect(matchesCondition(undefined, null)).toBe(false) // undefined !== null
    expect(matchesCondition('value', null)).toBe(false)
  })

  it('matches arrays', () => {
    expect(matchesCondition([1, 2, 3], [1, 2, 3])).toBe(true)
    expect(matchesCondition([1, 2, 3], [3, 2, 1])).toBe(false)
  })

  it('matches $eq operator', () => {
    expect(matchesCondition(42, { $eq: 42 })).toBe(true)
    expect(matchesCondition(42, { $eq: 43 })).toBe(false)
  })

  it('matches $ne operator', () => {
    expect(matchesCondition(42, { $ne: 43 })).toBe(true)
    expect(matchesCondition(42, { $ne: 42 })).toBe(false)
  })

  it('matches $gt operator', () => {
    expect(matchesCondition(42, { $gt: 40 })).toBe(true)
    expect(matchesCondition(42, { $gt: 42 })).toBe(false)
    expect(matchesCondition(42, { $gt: 45 })).toBe(false)
  })

  it('matches $gte operator', () => {
    expect(matchesCondition(42, { $gte: 40 })).toBe(true)
    expect(matchesCondition(42, { $gte: 42 })).toBe(true)
    expect(matchesCondition(42, { $gte: 45 })).toBe(false)
  })

  it('matches $lt operator', () => {
    expect(matchesCondition(42, { $lt: 45 })).toBe(true)
    expect(matchesCondition(42, { $lt: 42 })).toBe(false)
    expect(matchesCondition(42, { $lt: 40 })).toBe(false)
  })

  it('matches $lte operator', () => {
    expect(matchesCondition(42, { $lte: 45 })).toBe(true)
    expect(matchesCondition(42, { $lte: 42 })).toBe(true)
    expect(matchesCondition(42, { $lte: 40 })).toBe(false)
  })

  it('matches $in operator', () => {
    expect(matchesCondition('apple', { $in: ['apple', 'banana'] })).toBe(true)
    expect(matchesCondition('cherry', { $in: ['apple', 'banana'] })).toBe(false)
    expect(matchesCondition(42, { $in: [40, 42, 44] })).toBe(true)
  })

  it('matches $nin operator', () => {
    expect(matchesCondition('cherry', { $nin: ['apple', 'banana'] })).toBe(true)
    expect(matchesCondition('apple', { $nin: ['apple', 'banana'] })).toBe(false)
  })

  it('matches $not operator', () => {
    expect(matchesCondition(42, { $not: { $gt: 50 } })).toBe(true)
    expect(matchesCondition(42, { $not: { $lt: 50 } })).toBe(false)
  })

  it('matches multiple operators (all must be true)', () => {
    expect(matchesCondition(42, { $gte: 40, $lte: 45 })).toBe(true)
    expect(matchesCondition(42, { $gte: 40, $lt: 42 })).toBe(false)
  })
})

describe('sortRows', () => {
  it('sorts rows by column ascending', () => {
    const rows = [
      { name: 'Charlie', age: 30 },
      { name: 'Alice', age: 25 },
      { name: 'Bob', age: 35 },
    ]
    sortRows(rows, 'name', false)
    expect(rows.map(r => r.name)).toEqual(['Alice', 'Bob', 'Charlie'])
  })

  it('sorts rows by column descending', () => {
    const rows = [
      { name: 'Charlie', age: 30 },
      { name: 'Alice', age: 25 },
      { name: 'Bob', age: 35 },
    ]
    sortRows(rows, 'age', true)
    expect(rows.map(r => r.age)).toEqual([35, 30, 25])
  })

  it('handles null values', () => {
    const rows = [
      { name: 'Charlie', age: 30 },
      { name: null, age: 25 },
      { name: 'Bob', age: 35 },
      { name: undefined, age: 40 },
    ]
    sortRows(rows, 'name', false)
    // nulls go to end when ascending
    expect(rows.map(r => r.name)).toEqual(['Bob', 'Charlie', null, undefined])
  })

  it('maintains stable sort with __index__', () => {
    const rows = [
      { name: 'Alice', age: 30, __index__: 0 },
      { name: 'Bob', age: 30, __index__: 1 },
      { name: 'Charlie', age: 30, __index__: 2 },
    ]
    sortRows(rows, 'age', false)
    // Same age values maintain original order
    expect(rows.map(r => r.name)).toEqual(['Alice', 'Bob', 'Charlie'])
  })
})

describe('projectRow', () => {
  it('projects selected columns', () => {
    const row = { name: 'John', age: 30, city: 'NYC', country: 'USA' }
    const projected = projectRow(row, ['name', 'age'])
    expect(projected).toEqual({ name: 'John', age: 30 })
  })

  it('handles missing columns', () => {
    const row = { name: 'John', age: 30 }
    const projected = projectRow(row, ['name', 'city'])
    expect(projected).toEqual({ name: 'John', city: undefined })
  })

  it('preserves column order', () => {
    const row = { z: 3, y: 2, x: 1 }
    const projected = projectRow(row, ['x', 'y', 'z'])
    expect(Object.keys(projected)).toEqual(['x', 'y', 'z'])
  })
})

describe('assembleRows', () => {
  it('assembles column data into rows', () => {
    const columnData = new Map([
      ['name', ['Alice', 'Bob', 'Charlie']],
      ['age', [25, 30, 35]],
    ])
    const rows = assembleRows(columnData, ['name', 'age'])
    expect(rows).toEqual([
      { name: 'Alice', age: 25 },
      { name: 'Bob', age: 30 },
      { name: 'Charlie', age: 35 },
    ])
  })

  it('handles missing columns', () => {
    const columnData = new Map([
      ['name', ['Alice', 'Bob']],
    ])
    const rows = assembleRows(columnData, ['name', 'age'])
    expect(rows).toEqual([
      { name: 'Alice', age: null },
      { name: 'Bob', age: null },
    ])
  })

  it('handles uneven column lengths', () => {
    const columnData = new Map([
      ['name', ['Alice', 'Bob', 'Charlie']],
      ['age', [25, 30]], // shorter
    ])
    const rows = assembleRows(columnData, ['name', 'age'])
    expect(rows).toEqual([
      { name: 'Alice', age: 25 },
      { name: 'Bob', age: 30 },
      { name: 'Charlie', age: null },
    ])
  })

  it('returns empty array for empty data', () => {
    const columnData = new Map()
    const rows = assembleRows(columnData, ['name', 'age'])
    expect(rows).toEqual([])
  })
})

describe('sliceAll', () => {
  it('uses native sliceAll when available', async () => {
    const mockFile = {
      byteLength: 1000,
      slice: vi.fn(),
      sliceAll: vi.fn().mockResolvedValue([
        new ArrayBuffer(10),
        new ArrayBuffer(20),
      ]),
    }
    /** @type {[number, number][]} */
    const ranges = [[0, 10], [20, 40]]
    const result = await sliceAll(mockFile, ranges)

    expect(mockFile.sliceAll).toHaveBeenCalledWith(ranges)
    expect(result).toHaveLength(2)
    expect(result[0].byteLength).toBe(10)
    expect(result[1].byteLength).toBe(20)
  })

  it('falls back to parallel slices', async () => {
    const mockFile = {
      byteLength: 1000,
      slice: vi.fn()
        .mockResolvedValueOnce(new ArrayBuffer(10))
        .mockResolvedValueOnce(new ArrayBuffer(20)),
    }
    /** @type {[number, number][]} */
    const ranges = [[0, 10], [20, 40]]
    const result = await sliceAll(mockFile, ranges)

    expect(mockFile.slice).toHaveBeenCalledTimes(2)
    expect(mockFile.slice).toHaveBeenCalledWith(0, 10)
    expect(mockFile.slice).toHaveBeenCalledWith(20, 40)
    expect(result).toHaveLength(2)
  })

  it('handles null ranges', async () => {
    const mockFile = {
      byteLength: 1000,
      slice: vi.fn().mockResolvedValue(new ArrayBuffer(10)),
    }
    /** @type {([number, number] | null)[]} */
    const ranges = [[0, 10], null, [20, 30]]
    const result = await sliceAll(mockFile, ranges)

    expect(mockFile.slice).toHaveBeenCalledTimes(2)
    expect(result).toHaveLength(3)
    expect(result[1].byteLength).toBe(0) // null range returns empty buffer
  })
})
