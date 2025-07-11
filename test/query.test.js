import { describe, expect, it } from 'vitest'
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
    expect(file.bytes).toBe(5261)
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
