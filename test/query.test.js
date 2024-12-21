import { describe, expect, it } from 'vitest'
import { parquetQuery } from '../src/query.js'
import { asyncBufferFromFile, toJson } from '../src/utils.js'

describe('parquetQuery', () => {
  it('throws error for undefined file', async () => {
    // @ts-expect-error testing invalid input
    await expect(parquetQuery({ file: undefined }))
      .rejects.toThrow('parquet file is required')
  })

  it('reads data without orderBy', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file })
    expect(toJson(rows)).toEqual([
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
    expect(toJson(rows)).toEqual([
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
    expect(toJson(rows)).toEqual([
      { __index__: 4, a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
      { __index__: 1, a: 'abc', b: 2, c: 3, d: true },
      { __index__: 2, a: 'abc', b: 3, c: 4, d: true },
    ])
  })

  it('reads data with rowStart and rowEnd without orderBy', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, rowStart: 1, rowEnd: 4 })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('throws for invalid orderBy column', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const futureRows = parquetQuery({ file, orderBy: 'nonexistent' })
    await expect(futureRows).rejects.toThrow('parquet columns not found: nonexistent')
  })

  it('reads data with filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: 2 } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [ 1, 2, 3 ] },
      { a: 'abc', b: 5, c: 2, d: true, e: [ 1, 2 ] },
    ])
  })

  it('reads data with filter and rowStart/rowEnd', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: 2 }, rowStart: 1, rowEnd: 5 })
    expect(toJson(rows)).toEqual([ { a: 'abc', b: 5, c: 2, d: true, e: [ 1, 2 ] } ])
  })

  it('reads data with filter and orderBy', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: 2 }, orderBy: 'b' })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [ 1, 2, 3 ] },
      { a: 'abc', b: 5, c: 2, d: true, e: [ 1, 2 ] },
    ])
  })

  it('reads data with filter, orderBy, and rowStart/rowEnd', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: 2 }, orderBy: 'b', rowStart: 1, rowEnd: 2 })
    expect(toJson(rows)).toEqual([ { a: 'abc', b: 5, c: 2, d: true, e: [ 1, 2 ] } ])
  })

  it('reads data with $and filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { $and: [{ c: 2 }, { e: [1, 2, 3] }] } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
    ])
  })

  it('reads data with $or filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { $or: [{ c: 2 }, { d: false }] } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data with $not filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { $not: { c: 2 } } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('reads data with $not value filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { c: { $not: 2 } } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('reads data with $gt filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $gt: 3 } } })
    expect(toJson(rows)).toEqual([
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })


  it('reads data with $gte filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $gte: 3 } } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data with $lt filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $lt: 3 } } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
    ])
  })

  it('reads data with $lte filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $lte: 3 } } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
    ])
  })

  it('reads data with $ne filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $ne: 3 } } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('reads data with $in filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $in: [2, 4] } } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 2, c: 3, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
    ])
  })

  it('reads data with $nin filter', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetQuery({ file, filter: { b: { $nin: [2, 4] } } })
    expect(toJson(rows)).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })
})
