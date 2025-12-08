import { describe, expect, it, vi } from 'vitest'
import { parquetMetadata, parquetRead, parquetReadObjects } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { countingBuffer } from './helpers.js'

vi.mock('../src/convert.js', { spy: true })

describe('parquetRead', () => {
  it('throws error for undefined file', async () => {
    // @ts-expect-error testing invalid input
    await expect(parquetRead({ file: undefined }))
      .rejects.toThrow('parquet expected AsyncBuffer')
  })

  it('throws error for undefined byteLength', async () => {
    const file = { byteLength: undefined, slice: () => new ArrayBuffer(0) }
    // @ts-expect-error testing invalid input
    await expect(parquetRead({ file }))
      .rejects.toThrow('parquet expected AsyncBuffer')
  })

  it('read row range', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    await parquetRead({
      file,
      rowStart: 2,
      rowEnd: 4,
      onComplete(rows) {
        expect(rows).toEqual([{ numbers: 3n }, { numbers: 4n }])
      },
    })
  })

  it('row range overestimate', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    await parquetRead({
      file,
      rowEnd: 100,
      onComplete(rows) {
        expect(rows).toEqual([
          { numbers: 1n }, { numbers: 2n }, { numbers: 3n }, { numbers: 4n }, { numbers: 5n },
          { numbers: 6n }, { numbers: 7n }, { numbers: 8n }, { numbers: 9n }, { numbers: 10n },
          { numbers: 11n }, { numbers: 12n }, { numbers: 13n }, { numbers: 14n }, { numbers: 15n },
        ])
      },
    })
  })

  it('read a single column as typed array', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file,
      columns: ['b'],
      onChunk(chunk) {
        expect(chunk).toEqual({
          columnName: 'b',
          columnData: new Int32Array([1, 2, 3, 4, 5]),
          rowStart: 0,
          rowEnd: 5,
        })
        expect(chunk.columnData).toBeInstanceOf(Int32Array)
      },
    })
  })

  it('read a list-like column', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file,
      columns: ['e'],
      onChunk(chunk) {
        expect(chunk).toEqual({
          columnName: 'e',
          columnData: [[1, 2, 3], undefined, undefined, [1, 2, 3], [1, 2]],
          rowStart: 0,
          rowEnd: 5,
        })
      },
      onComplete(rows) {
        expect(rows).toEqual([
          { e: [1, 2, 3] },
          { e: undefined },
          { e: undefined },
          { e: [1, 2, 3] },
          { e: [1, 2] },
        ])
      },
    })
  })

  it('read a map-like column', async () => {
    const file = await asyncBufferFromFile('test/files/nullable.impala.parquet')
    await parquetRead({
      file,
      columns: ['int_map'],
      onChunk(chunk) {
        expect(chunk).toEqual({
          columnName: 'int_map',
          columnData: [
            { k1: 1, k2: 100 },
            { k1: 2, k2: null },
            { },
            { },
            { },
            undefined,
            { k1: null, k3: null },
          ],
          rowStart: 0,
          rowEnd: 7,
        })
      },
      onComplete(rows) {
        expect(rows).toEqual([
          { int_map: { k1: 1, k2: 100 } },
          { int_map: { k1: 2, k2: null } },
          { int_map: { } },
          { int_map: { } },
          { int_map: { } },
          { int_map: undefined },
          { int_map: { k1: null, k3: null } },
        ])
      },
    })
  })

  it('read single column as objects', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file,
      columns: ['c'],
      onComplete(rows) {
        expect(rows).toEqual([
          { c: 2 },
          { c: 3 },
          { c: 4 },
          { c: 5 },
          { c: 2 },
        ])
      },
    })
  })

  it('read selected columns', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file,
      columns: ['c', 'b'],
      onComplete(rows) {
        expect(rows).toEqual([
          { b: 1, c: 2 },
          { b: 2, c: 3 },
          { b: 3, c: 4 },
          { b: 4, c: 5 },
          { b: 5, c: 2 },
        ])
      },
    })
  })

  it('read objects and return a promise', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
      { a: 'abc', b: 3, c: 4, d: true },
      { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
      { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
    ])
  })

  it('does not use OffsetIndex by default', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadata(file)
    const counting = countingBuffer(file)
    const rows = await parquetReadObjects({
      file: counting,
      metadata,
      rowStart: 97,
      rowEnd: 98,
      columns: ['content'],
    })
    // expect(rows[0].id).toBe(98n)
    expect(rows[0].content).toMatch(/^brown data sit fox/)
    expect(counting.fetches).toBe(1) // 1 column chunk
    expect(counting.bytes).toBe(14334)
  })

  it('uses OffsetIndex to skip pages', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadata(file)
    const counting = countingBuffer(file)
    const rows = await parquetReadObjects({
      file: counting,
      metadata,
      rowStart: 97,
      rowEnd: 98,
      columns: ['content'],
      useOffsetIndex: true,
    })
    // much less data, one extra fetch for the offset index
    expect(rows[0].content).toMatch(/^brown data sit fox/)
    expect(counting.fetches).toBe(2) // 1 offset index + 1 page
    expect(counting.bytes).toBe(892)
  })

  it('reads only required row groups on the boundary', async () => {
    const originalFile = await asyncBufferFromFile('test/files/alpha.parquet')
    const metadata = await parquetMetadata(originalFile)
    const file = countingBuffer(originalFile)
    await parquetReadObjects({
      file,
      metadata,
      rowStart: 100,
      rowEnd: 200,
    })
    expect(file.fetches).toBe(1) // 1 rowgroup
    expect(file.bytes).toBe(441) // bytes for 2nd rowgroup
  })

  it('groups column chunks', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadata(file)
    const counting = countingBuffer(file)

    // check onPage callback
    const [row] = await parquetReadObjects({
      file: counting,
      metadata,
      rowStart: 25,
      rowEnd: 26,
    })
    expect(row).toEqual({ id: 26n, content: expect.any(String) })
    expect(counting.fetches).toBe(1) // 1 column chunk run
    expect(counting.bytes).toBe(14768)
  })

  it('does not groups column chunks when columns are specified', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadata(file)
    const counting = countingBuffer(file)

    // check onPage callback
    const [row] = await parquetReadObjects({
      file: counting,
      metadata,
      rowStart: 25,
      rowEnd: 26,
      columns: ['id', 'content'],
    })
    expect(row).toEqual({ id: 26n, content: expect.any(String) })
    expect(counting.fetches).toBe(2) // 2 column chunks
    expect(counting.bytes).toBe(14768)
  })

  it('reads individual pages', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadata(file)
    const counting = countingBuffer(file)
    /** @type {import('../src/types.js').SubColumnData[]} */
    const pages = []

    // check onPage callback
    await parquetRead({
      file: counting,
      metadata,
      rowStart: 25,
      rowEnd: 50,
      columns: ['content'],
      onPage(page) {
        pages.push(page)
      },
    })

    // TODO: should be 2 but we emit an empty page when skipping pages
    expect(pages.length).toBe(3) // 3 pages read
    expect(counting.fetches).toBe(1) // 1 column chunk
    expect(counting.bytes).toBe(14334)
  })

  it('filter rows with parquetRead', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file,
      filter: { b: { $gt: 2 } },
      onComplete(rows) {
        expect(rows).toEqual([
          { a: 'abc', b: 3, c: 4, d: true },
          { a: null, b: 4, c: 5, d: false, e: [1, 2, 3] },
          { a: 'abc', b: 5, c: 2, d: true, e: [1, 2] },
        ])
      },
    })
  })

  it('filter rows with parquetReadObjects', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetReadObjects({
      file,
      filter: { b: { $lte: 2 } },
    })
    expect(rows).toEqual([
      { a: 'abc', b: 1, c: 2, d: true, e: [1, 2, 3] },
      { a: 'abc', b: 2, c: 3, d: true },
    ])
  })

  it('filter with column projection', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    // Select columns a and d, but filter on b
    const rows = await parquetReadObjects({
      file,
      columns: ['a', 'd'],
      filter: { b: { $eq: 3 } },
    })
    // Result should only have a and d columns, not b
    expect(rows).toEqual([
      { a: 'abc', d: true },
    ])
  })
})
