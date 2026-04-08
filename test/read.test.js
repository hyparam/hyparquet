import { describe, expect, it, vi } from 'vitest'
import { parquetMetadataAsync, parquetRead, parquetReadObjects } from '../src/index.js'
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
        expect(rows).toEqual([[3n], [4n]])
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
          [1n], [2n], [3n], [4n], [5n], [6n], [7n], [8n], [9n], [10n], [11n], [12n], [13n], [14n], [15n],
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
          [[1, 2, 3]],
          [undefined],
          [undefined],
          [[1, 2, 3]],
          [[1, 2]],
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
          [{ k1: 1, k2: 100 }],
          [{ k1: 2, k2: null }],
          [{ }],
          [{ }],
          [{ }],
          [undefined],
          [{ k1: null, k3: null }],
        ])
      },
    })
  })

  it('format row as object', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file,
      columns: ['c'],
      rowFormat: 'object',
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

  it('read columns out of order', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file,
      columns: ['c', 'b', 'c'],
      onComplete(rows) {
        expect(rows).toEqual([
          [2, 1, 2],
          [3, 2, 3],
          [4, 3, 4],
          [5, 4, 5],
          [2, 5, 2],
        ])
      },
    })
  })

  it('throws error if requested column is not found', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await expect(parquetRead({
      file,
      columns: ['a', 'missing'],
      onComplete: () => {},
    })).rejects.toThrow('parquet column not found: missing')
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
    const metadata = await parquetMetadataAsync(file)
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

  it('uses OffsetIndex with dictionary-encoded columns', async () => {
    const file = await asyncBufferFromFile('test/files/dictionary_offset_indexed.parquet')
    const allRows = await parquetReadObjects({ file })

    const subset = await parquetReadObjects({
      file,
      rowStart: 50,
      rowEnd: 100,
      useOffsetIndex: true,
    })

    expect(subset).toHaveLength(50)
    for (let i = 0; i < subset.length; i++) {
      expect(subset[i]).toEqual(allRows[50 + i])
    }
  })

  it('uses OffsetIndex across multiple pages without repeating earlier rows', async () => {
    const file = await asyncBufferFromFile('test/files/dictionary_offset_indexed.parquet')
    const allRows = await parquetReadObjects({ file, useOffsetIndex: false })

    expect(allRows).toHaveLength(200)

    const firstWindow = await parquetReadObjects({
      file,
      rowStart: 0,
      rowEnd: 100,
      useOffsetIndex: true,
    })
    const secondWindow = await parquetReadObjects({
      file,
      rowStart: 100,
      rowEnd: 200,
      useOffsetIndex: true,
    })

    expect(firstWindow).toEqual(allRows.slice(0, 100))
    expect(secondWindow).toEqual(allRows.slice(100, 200))
  })

  it('uses OffsetIndex to skip pages', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
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

  it('uses OffsetIndex when dictionary_page_offset is missing (polars)', async () => {
    // polars writes RLE_DICTIONARY columns without setting dictionary_page_offset
    const file = await asyncBufferFromFile('test/files/offset_index_no_dict_offset.parquet')
    const allRows = await parquetReadObjects({ file })
    const rows = await parquetReadObjects({ file, rowEnd: 1, useOffsetIndex: true })
    expect(rows).toEqual(allRows.slice(0, 1))
  })

  it('uses OffsetIndex with struct sub-columns having different page counts', async () => {
    // struct sub-columns may have different page boundaries with offset index
    const file = await asyncBufferFromFile('test/files/struct_offset_index.parquet')
    const allRows = await parquetReadObjects({ file })
    const rows = await parquetReadObjects({ file, rowEnd: 3, useOffsetIndex: true })
    expect(rows).toEqual(allRows.slice(0, 3))
  })

  it('reads only required row groups on the boundary', async () => {
    const originalFile = await asyncBufferFromFile('test/files/alpha.parquet')
    const metadata = await parquetMetadataAsync(originalFile)
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
    const metadata = await parquetMetadataAsync(file)
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
    const metadata = await parquetMetadataAsync(file)
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
    const metadata = await parquetMetadataAsync(file)
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

    expect(pages.length).toBe(2) // 2 pages read
    expect(counting.fetches).toBe(1) // 1 column chunk
    expect(counting.bytes).toBe(14334)
  })

  it('filter rows with parquetRead', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file,
      rowFormat: 'object',
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

  it('skipped pages should not emit chunks with undefined data', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    /** @type {{ columnName: string, columnData: any, rowStart: number, rowEnd: number }[]} */
    const chunks = []
    await parquetRead({
      file,
      columns: ['content'],
      rowStart: 50,
      rowEnd: 100,
      onChunk(chunk) {
        chunks.push(chunk)
      },
    })

    // Every emitted chunk should contain only real string values, not undefined
    for (const chunk of chunks) {
      for (let i = 0; i < chunk.columnData.length; i++) {
        expect(typeof chunk.columnData[i]).toBe('string')
      }
    }

    // No chunk should start before the selection range's first relevant page
    // The first page that overlaps with row 50 starts at row 37
    for (const chunk of chunks) {
      expect(chunk.rowStart).toBeGreaterThanOrEqual(37)
    }
  })

  it('requires compressors for compressed files', async () => {
    const file = await asyncBufferFromFile('test/files/rle_boolean_encoding.parquet')
    await expect(parquetReadObjects({ file }))
      .rejects.toThrow('parquet unsupported compression codec: GZIP')
  })
})
