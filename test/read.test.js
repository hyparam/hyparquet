import { describe, expect, it, vi } from 'vitest'
import { convertWithDictionary } from '../src/convert.js'
import { parquetMetadataAsync, parquetRead, parquetReadObjects } from '../src/hyparquet.js'
import { asyncBufferFromFile } from '../src/utils.js'

vi.mock('../src/convert.js', { spy: true })

describe('parquetRead', () => {
  it('throws error for undefined file', async () => {
    // @ts-expect-error testing invalid input
    await expect(parquetRead({ file: undefined }))
      .rejects.toThrow('parquetRead expected file AsyncBuffer')
  })

  it('throws error for undefined byteLength', async () => {
    const file = { byteLength: undefined, slice: () => new ArrayBuffer(0) }
    // @ts-expect-error testing invalid input
    await expect(parquetRead({ file }))
      .rejects.toThrow('parquetRead expected file AsyncBuffer')
  })

  it('filter by row', async () => {
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

  it('filter by row overestimate', async () => {
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
      columns: ['c', 'missing', 'b', 'c'],
      onComplete(rows) {
        expect(rows).toEqual([
          [2, undefined, 1, 2],
          [3, undefined, 2, 3],
          [4, undefined, 3, 4],
          [5, undefined, 4, 5],
          [2, undefined, 5, 2],
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

  it('skips converting unnecessary pages', async () => {
    const file = await asyncBufferFromFile('test/files/page_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    vi.mocked(convertWithDictionary).mockClear()
    const rows = await parquetReadObjects({
      file,
      metadata,
      rowStart: 90,
      rowEnd: 91,
    })
    expect(rows).toEqual([{ row: 90n, quality: 'bad' }])
    expect(convertWithDictionary).toHaveBeenCalledTimes(4)
  })

  it('reads individual pages', async () => {
    const file = countingBuffer(await asyncBufferFromFile('test/files/page_indexed.parquet'))
    /** @type {ColumnData[]} */
    const pages = []

    await parquetRead({
      file,
      onPage(page) {
        pages.push(page)
      },
    })

    expect(pages).toEqual([
      {
        columnName: 'row',
        columnData: Array.from({ length: 100 }, (_, i) => BigInt(i)),
        rowStart: 0,
        rowEnd: 100,
      },
      {
        columnName: 'quality',
        columnData: [
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad', 'bad',
          'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good',
          'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad',
          'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'good', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad',
          'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad',
          'bad', 'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad',
        ],
        rowStart: 0,
        rowEnd: 100,
      },
      {
        columnName: 'row',
        columnData: Array.from({ length: 100 }, (_, i) => BigInt(i + 100)),
        rowStart: 100,
        rowEnd: 200,
      },
      {
        columnName: 'quality',
        columnData: [
          'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad', 'good', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'good', 'bad', 'bad', 'bad', 'good', 'bad',
          'bad', 'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
          'good', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad', 'bad',
        ],
        rowStart: 100,
        rowEnd: 200,
      },
    ])
    expect(file.fetches).toBe(3) // 1 metadata, 2 rowgroups
  })
})

/**
 * Wraps an AsyncBuffer to count the number of fetches made
 *
 * @import {AsyncBuffer, ColumnData} from '../src/types.js'
 * @param {AsyncBuffer} asyncBuffer
 * @returns {AsyncBuffer & {fetches: number}}
 */

function countingBuffer(asyncBuffer) {
  return {
    ...asyncBuffer,
    fetches: 0,
    slice(start, end) {
      this.fetches++
      return asyncBuffer.slice(start, end)
    },
  }
}
