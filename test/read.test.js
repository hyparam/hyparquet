import { describe, expect, it, vi } from 'vitest'
import { convertWithDictionary } from '../src/convert.js'
import { parquetMetadata, parquetReadObjects } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { parquetRead, parquetReadAsync } from '../src/read.js'
import { countingBuffer } from './helpers.js'

vi.mock('../src/convert.js', { spy: true })

describe('parquetReadObjects', () => {
  it('throws error for undefined file', async () => {
    // @ts-expect-error testing invalid input
    await expect(parquetReadObjects({ file: undefined }))
      .rejects.toThrow('parquet expected AsyncBuffer')
  })

  it('throws error for undefined byteLength', async () => {
    const file = { byteLength: undefined, slice: () => new ArrayBuffer(0) }
    // @ts-expect-error testing invalid input
    await expect(parquetReadObjects({ file }))
      .rejects.toThrow('parquet expected AsyncBuffer')
  })

  it('read row range', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const rows = await parquetReadObjects({
      file,
      rowStart: 2,
      rowEnd: 4,
    })
    expect(rows).toEqual([{ numbers: 3n }, { numbers: 4n }])
  })

  it('row range overestimate', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const rows = await parquetReadObjects({
      file,
      rowEnd: 100,
    })
    expect(rows).toEqual([
      { numbers: 1n }, { numbers: 2n }, { numbers: 3n }, { numbers: 4n }, { numbers: 5n },
      { numbers: 6n }, { numbers: 7n }, { numbers: 8n }, { numbers: 9n }, { numbers: 10n },
      { numbers: 11n }, { numbers: 12n }, { numbers: 13n }, { numbers: 14n }, { numbers: 15n },
    ])
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
    const rows = await parquetReadObjects({
      file,
      columns: ['int_map'],
    })
    expect(rows).toEqual([
      { int_map: { k1: 1, k2: 100 } },
      { int_map: { k1: 2, k2: null } },
      { int_map: { } },
      { int_map: { } },
      { int_map: { } },
      { int_map: undefined },
      { int_map: { k1: null, k3: null } },
    ])
  })

  it('read single column as objects', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetReadObjects({
      file,
      columns: ['c'],
    })
    expect(rows).toEqual([
      { c: 2 },
      { c: 3 },
      { c: 4 },
      { c: 5 },
      { c: 2 },
    ])
  })

  it('read selected columns', async () => {
    const file = await asyncBufferFromFile('test/files/datapage_v2.snappy.parquet')
    const rows = await parquetReadObjects({
      file,
      columns: ['c', 'b'],
    })
    expect(rows).toEqual([
      { b: 1, c: 2 },
      { b: 2, c: 3 },
      { b: 3, c: 4 },
      { b: 4, c: 5 },
      { b: 5, c: 2 },
    ])
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
    const metadata = await parquetMetadata({ file })
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

  it('reads only required row groups on the boundary', async () => {
    const originalFile = await asyncBufferFromFile('test/files/alpha.parquet')
    const metadata = await parquetMetadata({ file: originalFile })
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

  it('reads individual pages', async () => {
    const file = countingBuffer(await asyncBufferFromFile('test/files/page_indexed.parquet'))
    /** @type {import('../src/types.js').SubColumnData[]} */
    const pages = []

    // check onPage callback
    const gen = parquetReadAsync({
      file,
      onPage(page) {
        pages.push(page)
      },
    })
    // consume the generators
    for await (const rg of gen) {
      for (const col of rg.asyncColumns) {
        // eslint-disable-next-line no-unused-vars
        for await (const _ of col.data) { /* consume */ }
      }
    }

    const expectedPages = [
      {
        pathInSchema: ['row'],
        columnData: Array.from({ length: 100 }, (_, i) => BigInt(i)),
        rowStart: 0,
        rowEnd: 100,
      },
      {
        pathInSchema: ['quality'],
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
        pathInSchema: ['row'],
        columnData: Array.from({ length: 100 }, (_, i) => BigInt(i + 100)),
        rowStart: 100,
        rowEnd: 200,
      },
      {
        pathInSchema: ['quality'],
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
    ]

    // expect each page to exist in expected
    for (const expected of expectedPages) {
      const page = pages.find(p => p.pathInSchema[0] === expected.pathInSchema[0] && p.rowStart === expected.rowStart)
      expect(page).toEqual(expected)
    }
    expect(file.fetches).toBe(3) // 1 metadata, 2 rowgroups
    expect(file.bytes).toBe(6421)
  })
})
