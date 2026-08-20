import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync, parquetScan } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { parquetReadColumn } from '../src/read.js'
import { countingBuffer } from './helpers.js'

describe('parquetScan', () => {
  it('exposes physical ranges and reads columns lazily', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const counted = countingBuffer(file)
    const scan = await parquetScan({
      file: counted,
      columns: ['numbers'],
      rowStart: 2,
      rowEnd: 13,
    })

    expect(scan.ranges).toEqual([
      { rowStart: 2, rowEnd: 10 },
      { rowStart: 10, rowEnd: 13 },
    ])
    const fetchesAfterPlanning = counted.fetches
    expect(fetchesAfterPlanning).toBeGreaterThan(0) // footer only

    const first = await scan.readColumn({ column: 'numbers', ...scan.ranges[0] })
    expect(first).toEqual([3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n])
    expect(counted.fetches).toBeGreaterThan(fetchesAfterPlanning)
  })

  it('reads an exact subrange and reuses a cached covering range', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const counted = countingBuffer(file)
    const scan = await parquetScan({ file: counted, columns: ['numbers'] })
    const range = scan.ranges[0]

    expect(await scan.readColumn({ column: 'numbers', ...range }))
      .toEqual([1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n])
    const fetchesAfterFullRange = counted.fetches

    expect(await scan.readColumn({ column: 'numbers', rowStart: 3, rowEnd: 6 }))
      .toEqual([4n, 5n, 6n])
    expect(counted.fetches).toBe(fetchesAfterFullRange)
  })

  it('reuses its retained row-group plan for column reads', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const metadata = await parquetMetadataAsync(file)
    const rowGroups = metadata.row_groups
    let rowGroupReads = 0
    Object.defineProperty(metadata, 'row_groups', {
      get() {
        rowGroupReads++
        return rowGroups
      },
    })

    const scan = await parquetScan({ file, metadata, columns: ['numbers'] })
    const readsAfterPlanning = rowGroupReads
    await scan.readColumn({ column: 'numbers', ...scan.ranges[0] })
    expect(rowGroupReads).toBe(readsAfterPlanning)
  })

  it('defers column planning until a column is read', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const metadata = await parquetMetadataAsync(file)
    const { columns } = metadata.row_groups[0]
    let columnReads = 0
    Object.defineProperty(metadata.row_groups[0], 'columns', {
      get() {
        columnReads++
        return columns
      },
    })

    const scan = await parquetScan({ file, metadata, columns: ['numbers'] })
    expect(columnReads).toBe(0)
    await scan.readColumn({ column: 'numbers', ...scan.ranges[0] })
    expect(columnReads).toBeGreaterThan(0)
  })

  it('uses offset indexes by default for exact subranges', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const counted = countingBuffer(file)
    const scan = await parquetScan({ file: counted, metadata, columns: ['content'] })

    const content = await scan.readColumn({ column: 'content', rowStart: 97, rowEnd: 98 })
    expect(content).toHaveLength(1)
    expect(content[0]).toMatch(/^brown data sit fox/)
    expect(counted.fetches).toBe(2) // offset index and selected page
    expect(counted.bytes).toBe(892)
  })

  it('uses row-group statistics for candidate-range pruning', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const scan = await parquetScan({
      file,
      columns: ['numbers'],
      pruningFilter: { numbers: { $gt: 10n } },
    })

    expect(scan.ranges).toEqual([{ rowStart: 10, rowEnd: 15 }])
    expect(await scan.readColumn({ column: 'numbers', ...scan.ranges[0] }))
      .toEqual([11n, 12n, 13n, 14n, 15n])
  })

  it('uses bloom filters without applying the pruning filter to values', async () => {
    const file = await asyncBufferFromFile('test/files/bloom_filter.parquet')
    const scan = await parquetScan({
      file,
      columns: ['code'],
      pruningFilter: { code: { $eq: 30 } },
      useBloomFilters: true,
    })

    expect(scan.ranges).toEqual([{ rowStart: 2048, rowEnd: 4096 }])
    const values = await scan.readColumn({ column: 'code', ...scan.ranges[0] })
    // The bloom filter prunes whole ranges. Exact row filtering remains the
    // caller's responsibility, so both physical values in the group remain.
    expect(new Set(values)).toEqual(new Set([30, 70]))
  })

  it('uses page indexes and preserves absolute physical coordinates', async () => {
    const file = await asyncBufferFromFile('test/files/page_index.parquet')
    const metadata = await parquetMetadataAsync(file)
    const counted = countingBuffer(file)
    const scan = await parquetScan({
      file: counted,
      metadata,
      columns: ['word'],
      pruningFilter: { id: { $eq: 1234 } },
      usePageIndex: true,
    })

    expect(scan.ranges).toEqual([{ rowStart: 1196, rowEnd: 1495 }])
    // Filter columns are available automatically even when not projected.
    const ids = await scan.readColumn({ column: 'id', ...scan.ranges[0] })
    expect(ids).toBeInstanceOf(Int32Array)
    expect(ids[0]).toBe(1196)
    expect(ids[ids.length - 1]).toBe(1494)
    expect(ids).toContain(1234)

    const oneId = await scan.readColumn({ column: 'id', rowStart: 1234, rowEnd: 1235 })
    expect(oneId).toBeInstanceOf(Int32Array)
    if (!(ids instanceof Int32Array) || !(oneId instanceof Int32Array)) {
      throw new Error('expected typed id columns')
    }
    expect(oneId.buffer).toBe(ids.buffer)

    const bytesBeforeWord = counted.bytes
    const words = await scan.readColumn({ column: 'word', rowStart: 1234, rowEnd: 1235 })
    expect(words).toEqual(['word-001234'])
    const fullWordChunkBytes = Number(metadata.row_groups[0].columns[1].meta_data?.total_compressed_size)
    expect(counted.bytes - bytesBeforeWord).toBeLessThan(fullWordChunkBytes)
  })

  it('validates columns and physical ranges at the API boundary', async () => {
    const file = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const scan = await parquetScan({ file, columns: ['numbers'] })

    expect(() => scan.readColumn({ column: 'missing', rowStart: 0, rowEnd: 1 }))
      .toThrow('parquet column not found in scan: missing')
    expect(() => scan.readColumn({ column: 'numbers', rowStart: 9, rowEnd: 11 }))
      .toThrow('outside scan ranges')
    expect(() => scan.readColumn({ column: 'numbers', rowStart: -1, rowEnd: 1 }))
      .toThrow('rowStart')
    expect(() => scan.readColumn({ column: 'numbers', rowStart: 2, rowEnd: 1 }))
      .toThrow('rowEnd')
  })

  it('does not fetch filter-only column data in parquetReadColumn', async () => {
    const file = await asyncBufferFromFile('test/files/page_index.parquet')
    const metadata = await parquetMetadataAsync(file)
    const counted = countingBuffer(file)
    const words = await parquetReadColumn({
      file: counted,
      metadata,
      columns: ['word'],
      filter: { id: { $eq: 1234 } },
    })

    const wordBytes = Number(metadata.row_groups[0].columns[1].meta_data?.total_compressed_size)
    expect(words).toHaveLength(1500)
    expect(counted.fetches).toBe(1)
    expect(counted.bytes).toBe(wordBytes)
  })
})
