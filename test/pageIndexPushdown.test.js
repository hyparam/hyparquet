import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync, parquetReadObjects } from '../src/index.js'
import { filterPageRanges, intersectRanges, unionRanges } from '../src/filter.js'
import { asyncBufferFromFile } from '../src/node.js'
import { parquetPlan, prefetchPageIndexes } from '../src/plan.js'

/**
 * @import {AsyncBuffer, ColumnPageStats, ParquetQueryFilter} from '../src/types.js'
 */

/**
 * Wrap an AsyncBuffer to count fetched byte ranges.
 *
 * @param {AsyncBuffer} file
 * @returns {AsyncBuffer & {bytes(): number, fetches(): number, ranges(): [number, number][]}}
 */
function countingBuffer(file) {
  let bytes = 0
  let fetches = 0
  /** @type {[number, number][]} */
  const ranges = []
  return {
    byteLength: file.byteLength,
    slice(start, end = file.byteLength) {
      fetches++
      bytes += end - start
      ranges.push([start, end])
      return file.slice(start, end)
    },
    bytes: () => bytes,
    fetches: () => fetches,
    ranges: () => ranges,
  }
}

/**
 * Layout of test/files/page_index.parquet (3,000 rows in 2 row groups of 1,500):
 *   id: ascending INT32 0..2999, column index enabled, 5 pages of 300 rows per group
 *   word: ascending strings word-000000..word-002999, column index enabled
 *   payload: text, offset index only (no column index)
 *   category: repeated strings cat-0..cat-49, dictionary encoded
 * The dictionary-encoded category column exercises the separate dictionary
 * page fetch when leading data pages are skipped.
 */
describe('page index pushdown against test/files/page_index.parquet', () => {
  const path = 'test/files/page_index.parquet'

  it('fixture has column indexes and multiple pages per row group', async () => {
    const file = await asyncBufferFromFile(path)
    const metadata = await parquetMetadataAsync(file)
    expect(metadata.row_groups).toHaveLength(2)
    for (const rowGroup of metadata.row_groups) {
      const idChunk = rowGroup.columns[0]
      expect(idChunk.column_index_offset).toBeDefined()
      expect(idChunk.offset_index_offset).toBeDefined()
    }
    const { pageLocationsByGroup } = await prefetchPageIndexes({
      file, metadata, filter: { id: { $eq: 0 } },
    })
    expect(pageLocationsByGroup[0].id.length).toBeGreaterThan(1)
  })

  it('prefetchPageIndexes narrows candidate ranges to matching pages', async () => {
    const file = await asyncBufferFromFile(path)
    const metadata = await parquetMetadataAsync(file)
    const { pageRangesByGroup } = await prefetchPageIndexes({
      file, metadata, filter: { id: { $eq: 1234 } },
    })
    // row group 0 keeps only the page containing row 1234
    const ranges = pageRangesByGroup[0]
    const range = ranges?.[0]
    if (!range) throw new Error('expected one candidate page range')
    expect(ranges).toHaveLength(1)
    expect(range[0]).toBeLessThanOrEqual(1234)
    expect(range[1]).toBeGreaterThan(1234)
    expect(range[1] - range[0]).toBeLessThan(1500)
    // row group 1 was already skipped by row-group stats
    expect(pageRangesByGroup[1]).toBeUndefined()
  })

  it('parquetPlan splits a group into sub-ranges and drops groups with no candidate pages', async () => {
    const file = await asyncBufferFromFile(path)
    const metadata = await parquetMetadataAsync(file)
    // two values in distant pages of row group 0
    const filter = { id: { $in: [100, 1400] } }
    const { pageRangesByGroup, pageLocationsByGroup } = await prefetchPageIndexes({ file, metadata, filter })
    const plan = parquetPlan({ file, metadata, filter, pageRangesByGroup, pageLocationsByGroup })
    // row group 0 splits into two sub-ranges; row group 1 skipped by group stats
    const rg0Plans = plan.groups.filter(g => g.groupStart === 0)
    expect(rg0Plans.length).toBe(2)
    expect(rg0Plans[0].selectEnd).toBeLessThanOrEqual(rg0Plans[1].selectStart)
    for (const groupPlan of rg0Plans) {
      for (const chunk of groupPlan.chunks) {
        // every selected chunk reuses its prefetched page layout
        expect('pageLocations' in chunk).toBe(true)
      }
    }
  })

  it('emits each coarse output page and chunk only once', async () => {
    const file = await asyncBufferFromFile(path)
    const metadata = await parquetMetadataAsync(file)
    /** @type {import('../src/types.js').SubColumnData[]} */
    const pages = []
    /** @type {import('../src/types.js').ColumnData[]} */
    const chunks = []

    // These values occupy disjoint word pages but both ranges overlap the
    // first, coarser id page.
    const rows = await parquetReadObjects({
      file,
      metadata,
      columns: ['id'],
      filter: { word: { $in: ['word-000042', 'word-000250'] } },
      usePageIndex: true,
      onPage: page => pages.push(page),
      onChunk: chunk => chunks.push(chunk),
    })

    expect(rows.map(row => row.id)).toEqual([42, 250])
    expect(pages
      .filter(page => page.pathInSchema[0] === 'id')
      .map(page => [page.rowStart, page.rowEnd])
    ).toEqual([[0, 299], [299, 598]])
    expect(chunks
      .filter(chunk => chunk.columnName === 'id')
      .map(chunk => [chunk.rowStart, chunk.rowEnd])
    ).toEqual([[0, 299], [299, 598]])
  })

  it('drops a row group when page conditions have no overlapping candidates', async () => {
    const file = await asyncBufferFromFile(path)
    const metadata = await parquetMetadataAsync(file)
    // Both values are inside row group 0, but occupy disjoint pages.
    const filter = { $and: [{ id: { $eq: 100 } }, { id: { $eq: 1400 } }] }
    const { pageRangesByGroup, pageLocationsByGroup } = await prefetchPageIndexes({ file, metadata, filter })
    expect(pageRangesByGroup[0]).toEqual([])
    const plan = parquetPlan({ file, metadata, filter, pageRangesByGroup, pageLocationsByGroup })
    expect(plan.groups).toEqual([])
  })

  it('e2e: results with usePageIndex match results without, for many filters', async () => {
    const file = await asyncBufferFromFile(path)
    /** @type {ParquetQueryFilter[]} */
    const filters = [
      { id: { $eq: 1234 } },
      { id: { $in: [3, 1321, 2999] } },
      { id: { $gt: 1490, $lt: 1510 } }, // straddles the row group boundary
      { id: { $gte: 2990 } },
      { word: { $eq: 'word-002222' } },
      { word: { $gt: 'word-002995' } },
      { id: { $eq: 123456 } }, // absent value
      { $or: [{ id: { $lt: 5 } }, { id: { $gt: 2995 } }] },
      { $and: [{ id: { $gt: 100 } }, { word: { $lt: 'word-000200' } }] },
      { id: { $ne: 0 } }, // not prunable, must still be correct
    ]
    for (const filter of filters) {
      const expected = await parquetReadObjects({ file, filter })
      const actual = await parquetReadObjects({ file, filter, usePageIndex: true })
      expect(actual, JSON.stringify(filter)).toEqual(expected)
    }
  })

  it('e2e: usePageIndex composes with rowStart/rowEnd', async () => {
    const file = await asyncBufferFromFile(path)
    const filter = { id: { $in: [100, 1400, 1600] } }
    const expected = await parquetReadObjects({ file, filter, rowStart: 200, rowEnd: 1700 })
    const actual = await parquetReadObjects({ file, filter, rowStart: 200, rowEnd: 1700, usePageIndex: true })
    expect(actual).toEqual(expected)
    expect(actual.map(row => row.id)).toEqual([1400, 1600])
  })

  it('fetches far fewer data bytes with usePageIndex', async () => {
    const file = await asyncBufferFromFile(path)
    // pass metadata so the footer fetch is excluded from both byte counts
    const metadata = await parquetMetadataAsync(file)
    const filter = { word: { $eq: 'word-001234' } }

    const plain = countingBuffer(file)
    const expected = await parquetReadObjects({ file: plain, metadata, filter })

    const paged = countingBuffer(file)
    const actual = await parquetReadObjects({ file: paged, metadata, filter, usePageIndex: true })

    expect(actual).toEqual(expected)
    expect(actual).toHaveLength(1)
    // without pushdown: all of row group 0
    // with pushdown: page indexes plus the pages covering one short row range
    expect(paged.bytes()).toBeLessThan(plain.bytes() / 3)
  })

  it('dictionary column skips leading pages but still fetches the dictionary', async () => {
    const file = await asyncBufferFromFile(path)
    const metadata = await parquetMetadataAsync(file)
    // narrow to a page in the middle of row group 0: the dictionary-encoded
    // category column must fetch its dictionary page separately
    const filter = { id: { $eq: 1234 } }

    const plain = countingBuffer(file)
    const expected = await parquetReadObjects({ file: plain, metadata, filter })

    const paged = countingBuffer(file)
    const actual = await parquetReadObjects({ file: paged, metadata, filter, usePageIndex: true })

    expect(actual).toEqual(expected)
    expect(actual[0].category).toBe(`cat-${1234 % 50}`)
    expect(paged.bytes()).toBeLessThan(plain.bytes() / 3)
  })

  it('absent value fetches only page indexes, no data pages', async () => {
    const file = await asyncBufferFromFile(path)
    const metadata = await parquetMetadataAsync(file)
    const paged = countingBuffer(file)
    // 298.5 is inside row-group bounds but falls between adjacent integer
    // pages, so row-group statistics cannot skip it while page indexes can.
    const rows = await parquetReadObjects({
      file: paged,
      metadata,
      filter: { id: { $eq: 298.5 } },
      usePageIndex: true,
    })

    expect(rows).toEqual([])
    const indexStarts = new Set(metadata.row_groups[0].columns.flatMap(chunk => [
      chunk.column_index_offset,
      chunk.offset_index_offset,
    ].filter(offset => offset !== undefined).map(Number)))
    expect(paged.ranges().length).toBeGreaterThan(0)
    expect(paged.ranges().every(([start]) => indexStarts.has(start))).toBe(true)
  })

  it('discovers page indexes for nested physical filter paths', async () => {
    const file = await asyncBufferFromFile('test/files/struct_offset_index.parquet')
    const metadata = await parquetMetadataAsync(file)
    const path = 'messages.list.element.content'
    const { pageRangesByGroup, pageLocationsByGroup } = await prefetchPageIndexes({
      file,
      metadata,
      filter: { [path]: { $eq: 'x'.repeat(1640) } },
    })

    expect(pageLocationsByGroup[0][path]).toHaveLength(2)
    expect(pageRangesByGroup[0]).toEqual([[19, Number(metadata.row_groups[0].num_rows)]])
  })
})

describe('filterPageRanges', () => {
  // two columns, 4 pages each of 100 rows, groupRows=400
  const columnPages = {
    id: {
      minValues: [0, 100, 200, 300],
      maxValues: [99, 199, 299, 399],
      nullPages: [false, false, false, false],
      nullCounts: [0n, 0n, 0n, 0n],
      pageStarts: [0, 100, 200, 300],
    },
    word: {
      minValues: ['a', 'g', 'n', 't'],
      maxValues: ['f', 'm', 's', 'z'],
      nullPages: [false, false, false, false],
      nullCounts: [0n, 0n, 0n, 0n],
      pageStarts: [0, 100, 200, 300],
    },
  }

  it('narrows $eq to one page', () => {
    expect(filterPageRanges({ id: { $eq: 250 } }, columnPages, 400)).toEqual([[200, 300]])
  })

  it('merges adjacent surviving pages', () => {
    expect(filterPageRanges({ id: { $gt: 150 } }, columnPages, 400)).toEqual([[100, 400]])
  })

  it('$in selects disjoint pages', () => {
    expect(filterPageRanges({ id: { $in: [50, 350] } }, columnPages, 400)).toEqual([[0, 100], [300, 400]])
  })

  it('returns empty ranges when no page can match', () => {
    expect(filterPageRanges({ id: { $eq: 1000 } }, columnPages, 400)).toEqual([])
  })

  it('intersects conditions across fields', () => {
    const filter = { id: { $lt: 250 }, word: { $gt: 'p' } }
    expect(filterPageRanges(filter, columnPages, 400)).toEqual([[200, 300]])
  })

  it('$and intersects and $or unions', () => {
    expect(filterPageRanges({ $and: [{ id: { $gt: 150 } }, { id: { $lt: 250 } }] }, columnPages, 400)).toEqual([[100, 300]])
    expect(filterPageRanges({ $or: [{ id: { $lt: 50 } }, { id: { $gt: 350 } }] }, columnPages, 400)).toEqual([[0, 100], [300, 400]])
  })

  it('unknown columns and $nor return undefined (no pruning)', () => {
    expect(filterPageRanges({ other: { $eq: 1 } }, columnPages, 400)).toBeUndefined()
    expect(filterPageRanges({ $nor: [{ id: { $eq: 1 } }] }, columnPages, 400)).toBeUndefined()
    // $or with an unprunable branch cannot prune
    expect(filterPageRanges({ $or: [{ id: { $eq: 1 } }, { other: { $eq: 1 } }] }, columnPages, 400)).toBeUndefined()
  })

  it('keeps all-null pages conservatively', () => {
    const withNulls = {
      id: {
        minValues: [0, undefined, 200, 300],
        maxValues: [99, undefined, 299, 399],
        nullPages: [false, true, false, false],
        pageStarts: [0, 100, 200, 300],
      },
    }
    expect(filterPageRanges({ id: { $eq: 250 } }, withNulls, 400)).toEqual([[100, 300]])
  })

  it('keeps mixed-null pages when null rows can match', () => {
    /** @type {Record<string, ColumnPageStats>} */
    const withMixedNulls = {
      id: {
        minValues: [5, 5, 5],
        maxValues: [5, 5, 5],
        nullPages: [false, false, false],
        nullCounts: [0n, 1n, undefined],
        pageStarts: [0, 100, 200],
      },
    }
    expect(filterPageRanges({ id: { $ne: 5 } }, withMixedNulls, 300)).toEqual([[100, 300]])
  })

  it('uses unsigned lexicographic ordering for binary bounds', () => {
    const binaryPages = {
      bytes: {
        minValues: [new Uint8Array([2])],
        maxValues: [new Uint8Array([10])],
        nullPages: [false],
        pageStarts: [0],
      },
    }
    expect(filterPageRanges({ bytes: { $eq: new Uint8Array([3]) } }, binaryPages, 100)).toEqual([[0, 100]])
    expect(filterPageRanges({ bytes: { $eq: new Uint8Array([11]) } }, binaryPages, 100)).toEqual([])
  })

  it('uses unsigned UTF-8 ordering for STRING bounds', () => {
    /** @type {Record<string, ColumnPageStats>} */
    const stringPages = {
      word: {
        minValues: ['\uE000'],
        maxValues: ['\u{1F600}'],
        nullPages: [false],
        pageStarts: [0],
        element: { name: 'word', type: 'BYTE_ARRAY', logical_type: { type: 'STRING' } },
      },
    }

    // U+F000 sorts between U+E000 and U+1F600 as UTF-8 bytes, but after the
    // surrogate pair for U+1F600 in JavaScript's UTF-16 string ordering.
    expect(filterPageRanges({ word: { $eq: '\uF000' } }, stringPages, 100)).toEqual([[0, 100]])
    // Relational filters retain JavaScript semantics, so non-ASCII bounds are
    // conservative when UTF-8 and UTF-16 order can disagree.
    expect(filterPageRanges({ word: { $gt: '\u{1F600}' } }, stringPages, 100)).toEqual([[0, 100]])
  })

  it('uses UTF-8 ordering for default-decoded unannotated BYTE_ARRAY bounds', () => {
    /** @type {Record<string, ColumnPageStats>} */
    const stringPages = {
      word: {
        minValues: ['\uE000'],
        maxValues: ['\u{10000}'],
        nullPages: [false],
        pageStarts: [0],
        element: { name: 'word', type: 'BYTE_ARRAY' },
      },
    }

    expect(filterPageRanges({ word: { $eq: '\u{10000}' } }, stringPages, 100)).toEqual([[0, 100]])
  })

  it('keeps exactly the pages whose Date values satisfy equality', () => {
    const first = new Date('2024-01-01T00:00:00Z')
    const second = new Date('2024-01-02T00:00:00Z')
    /** @type {Record<string, ColumnPageStats>} */
    const datePages = {
      created: {
        minValues: [first, second],
        maxValues: [first, second],
        nullPages: [false, false],
        pageStarts: [0, 100],
        element: { name: 'created', type: 'INT32', converted_type: 'DATE' },
      },
    }

    expect(filterPageRanges({ created: { $eq: new Date(first) } }, datePages, 200)).toEqual([[0, 100]])
  })

  it('keeps floating-point pages with uniform bounds for $ne and $nin', () => {
    /** @type {Record<string, ColumnPageStats>} */
    const floatPages = {
      value: {
        minValues: [5, 6],
        maxValues: [5, 6],
        nullPages: [false, false],
        nullCounts: [0n, 0n],
        pageStarts: [0, 100],
        element: { name: 'value', type: 'DOUBLE' },
      },
    }

    // A page with bounds [5, 5] can also contain NaN because Parquet excludes
    // NaNs from floating-point min/max statistics.
    expect(filterPageRanges({ value: { $ne: 5 } }, floatPages, 200)).toEqual([[0, 200]])
    expect(filterPageRanges({ value: { $nin: [5] } }, floatPages, 200)).toEqual([[0, 200]])
  })
})

describe('range set operations', () => {
  it('intersectRanges', () => {
    expect(intersectRanges([[0, 10]], [[5, 15]])).toEqual([[5, 10]])
    expect(intersectRanges([[0, 10], [20, 30]], [[5, 25]])).toEqual([[5, 10], [20, 25]])
    expect(intersectRanges([[0, 10]], [[10, 20]])).toEqual([])
    expect(intersectRanges(undefined, [[1, 2]])).toEqual([[1, 2]])
    expect(intersectRanges([[1, 2]], undefined)).toEqual([[1, 2]])
  })

  it('unionRanges', () => {
    expect(unionRanges([[0, 10]], [[5, 15]])).toEqual([[0, 15]])
    expect(unionRanges([[0, 5]], [[10, 15]])).toEqual([[0, 5], [10, 15]])
    expect(unionRanges([[0, 5]], [[5, 10]])).toEqual([[0, 10]])
    expect(unionRanges([], [[1, 2]])).toEqual([[1, 2]])
  })
})
