import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { parquetPlan, prefetchPageIndexes } from '../src/plan.js'

/**
 * @import {PageLocation, PageRanges} from '../src/types.js'
 */

describe('parquetPlan', () => {
  it('generates a query plan', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const plan = parquetPlan({ file, metadata })
    expect(plan).toMatchObject({
      metadata,
      rowStart: 0,
      rowEnd: 200,
      fetches: [
        { startByte: 4, endByte: 14772 },
        { startByte: 14772, endByte: 29507 },
      ],
      groups: [
        {
          groupRows: 100,
          groupStart: 0,
          chunks: [
            { range: { startByte: 4, endByte: 438 } },
            { range: { startByte: 438, endByte: 14772 } },
          ],
        },
        {
          groupRows: 100,
          groupStart: 100,
          chunks: [
            { range: { startByte: 14772, endByte: 15208 } },
            { range: { startByte: 15208, endByte: 29507 } },
          ],
        },
      ],
    })
  })

  it('skips offset index when reading entire row group', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const plan = parquetPlan({ file, metadata, useOffsetIndex: true })
    // reading all rows, so offset index should not be used
    for (const group of plan.groups) {
      for (const chunk of group.chunks) {
        expect(chunk).toHaveProperty('range')
        expect(chunk).not.toHaveProperty('offsetIndex')
      }
    }
  })

  it('uses offset index when reading a row subset', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const plan = parquetPlan({ file, metadata, useOffsetIndex: true, rowStart: 50, rowEnd: 150 })
    // partial read should use offset index
    const hasOffsetIndex = plan.groups.some(g =>
      g.chunks.some(c => 'offsetIndex' in c)
    )
    expect(hasOffsetIndex).toBe(true)
  })

  it('does not fetch page indexes for top-level $nor filters', async () => {
    const source = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(source)
    const contentChunk = metadata.row_groups[0].columns[1]
    contentChunk.column_index_offset = 1n
    contentChunk.column_index_length = 1
    let slices = 0
    const file = {
      byteLength: source.byteLength,
      slice() {
        slices++
        throw new Error('unexpected page index fetch')
      },
    }

    const indexes = await prefetchPageIndexes({
      file,
      metadata,
      filter: { $nor: [{ content: { $eq: 'x' } }] },
    })

    expect(slices).toBe(0)
    expect(indexes.pageRangesByGroup).toEqual([undefined, undefined])
    expect(indexes.pageLocationsByGroup).toEqual([{}, {}])
  })

  it('coalesces candidate ranges that select the same coarse output page', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    /** @type {(PageRanges | undefined)[]} */
    const pageRangesByGroup = [
      [[0, 10], [20, 30]],
      undefined,
    ]
    /** @type {Record<string, PageLocation[]>[]} */
    const pageLocationsByGroup = [
      {
        content: [{
          offset: 438n,
          compressed_page_size: 14334,
          first_row_index: 0n,
        }],
      },
      {},
    ]

    const plan = parquetPlan({
      file,
      metadata,
      rowEnd: 100,
      columns: ['content'],
      filter: { id: { $in: [0, 20] } },
      pageRangesByGroup,
      pageLocationsByGroup,
    })

    expect(plan.groups).toHaveLength(1)
    expect(plan.groups[0]).toMatchObject({ selectStart: 0, selectEnd: 30 })
  })

  it('reuses chunk plans across disjoint candidate ranges', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const plan = parquetPlan({
      file,
      metadata,
      rowEnd: 100,
      columns: ['content'],
      filter: { id: { $in: [0, 60] } },
      pageRangesByGroup: [[[0, 10], [60, 70]], undefined],
      pageLocationsByGroup: [{
        content: [
          { offset: 438n, compressed_page_size: 100, first_row_index: 0n },
          { offset: 538n, compressed_page_size: 100, first_row_index: 50n },
        ],
      }, {}],
    })

    expect(plan.groups).toHaveLength(2)
    expect(plan.groups[0].chunks).toBe(plan.groups[1].chunks)
  })

  it.for([
    { gap: 8192, fetches: [{ startByte: 4, endByte: 22964 }] },
    { gap: 8193, fetches: [{ startByte: 4, endByte: 438 }, { startByte: 8631, endByte: 22965 }] },
  ])('combines selected column chunks across a $gap byte gap', async ({ gap, fetches }) => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const plan = parquetPlan({
      file,
      metadata: moveColumnChunk(metadata, 1, 438 + gap),
      columns: ['id', 'content'],
      rowEnd: 100,
    })
    expect(plan.fetches).toEqual(fetches)
  })

  it('combines selected column chunks in file order', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    // store content before id in the file, keeping schema order
    let reordered = moveColumnChunk(metadata, 1, 4)
    reordered = moveColumnChunk(reordered, 0, 14338)
    const plan = parquetPlan({ file, metadata: reordered, columns: ['id', 'content'], rowEnd: 100 })
    expect(plan.fetches).toEqual([{ startByte: 4, endByte: 14772 }])
  })

  it('does not combine selected column chunks across row groups', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const plan = parquetPlan({ file, metadata, columns: ['id', 'content'], rowEnd: 200 })
    // the row groups touch at byte 14772
    expect(plan.fetches).toEqual([
      { startByte: 4, endByte: 14772 },
      { startByte: 14772, endByte: 29507 },
    ])
  })

  it.for([
    { columns: ['id', 'payload'], fetches: [{ startByte: 4, endByte: 23486 }] },
    { columns: ['id', 'category'], fetches: [{ startByte: 4, endByte: 6177 }, { startByte: 23486, endByte: 24361 }] },
  ])('skips unselected columns only when they exceed the gap limit: $columns', async ({ columns, fetches }) => {
    const file = await asyncBufferFromFile('test/files/page_index.parquet')
    const metadata = await parquetMetadataAsync(file)
    // word (8167 bytes) sits between id and payload; word and payload (17309 bytes) sit between id and category
    const plan = parquetPlan({ file, metadata, columns, rowEnd: 1 })
    expect(plan.fetches).toEqual(fetches)
  })

  it.for([
    // the merged range starts at byte 4, so it reaches exactly 2mb at this size
    { size: (1 << 21) - 434, fetches: [{ startByte: 4, endByte: (1 << 21) + 4 }] },
    { size: (1 << 21) - 433, fetches: [{ startByte: 4, endByte: 438 }, { startByte: 438, endByte: (1 << 21) + 5 }] },
  ])('combines selected column chunks up to 2mb: $size byte chunk', async ({ size, fetches }) => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const resized = resizeColumnChunk(metadata, 1, size)
    const plan = parquetPlan({ file, metadata: resized, columns: ['id', 'content'], rowEnd: 100 })
    expect(plan.fetches).toEqual(fetches)
  })

  it('fetches a selected column chunk larger than 2mb in one request', async () => {
    const file = await asyncBufferFromFile('test/files/offset_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const resized = resizeColumnChunk(metadata, 1, 3 << 20)
    const plan = parquetPlan({ file, metadata: resized, columns: ['content'], rowEnd: 100 })
    expect(plan.fetches).toEqual([{ startByte: 438, endByte: 438 + (3 << 20) }])
  })
})

/**
 * Copy metadata with one column chunk of the first row group given a new compressed size.
 *
 * @param {FileMetaData} metadata
 * @param {number} columnIndex
 * @param {number} size
 * @returns {FileMetaData}
 */
function resizeColumnChunk(metadata, columnIndex, size) {
  const copy = structuredClone(metadata)
  const meta = copy.row_groups[0].columns[columnIndex].meta_data
  if (!meta) throw new Error('expected column metadata')
  meta.total_compressed_size = BigInt(size)
  return copy
}

/**
 * Copy metadata with one column chunk of the first row group moved to a new byte offset.
 *
 * @import {FileMetaData} from '../src/types.js'
 * @param {FileMetaData} metadata
 * @param {number} columnIndex
 * @param {number} startByte
 * @returns {FileMetaData}
 */
function moveColumnChunk(metadata, columnIndex, startByte) {
  const copy = structuredClone(metadata)
  const meta = copy.row_groups[0].columns[columnIndex].meta_data
  if (!meta) throw new Error('expected column metadata')
  const shift = BigInt(startByte) - (meta.dictionary_page_offset || meta.data_page_offset)
  meta.data_page_offset += shift
  if (meta.dictionary_page_offset) meta.dictionary_page_offset += shift
  return copy
}
