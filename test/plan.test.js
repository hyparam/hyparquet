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
})
