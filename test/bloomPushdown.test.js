import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync, parquetReadObjects } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { parquetSchema } from '../src/metadata.js'
import { parquetPlan, prefetchBloomFilters } from '../src/plan.js'

/**
 * @import {SchemaElement} from '../src/types.js'
 */

/**
 * End-to-end bloom-filter pushdown against a tiny real parquet file.
 *
 * Layout of test/files/bloom_filter.parquet (4096 rows in 2 row groups of 2048):
 *   row group 0: code alternates between 10 and 90 → stats min=10, max=90, bloom={10,90}
 *   row group 1: code alternates between 30 and 70 → stats min=30, max=70, bloom={30,70}
 *
 * The stats ranges overlap, so equality predicates inside the overlap region
 * cannot be pruned by stats alone. The bloom filter is what lets us skip.
 */
describe('bloom filter pushdown against test/files/bloom_filter.parquet', () => {
  const path = 'test/files/bloom_filter.parquet'

  it('file has 2 row groups, each with a bloom filter on `code`', async () => {
    const file = await asyncBufferFromFile(path)
    const meta = await parquetMetadataAsync(file)
    expect(meta.row_groups).toHaveLength(2)
    for (const rg of meta.row_groups) {
      const { meta_data } = rg.columns[0]
      expect(meta_data?.path_in_schema).toEqual(['code'])
      expect(meta_data?.bloom_filter_offset).toBeDefined()
      expect(meta_data?.bloom_filter_length).toBeGreaterThan(0)
    }
  })

  it('bloom filter skips a row group that stats cannot prune ($eq:30 is in [10,90] but absent from RG0)', async () => {
    const file = await asyncBufferFromFile(path)
    const metadata = await parquetMetadataAsync(file)
    const filter = { code: { $eq: 30 } }

    // Stats-only plan: both row groups survive (30 sits inside [10,90] and [30,70]).
    const statsPlan = parquetPlan({ file, metadata, filter })
    expect(statsPlan.groups.map(g => g.groupStart)).toEqual([0, 2048])

    // With bloom: RG 0's bloom proves 30 absent → only RG 1 remains.
    const bloomFiltersByGroup = await prefetchBloomFilters({ file, metadata, filter })
    const schemaTree = parquetSchema(metadata)
    /** @type {Record<string, SchemaElement>} */
    const schemaElements = {}
    for (const child of schemaTree.children) schemaElements[child.element.name] = child.element
    const bloomPlan = parquetPlan({ file, metadata, filter, bloomFiltersByGroup, schemaElements })
    expect(bloomPlan.groups.map(g => g.groupStart)).toEqual([2048])
  })

  it('absent value: both row groups skip via bloom and the read returns no rows', async () => {
    const file = await asyncBufferFromFile(path)
    const rows = await parquetReadObjects({ file, filter: { code: { $eq: 50 } } })
    expect(rows).toEqual([])
  })

  it('present-in-one-group value: $eq:30 returns the 1024 rows from RG 1', async () => {
    const file = await asyncBufferFromFile(path)
    const rows = await parquetReadObjects({ file, filter: { code: { $eq: 30 } } })
    expect(rows).toHaveLength(1024)
    expect(rows.every(r => r.code === 30)).toBe(true)
  })

  it('useBloomFilters:false matches the stats-only plan and still returns the right rows', async () => {
    const file = await asyncBufferFromFile(path)
    // 50 sits inside both stats ranges, so stats can't prune. Without bloom, both groups
    // are scanned; the filter then matches zero rows.
    const rows = await parquetReadObjects({ file, filter: { code: { $eq: 50 } }, useBloomFilters: false })
    expect(rows).toEqual([])
  })

  it('$in with one value absent from both bloom filters still reads the row group(s) that may contain the present value', async () => {
    const file = await asyncBufferFromFile(path)
    // 30 is in RG 1 only; 50 is in neither. RG 0 can be pruned (bloom rejects both); RG 1 cannot.
    const rows = await parquetReadObjects({ file, filter: { code: { $in: [30, 50] } } })
    expect(rows).toHaveLength(1024)
    expect(rows.every(r => r.code === 30)).toBe(true)
  })
})
