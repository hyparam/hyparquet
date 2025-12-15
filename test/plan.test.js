import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { parquetPlan } from '../src/plan.js'

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
          ranges: [
            { startByte: 4, endByte: 438 },
            { startByte: 438, endByte: 14772 },
          ],
        },
        {
          groupRows: 100,
          groupStart: 100,
          ranges: [
            { startByte: 14772, endByte: 15208 },
            { startByte: 15208, endByte: 29507 },
          ],
        },
      ],
    })
  })
})
