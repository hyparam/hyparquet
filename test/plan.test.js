import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync } from '../src/hyparquet.js'
import { asyncBufferFromFile } from '../src/utils.js'
import { parquetPlan } from '../src/plan.js'

describe('parquetPlan', () => {
  it('generates a query plan', async () => {
    const file = await asyncBufferFromFile('test/files/page_indexed.parquet')
    const metadata = await parquetMetadataAsync(file)
    const plan = parquetPlan({ file, metadata })
    expect(plan).toEqual({
      ranges: [
        { startByte: 4, endByte: 1166 },
        { startByte: 1166, endByte: 2326 },
      ],
      groups: [
        {
          plan: [
            { startByte: 4, endByte: 832 },
            { startByte: 832, endByte: 1166 },
          ],
        },
        {
          plan: [
            { startByte: 1166, endByte: 1998 },
            { startByte: 1998, endByte: 2326 },
          ],
        },
      ],
    })
  })
})
