import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync, parquetSchema } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'

describe('parquetSchema', () => {
  it('parse schema tree from rowgroups.parquet', async () => {
    const arrayBuffer = await asyncBufferFromFile('test/files/rowgroups.parquet')
    const metadata = await parquetMetadataAsync(arrayBuffer)
    const schemaTree = parquetSchema(metadata)
    expect(schemaTree).toEqual(rowgroupsSchema)
  })
})

// Parquet v2 from pandas with 2 row groups
const rowgroupsSchema = {
  children: [
    {
      children: [],
      count: 1,
      element: {
        name: 'numbers',
        repetition_type: 'OPTIONAL',
        type: 'INT64',
      },
      path: ['numbers'],
    },
  ],
  count: 2,
  element: {
    name: 'schema',
    num_children: 1,
    repetition_type: 'REQUIRED',
  },
  path: [],
}
