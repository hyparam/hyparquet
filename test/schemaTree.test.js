import { describe, expect, it } from 'vitest'
import { parquetMetadata, parquetSchema } from '../src/hyparquet.js'
import { readFileToArrayBuffer } from './helpers.js'

describe('schemaTree', () => {
  it('should parse schema tree from addrtype-missing-value.parquet', async () => {
    const arrayBuffer = await readFileToArrayBuffer('test/files/addrtype-missing-value.parquet')
    const metadata = parquetMetadata(arrayBuffer)
    const result = parquetSchema(metadata)
    expect(result).toEqual(addrtypeSchema)
  })

  it('should parse schema tree from rowgroups.parquet', async () => {
    const arrayBuffer = await readFileToArrayBuffer('test/files/rowgroups.parquet')
    const metadata = parquetMetadata(arrayBuffer)
    const result = parquetSchema(metadata)
    expect(result).toEqual(rowgroupsSchema)
  })
})

// Parquet v1 from DuckDB
const addrtypeSchema = {
  children: [
    {
      children: [],
      count: 1,
      element: {
        converted_type: 'UTF8',
        name: 'ADDRTYPE',
        repetition_type: 'OPTIONAL',
        type: 6,
      },
    },
  ],
  count: 2,
  element: {
    name: 'duckdb_schema',
    num_children: 1,
    repetition_type: 'REQUIRED',
  },
}

// Parquet v2 from pandas with 2 row groups
const rowgroupsSchema = {
  children: [
    {
      children: [],
      count: 1,
      element: {
        name: 'numbers',
        repetition_type: 'OPTIONAL',
        type: 2,
      },
    },
  ],
  count: 2,
  element: {
    name: 'schema',
    num_children: 1,
    repetition_type: 'REQUIRED',
  },
}
