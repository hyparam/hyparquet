import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { getColumnRange, readColumn } from '../src/column.js'
import { parquetMetadata } from '../src/hyparquet.js'
import { getSchemaPath } from '../src/schema.js'
import { asyncBufferFromFile } from '../src/utils.js'

const values = [null, 1, -2, NaN, 0, -1, -0, 2]

describe('readColumn', () => {
  it.for([
    { rowLimit: undefined, expected: [values] },
    { rowLimit: Infinity, expected: [values] },
    { rowLimit: 2, expected: [values.slice(0, 2)] },
    { rowLimit: 0, expected: [] },
  ])('readColumn with rowLimit %p', async ({ rowLimit, expected }) => {
    const testFile = 'test/files/float16_nonzeros_and_nans.parquet'
    const asyncBuffer = await asyncBufferFromFile(testFile)
    const arrayBuffer = await asyncBuffer.slice(0)
    const metadata = parquetMetadata(arrayBuffer)

    const column = metadata.row_groups[0].columns[0]
    if (!column.meta_data) throw new Error(`No column metadata for ${testFile}`)
    const [columnStartByte, columnEndByte] = getColumnRange(column.meta_data).map(Number)
    const columnArrayBuffer = arrayBuffer.slice(columnStartByte, columnEndByte)
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
    const reader = { view: new DataView(columnArrayBuffer), offset: 0 }

    const result = readColumn(reader, rowLimit, column.meta_data, schemaPath, { file: asyncBuffer, compressors })
    expect(result).toEqual(expected)
  })

  it('readColumn should return a typed array', async () => {
    const testFile = 'test/files/datapage_v2.snappy.parquet'
    const asyncBuffer = await asyncBufferFromFile(testFile)
    const arrayBuffer = await asyncBuffer.slice(0)
    const metadata = parquetMetadata(arrayBuffer)

    const column = metadata.row_groups[0].columns[1] // second column
    if (!column.meta_data) throw new Error(`No column metadata for ${testFile}`)
    const [columnStartByte, columnEndByte] = getColumnRange(column.meta_data).map(Number)
    const columnArrayBuffer = arrayBuffer.slice(columnStartByte, columnEndByte)
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
    const reader = { view: new DataView(columnArrayBuffer), offset: 0 }

    const columnData = readColumn(reader, Infinity, column.meta_data, schemaPath, { file: asyncBuffer, compressors })
    expect(columnData[0]).toBeInstanceOf(Int32Array)
  })
})
