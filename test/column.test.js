import { describe, expect, it } from 'vitest'
import { getColumnRange, readColumn } from '../src/column.js'
import { parquetMetadata } from '../src/hyparquet.js'
import { getSchemaPath } from '../src/schema.js'
import { asyncBufferFromFile } from '../src/utils.js'

const values = [null, 1, -2, NaN, 0, -1, -0, 2]

/**
 * @import {RowGroupSelect} from '../src/types.d.ts'
 */
describe('readColumn', () => {
  it.for([
    { selectEnd: Infinity, expected: [values] },
    { selectEnd: 2, expected: [values.slice(0, 2)] },
    { selectEnd: 0, expected: [] },
  ])('readColumn with rowGroupEnd %p', async ({ selectEnd, expected }) => {
    const testFile = 'test/files/float16_nonzeros_and_nans.parquet'
    const file = await asyncBufferFromFile(testFile)
    const arrayBuffer = await file.slice(0)
    const metadata = parquetMetadata(arrayBuffer)

    const column = metadata.row_groups[0].columns[0]
    if (!column.meta_data) throw new Error(`No column metadata for ${testFile}`)
    const [columnStartByte, columnEndByte] = getColumnRange(column.meta_data).map(Number)
    const columnArrayBuffer = arrayBuffer.slice(columnStartByte, columnEndByte)
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
    const reader = { view: new DataView(columnArrayBuffer), offset: 0 }
    const columnDecoder = {
      columnName: column.meta_data.path_in_schema.join('.'),
      type: column.meta_data.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      codec: column.meta_data.codec,
    }
    const rowGroupSelect = {
      groupStart: 0,
      selectStart: 0,
      selectEnd,
      numRows: expected.length,
    }

    const result = readColumn(reader, rowGroupSelect, columnDecoder)
    expect(result).toEqual(expected)
  })

  it('readColumn should return a typed array', async () => {
    const testFile = 'test/files/datapage_v2.snappy.parquet'
    const file = await asyncBufferFromFile(testFile)
    const arrayBuffer = await file.slice(0)
    const metadata = parquetMetadata(arrayBuffer)

    const column = metadata.row_groups[0].columns[1] // second column
    if (!column.meta_data) throw new Error(`No column metadata for ${testFile}`)
    const [columnStartByte, columnEndByte] = getColumnRange(column.meta_data).map(Number)
    const columnArrayBuffer = arrayBuffer.slice(columnStartByte, columnEndByte)
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
    const reader = { view: new DataView(columnArrayBuffer), offset: 0 }
    const columnDecoder = {
      columnName: column.meta_data.path_in_schema.join('.'),
      type: column.meta_data.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      codec: column.meta_data.codec,
    }
    const rowGroupSelect = {
      groupStart: 0,
      selectStart: 0,
      selectEnd: Infinity,
      numRows: Number(column.meta_data.num_values),
    }

    const columnData = readColumn(reader, rowGroupSelect, columnDecoder)
    expect(columnData[0]).toBeInstanceOf(Int32Array)
  })
})
