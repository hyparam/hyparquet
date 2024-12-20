import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { parquetMetadata } from '../src/hyparquet.js'
import { getSchemaPath } from '../src/schema.js'
import { getColumnRange, readColumn } from '../src/column.js'
import { asyncBufferFromFile } from '../src/utils.js'

describe('readColumn', () => {
  it('read columns when rowLimit is undefined', async () => {
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
    const result = readColumn(reader, undefined, column.meta_data, schemaPath, { file: asyncBuffer, compressors })
    const expected = [null, 1, -2, NaN, 0, -1, -0, 2]
    expect(result).toEqual(expected)
  })
})
