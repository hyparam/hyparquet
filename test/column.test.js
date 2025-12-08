import { describe, expect, it } from 'vitest'
import { readColumn } from '../src/column.js'
import { DEFAULT_PARSERS } from '../src/convert.js'
import { parquetMetadata } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { getSchemaPath } from '../src/schema.js'

const values = [null, 1, -2, NaN, 0, -1, -0, 2]

describe('readColumn', () => {
  it.for([
    { selectEnd: Infinity, expected: [values] },
    { selectEnd: 2, expected: [values] }, // readColumn does not truncate
    { selectEnd: 0, expected: [] },
  ])('readColumn with rowGroupEnd %p', async ({ selectEnd, expected }) => {
    const testFile = 'test/files/float16_nonzeros_and_nans.parquet'
    const file = await asyncBufferFromFile(testFile)
    const arrayBuffer = await file.slice(0)
    const metadata = await parquetMetadata(arrayBuffer)

    const column = metadata.row_groups[0].columns[0]
    if (!column.meta_data) throw new Error(`No column metadata for ${testFile}`)
    const { startByte, endByte } = getChunkPlan(column.meta_data)
    const columnArrayBuffer = arrayBuffer.slice(startByte, endByte)
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
    const reader = { view: new DataView(columnArrayBuffer), offset: 0 }
    const columnDecoder = {
      pathInSchema: column.meta_data.path_in_schema,
      type: column.meta_data.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      parsers: DEFAULT_PARSERS,
      codec: column.meta_data.codec,
    }
    const rowGroupSelect = {
      groupStart: 0,
      selectStart: 0,
      selectEnd,
      groupRows: expected.length,
    }

    const result = readColumn(reader, rowGroupSelect, columnDecoder)
    expect(result).toEqual(expected)
  })

  it('readColumn should return a typed array', async () => {
    const testFile = 'test/files/datapage_v2.snappy.parquet'
    const file = await asyncBufferFromFile(testFile)
    const arrayBuffer = await file.slice(0)
    const metadata = await parquetMetadata(arrayBuffer)

    const column = metadata.row_groups[0].columns[1] // second column
    if (!column.meta_data) throw new Error(`No column metadata for ${testFile}`)
    const { startByte, endByte } = getChunkPlan(column.meta_data)
    const columnArrayBuffer = arrayBuffer.slice(startByte, endByte)
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
    const reader = { view: new DataView(columnArrayBuffer), offset: 0 }
    const columnDecoder = {
      pathInSchema: column.meta_data.path_in_schema,
      type: column.meta_data.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      parsers: DEFAULT_PARSERS,
      codec: column.meta_data.codec,
    }
    const rowGroupSelect = {
      groupStart: 0,
      selectStart: 0,
      selectEnd: Infinity,
      groupRows: Number(column.meta_data.num_values),
    }

    const columnData = readColumn(reader, rowGroupSelect, columnDecoder)
    expect(columnData[0]).toBeInstanceOf(Int32Array)
  })
})

/**
 * @import {ByteRange, ColumnMetaData} from '../src/types.js'
 * @param {ColumnMetaData} meta
 * @returns {ByteRange}
 */
function getChunkPlan(meta) {
  const columnOffset = meta.dictionary_page_offset || meta.data_page_offset
  return {
    startByte: Number(columnOffset),
    endByte: Number(columnOffset + meta.total_compressed_size),
  }
}
