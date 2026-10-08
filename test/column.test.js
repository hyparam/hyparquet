import { describe, expect, it } from 'vitest'
import { readColumn, readPage } from '../src/column.js'
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
    const metadata = parquetMetadata(arrayBuffer)

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
    expect(result.data).toEqual(expected)
  })

  it('readColumn should return a typed array', async () => {
    const testFile = 'test/files/datapage_v2.snappy.parquet'
    const file = await asyncBufferFromFile(testFile)
    const arrayBuffer = await file.slice(0)
    const metadata = parquetMetadata(arrayBuffer)

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

    const { data } = readColumn(reader, rowGroupSelect, columnDecoder)
    expect(data[0]).toBeInstanceOf(Int32Array)
  })
})

describe('readPage', () => {
  it('skips a v2 page of a nested column by its row count', () => {
    /** @type {SchemaElement[]} */
    const schema = [
      { name: 'root', num_children: 1 },
      { name: 'list', repetition_type: 'OPTIONAL', converted_type: 'LIST', num_children: 1 },
      { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
      { name: 'element', repetition_type: 'OPTIONAL', type: 'INT32' },
    ]
    const schemaPath = getSchemaPath(schema, ['list', 'list', 'element'])
    /** @type {ColumnDecoder} */
    const columnDecoder = {
      pathInSchema: ['list', 'list', 'element'],
      type: 'INT32',
      element: schema[3],
      schemaPath,
      parsers: DEFAULT_PARSERS,
      codec: 'UNCOMPRESSED',
    }
    /** @type {PageHeader} */
    const header = {
      type: 'DATA_PAGE_V2',
      uncompressed_page_size: 8,
      compressed_page_size: 8,
      data_page_header_v2: {
        num_values: 10,
        num_nulls: 0,
        num_rows: 5,
        encoding: 'PLAIN',
        definition_levels_byte_length: 0,
        repetition_levels_byte_length: 0,
      },
    }
    const reader = { view: new DataView(new ArrayBuffer(8)), offset: 0 }
    const result = readPage(reader, header, columnDecoder, undefined, undefined, 6)
    expect(result).toEqual({ skipped: 5 })
    expect(reader.offset).toBe(8)
  })
})

/**
 * @import {ByteRange, ColumnDecoder, ColumnMetaData, PageHeader, SchemaElement} from '../src/types.js'
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
