import { describe, expect, it } from 'vitest'
import { compressors } from 'hyparquet-compressors'
import { parquetRead, toJson } from '../src/index.js'
import { readColumn } from '../src/column.js'
import { DEFAULT_PARSERS } from '../src/convert.js'
import { parquetMetadata } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { getMaxRepetitionLevel, getSchemaPath, isFlatColumn } from '../src/schema.js'
import { fileToJson } from './helpers.js'

/**
 * @param {import('../src/types.js').ColumnMetaData} meta
 * @returns {{ startByte: number, endByte: number }}
 */
function getChunkPlan(meta) {
  const columnOffset = meta.dictionary_page_offset || meta.data_page_offset
  return {
    startByte: Number(columnOffset),
    endByte: Number(columnOffset + meta.total_compressed_size),
  }
}

describe('nested column early termination', () => {
  // continued_page.parquet: 100 rows, LIST column (int_list), 2 data pages, 2000 values
  // This is a nested column with max_repetition_level > 0

  it('readColumn terminates early for nested LIST column', async () => {
    const testFile = 'test/files/continued_page.parquet'
    const file = await asyncBufferFromFile(testFile)
    const arrayBuffer = await file.slice(0)
    const metadata = parquetMetadata(arrayBuffer)
    const column = metadata.row_groups[0].columns[0]
    const meta = column.meta_data
    if (!meta) throw new Error('No column metadata')

    const schemaPath = getSchemaPath(metadata.schema, meta.path_in_schema)
    expect(isFlatColumn(schemaPath)).toBe(false)
    expect(getMaxRepetitionLevel(schemaPath)).toBeGreaterThan(0)

    const { startByte, endByte } = getChunkPlan(meta)
    const columnDecoder = {
      pathInSchema: meta.path_in_schema,
      type: meta.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      parsers: DEFAULT_PARSERS,
      codec: meta.codec,
      compressors,
    }

    // Read only the first 10 rows — should terminate early without reading all pages
    const buf = arrayBuffer.slice(startByte, endByte)
    const reader10 = { view: new DataView(buf), offset: 0 }
    const result10 = readColumn(reader10, {
      groupStart: 0, selectStart: 0, selectEnd: 10, groupRows: 100,
    }, columnDecoder)
    // Should have produced data, and the reader should NOT have consumed the entire buffer
    expect(result10.data.length).toBeGreaterThan(0)
    const totalValues10 = result10.data.reduce((sum, chunk) => sum + chunk.length, 0)
    expect(totalValues10).toBeGreaterThanOrEqual(10)
    // Verify early termination: reader should have stopped before the end
    expect(reader10.offset).toBeLessThan(buf.byteLength - 1)

    // Read all 100 rows for comparison
    const readerAll = { view: new DataView(buf), offset: 0 }
    const resultAll = readColumn(readerAll, {
      groupStart: 0, selectStart: 0, selectEnd: 100, groupRows: 100,
    }, columnDecoder)
    const totalValuesAll = resultAll.data.reduce((sum, chunk) => sum + chunk.length, 0)
    expect(totalValuesAll).toBe(100)
    // Verify they consumed the entire buffer
    expect(readerAll.offset).toBeGreaterThanOrEqual(buf.byteLength - 1)
  })

  it('reads first 10 rows from nested LIST column correctly', async () => {
    const file = await asyncBufferFromFile('test/files/continued_page.parquet')
    const allData = fileToJson('test/files/continued_page.json')

    /** @type {any[][]} */
    const rows = []
    await parquetRead({
      file,
      compressors,
      rowEnd: 10,
      onComplete(result) { rows.push(...result) },
    })
    expect(toJson(rows)).toEqual(allData.slice(0, 10))
  })

  it('reads last 10 rows from nested LIST column correctly', async () => {
    const file = await asyncBufferFromFile('test/files/continued_page.parquet')
    const allData = fileToJson('test/files/continued_page.json')

    /** @type {any[][]} */
    const rows = []
    await parquetRead({
      file,
      compressors,
      rowStart: 90,
      rowEnd: 100,
      onComplete(result) { rows.push(...result) },
    })
    expect(toJson(rows)).toEqual(allData.slice(90, 100))
  })

  it('reads middle rows from nested LIST column correctly', async () => {
    const file = await asyncBufferFromFile('test/files/continued_page.parquet')
    const allData = fileToJson('test/files/continued_page.json')

    /** @type {any[][]} */
    const rows = []
    await parquetRead({
      file,
      compressors,
      rowStart: 40,
      rowEnd: 60,
      onComplete(result) { rows.push(...result) },
    })
    expect(toJson(rows)).toEqual(allData.slice(40, 60))
  })

  it('reads single row from nested LIST column correctly', async () => {
    const file = await asyncBufferFromFile('test/files/continued_page.parquet')
    const allData = fileToJson('test/files/continued_page.json')

    // Read row 50 (middle of the data, potentially at a page boundary)
    /** @type {any[][]} */
    const rows = []
    await parquetRead({
      file,
      compressors,
      rowStart: 50,
      rowEnd: 51,
      onComplete(result) { rows.push(...result) },
    })
    expect(toJson(rows)).toEqual(allData.slice(50, 51))
  })

  it('page-skip works for nested column via repetition levels', async () => {
    const testFile = 'test/files/continued_page.parquet'
    const file = await asyncBufferFromFile(testFile)
    const arrayBuffer = await file.slice(0)
    const metadata = parquetMetadata(arrayBuffer)
    const column = metadata.row_groups[0].columns[0]
    const meta = column.meta_data
    if (!meta) throw new Error('No column metadata')

    const schemaPath = getSchemaPath(metadata.schema, meta.path_in_schema)
    const { startByte, endByte } = getChunkPlan(meta)
    const columnDecoder = {
      pathInSchema: meta.path_in_schema,
      type: meta.type,
      element: schemaPath[schemaPath.length - 1].element,
      schemaPath,
      parsers: DEFAULT_PARSERS,
      codec: meta.codec,
      compressors,
    }

    // Read only the last row — the first page should be skipped via rep level counting
    const buf = arrayBuffer.slice(startByte, endByte)
    const reader = { view: new DataView(buf), offset: 0 }
    const result = readColumn(reader, {
      groupStart: 0, selectStart: 99, selectEnd: 100, groupRows: 100,
    }, columnDecoder)

    // Should have skipped some rows
    expect(result.skipped).toBeGreaterThan(0)
    // Should have produced data
    expect(result.data.length).toBeGreaterThan(0)
  })
})
