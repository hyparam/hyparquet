import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetMetadata } from '../src/hyparquet.js'
import { readColumnIndex, readOffsetIndex } from '../src/indicies.js'
import { getSchemaPath } from '../src/schema.js'
import { toJson } from '../src/utils.js'
import { fileToJson, readFileToArrayBuffer } from './helpers.js'

describe('readColumnIndex', () => {
  const columnIndicesFiles = fs.readdirSync('test/files').filter(f => f.endsWith('.column_indices.json'))
  const parquetFiles = columnIndicesFiles.map(f => f.replace(/.column_indices.json$/i, '.parquet'))

  parquetFiles.forEach((file, i) => {
    it(`parse column indices from ${file}`, async () => {
      const arrayBuffer = await readFileToArrayBuffer(`test/files/${file}`)
      const metadata = parquetMetadata(arrayBuffer)

      const result = metadata.row_groups.map((rowGroup) => rowGroup.columns.map((column) => {
        if (column.column_index_offset === undefined || column.column_index_length === undefined) return null
        const columnIndexOffset = Number(column.column_index_offset)
        const columnIndexLength = Number(column.column_index_length)
        const columnIndexArrayBuffer = arrayBuffer.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
        const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
        const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
        return readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })
      }))
      const expected = fileToJson(`test/files/${columnIndicesFiles[i]}`)
      expect(toJson(result)).toEqual(expected)
    })
  })
})

describe('readOffsetIndex', () => {
  const offsetIndicesFiles = fs.readdirSync('test/files').filter(f => f.endsWith('.offset_indices.json'))
  const parquetFiles = offsetIndicesFiles.map(f => f.replace(/.offset_indices.json$/i, '.parquet'))

  parquetFiles.forEach((file, i) => {
    it(`parse offset indices from ${file}`, async () => {
      const arrayBuffer = await readFileToArrayBuffer(`test/files/${file}`)
      const metadata = parquetMetadata(arrayBuffer)

      const result = metadata.row_groups.map((rowGroup) => rowGroup.columns.map((column) => {
        if (column.offset_index_offset === undefined || column.offset_index_length === undefined) return null
        const offsetIndexOffset = Number(column.offset_index_offset)
        const offsetIndexLength = Number(column.offset_index_length)
        const offsetIndexArrayBuffer = arrayBuffer.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
        const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
        return readOffsetIndex(offsetIndexReader)
      }))
      const expected = fileToJson(`test/files/${offsetIndicesFiles[i]}`)
      expect(toJson(result)).toEqual(expected)
    })
  })
})
