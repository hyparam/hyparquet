import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetMetadataAsync, toJson } from '../src/index.js'
import { readColumnIndex, readOffsetIndex } from '../src/indexes.js'
import { asyncBufferFromFile } from '../src/node.js'
import { getSchemaPath } from '../src/schema.js'
import { fileToJson } from './helpers.js'

describe('readColumnIndex', () => {
  const columnIndexesFiles = fs.readdirSync('test/files').filter(f => f.endsWith('.column_indexes.json'))
  const parquetFiles = columnIndexesFiles.map(f => f.replace(/.column_indexes.json$/i, '.parquet'))

  parquetFiles.forEach((file, i) => {
    it(`parse column indexes from ${file}`, async () => {
      const arrayBuffer = await readFileToArrayBuffer(`test/files/${file}`)
      const metadata = await parquetMetadataAsync(arrayBuffer)

      const result = metadata.row_groups.map((rowGroup) => rowGroup.columns.map((column) => {
        if (column.column_index_offset === undefined || column.column_index_length === undefined) return null
        const columnIndexOffset = Number(column.column_index_offset)
        const columnIndexLength = Number(column.column_index_length)
        const columnIndexArrayBuffer = arrayBuffer.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
        const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
        const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
        return readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })
      }))
      const expected = fileToJson(`test/files/${columnIndexesFiles[i]}`)
      expect(toJson(result)).toEqual(expected)
    })
  })
})

describe('readOffsetIndex', () => {
  const offsetIndexesFiles = fs.readdirSync('test/files').filter(f => f.endsWith('.offset_indexes.json'))
  const parquetFiles = offsetIndexesFiles.map(f => f.replace(/.offset_indexes.json$/i, '.parquet'))

  parquetFiles.forEach((file, i) => {
    it(`parse offset indexes from ${file}`, async () => {
      const arrayBuffer = await readFileToArrayBuffer(`test/files/${file}`)
      const metadata = await parquetMetadataAsync(arrayBuffer)

      const result = metadata.row_groups.map((rowGroup) => rowGroup.columns.map((column) => {
        if (column.offset_index_offset === undefined || column.offset_index_length === undefined) return null
        const offsetIndexOffset = Number(column.offset_index_offset)
        const offsetIndexLength = Number(column.offset_index_length)
        const offsetIndexArrayBuffer = arrayBuffer.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
        const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
        return readOffsetIndex(offsetIndexReader)
      }))
      const expected = fileToJson(`test/files/${offsetIndexesFiles[i]}`)
      expect(toJson(result)).toEqual(expected)
    })
  })
})

/**
 * @param {string} filename
 * @returns {Promise<ArrayBuffer>}
 */
function readFileToArrayBuffer(filename) {
  return asyncBufferFromFile(filename).then((buffer) => buffer.slice(0))
}
