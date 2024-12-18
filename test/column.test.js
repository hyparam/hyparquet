import fs from 'fs'
import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { parquetMetadata } from '../src/hyparquet.js'
import { getSchemaPath } from '../src/schema.js'
import { getColumnRange, readColumn } from '../src/column.js'
import { asyncBufferFromFile, toJson } from '../src/utils.js'
import { fileToJson } from './helpers.js'

describe('readColumn', () => {
  const parquetFiles = fs.readdirSync('test/files').filter(f => f.endsWith('.parquet'))
  parquetFiles.forEach((file) => {
    it(`read columns from ${file} when rowLimit is undefined`, async () => {
      const arrayBuffer = await asyncBufferFromFile(`test/files/${file}`).then(e => e.slice(0))
      const metadata = parquetMetadata(arrayBuffer)

      const result = metadata.row_groups.map((rowGroup) => rowGroup.columns.map((column) => {
        const [columnStartByte, columnEndByte] = getColumnRange(column.meta_data).map(Number)
        const columnArrayBuffer = arrayBuffer.slice(columnStartByte, columnEndByte)
        const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
        const reader = { view: new DataView(columnArrayBuffer), offset: 0 }
        return readColumn(reader, undefined, column.meta_data, schemaPath, { compressors })
      }))

      const base = file.replace('.parquet', '')
      const expected = fileToJson(`test/files/${base}.columns.json`)
      expect(JSON.stringify(toJson(result))).toEqual(JSON.stringify(toJson(expected))) // ensure that we're not comparing NaN, -0, etc.
    })
  })
})
