import fs from 'fs'
import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { parquetRead } from '../src/hyparquet.js'
import { toJson } from '../src/utils.js'
import { fileToAsyncBuffer, fileToJson } from './helpers.js'

describe('parquetRead test files', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.parquet'))

  files.forEach(filename => {
    it(`parse data from ${filename}`, async () => {
      const file = fileToAsyncBuffer(`test/files/${filename}`)
      await parquetRead({
        file,
        compressors,
        onComplete: (rows) => {
          const base = filename.replace('.parquet', '')
          const expected = fileToJson(`test/files/${base}.json`)
          // stringify and parse to make legal json
          expect(JSON.parse(JSON.stringify(toJson(rows)))).toEqual(expected)
        },
      })
    })
  })
})
