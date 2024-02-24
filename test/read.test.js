import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetRead } from '../src/hyparquet.js'
import { toJson } from '../src/toJson.js'
import { fileToAsyncBuffer, fileToJson } from './helpers.js'

describe('parquetRead', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.parquet'))

  files.forEach(file => {
    it(`should parse data from ${file}`, async () => {
      const asyncBuffer = fileToAsyncBuffer(`test/files/${file}`)
      await parquetRead({
        file: asyncBuffer,
        onComplete: (rows) => {
          const base = file.replace('.parquet', '')
          const expected = fileToJson(`test/files/${base}.json`)
          expect(toJson(rows)).toEqual(expected)
        },
      })
    })
  })
})
