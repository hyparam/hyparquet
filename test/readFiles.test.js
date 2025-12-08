import fs from 'fs'
import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { parquetMetadata, parquetRead, toJson } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { fileToJson } from './helpers.js'

/**
 * Convert object rows to array rows for comparison with legacy JSON test files.
 * @param {Record<string, any>[]} rows
 * @returns {any[][]}
 */
function toArrays(rows) {
  return rows.map(row => Object.values(row))
}

describe('parquetRead test files', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.parquet'))

  files.forEach(filename => {
    it(`parse data from ${filename}`, async () => {
      const file = await asyncBufferFromFile(`test/files/${filename}`)
      await parquetRead({
        file,
        compressors,
        onComplete(rows) {
          const base = filename.replace('.parquet', '')
          const expected = fileToJson(`test/files/${base}.json`)
          // stringify and parse to make legal json (NaN, -0, etc)
          expect(JSON.parse(JSON.stringify(toJson(toArrays(rows))))).toEqual(expected)
        },
      })
    })

    it(`read the last row from ${filename}`, async () => {
      // this exercises some of the page-skipping optimizations
      const file = await asyncBufferFromFile(`test/files/${filename}`)
      const metadata = await parquetMetadata(file)
      let numRows = Number(metadata.num_rows)
      // repeated_no_annotation has wrong num_rows in metadata:
      if (filename === 'repeated_no_annotation.parquet') numRows = 6
      await parquetRead({
        file,
        compressors,
        rowStart: numRows - 1,
        rowEnd: numRows,
        onComplete(rows) {
          const base = filename.replace('.parquet', '')
          if (rows.length) {
            const expected = [fileToJson(`test/files/${base}.json`).at(-1)]
            expect(toJson(toArrays(rows))).toEqual(expected)
          }
        },
      })
    })
  })
})
