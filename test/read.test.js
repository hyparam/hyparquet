import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { gunzipSync } from 'zlib'
import { parquetRead } from '../src/hyparquet.js'
import { toJson } from '../src/toJson.js'
import { fileToAsyncBuffer, fileToJson } from './helpers.js'

/**
 * @typedef {import('../src/types.js').Compressors} Compressors
 * @type {Compressors}
 */
const compressors = {
  GZIP: (/** @type {Uint8Array} */ input, /** @type {number} */ outputLength) => {
    const result = gunzipSync(input)
    return new Uint8Array(result.buffer, result.byteOffset, outputLength)
  },
}

describe('parquetRead', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.parquet'))

  files.forEach(file => {
    it(`should parse data from ${file}`, async () => {
      const asyncBuffer = fileToAsyncBuffer(`test/files/${file}`)
      await parquetRead({
        file: asyncBuffer,
        compressors,
        onComplete: (rows) => {
          const base = file.replace('.parquet', '')
          const expected = fileToJson(`test/files/${base}.json`)
          expect(toJson(rows)).toEqual(expected)
        },
      })
    })
  })

  it('should read a single column from a file', async () => {
    const asyncBuffer = fileToAsyncBuffer('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file: asyncBuffer,
      columns: ['c'],
      onChunk: (rows) => {
        expect(toJson(rows)).toEqual({
          columnName: 'c',
          columnData: [2, 3, 4, 5, 2],
          rowStart: 0,
          rowEnd: 5,
        })
      },
      onComplete: (rows) => {
        /* eslint-disable no-sparse-arrays */
        expect(toJson(rows)).toEqual([
          [,, 2],
          [,, 3],
          [,, 4],
          [,, 5],
          [,, 2],
        ])
      },
    })
  })

  it('should read a list-like column from a file', async () => {
    const asyncBuffer = fileToAsyncBuffer('test/files/datapage_v2.snappy.parquet')
    await parquetRead({
      file: asyncBuffer,
      columns: ['e'],
      onChunk: (rows) => {
        expect(toJson(rows)).toEqual({
          columnName: 'e',
          columnData: [[1, 2, 3], null, null, [1, 2, 3], [1, 2]],
          rowStart: 0,
          rowEnd: 5,
        })
      },
      onComplete: (rows) => {
        /* eslint-disable no-sparse-arrays */
        expect(toJson(rows)).toEqual([
          [,,,, [1, 2, 3]],
          [,,,, null],
          [,,,, null],
          [,,,, [1, 2, 3]],
          [,,,, [1, 2]],
        ])
      },
    })
  })
})
