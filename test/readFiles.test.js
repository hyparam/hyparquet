import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { gunzipSync } from 'zlib'
import { parquetRead } from '../src/hyparquet.js'
import { toJson } from '../src/utils.js'
import { fileToAsyncBuffer, fileToJson } from './helpers.js'

/**
 * @type {import('../src/types.js').Compressors}
 */
const compressors = {
  GZIP: (/** @type {Uint8Array} */ input, /** @type {number} */ outputLength) => {
    const result = gunzipSync(input)
    return new Uint8Array(result.buffer, result.byteOffset, outputLength)
  },
}

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
          expect(toJson(rows)).toEqual(expected)
        },
      })
    })
  })
})
