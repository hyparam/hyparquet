import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetRead } from '../src/hyparquet.js'
import { toJson } from '../src/toJson.js'

/**
 * Helper function to read .parquet file into ArrayBuffer
 *
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
async function readFileToArrayBuffer(filePath) {
  const buffer = await fs.promises.readFile(filePath)
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
}

/**
 * Wrap .parquet file in an AsyncBuffer
 *
 * @typedef {import('../src/types.js').AsyncBuffer} AsyncBuffer
 * @param {string} filePath
 * @returns {AsyncBuffer}
 */
function fileToAsyncBuffer(filePath) {
  return {
    byteLength: fs.statSync(filePath).size,
    slice: async (start, end) => (await readFileToArrayBuffer(filePath)).slice(start, end),
  }
}

describe('parquetMetadataAsync', () => {
  it('should parse data from addrtype-missing-value.parquet', async () => {
    const asyncBuffer = fileToAsyncBuffer('test/files/addrtype-missing-value.parquet')
    await parquetRead({
      file: asyncBuffer,
      onComplete: (rows) => {
        expect(toJson(rows)).toEqual(addrtypeData)
      },
    })
  })

  it('should parse data from rowgroups.parquet', async () => {
    const asyncBuffer = fileToAsyncBuffer('test/files/rowgroups.parquet')
    await parquetRead({
      file: asyncBuffer,
      onComplete: (rows) => {
        expect(toJson(rows)).toEqual(rowgroupsData)
      },
    })
  })
})

// Parquet v1 from DuckDB
const addrtypeData = [
  ['Block'],
  ['Intersection'],
  ['Block'],
  ['Block'],
  [undefined],
  ['Block'],
  ['Intersection'],
  ['Block'],
  ['Block'],
  ['Intersection'],
]

const rowgroupsData = [
  [1],
  [2],
  [3],
  [4],
  [5],
  [6],
  [7],
  [8],
  [9],
  [10],
  [11],
  [12],
  [13],
  [14],
  [15],
]
