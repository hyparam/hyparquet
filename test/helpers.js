import fs from 'fs'

/**
 * Read file and parse as JSON
 *
 * @param {string} filePath
 * @returns {any}
 */
export function fileToJson(filePath) {
  const buffer = fs.readFileSync(filePath)
  return JSON.parse(buffer.toString())
}

/**
 * Make a DataReader from bytes
 *
 * @import {DataReader} from '../src/types.d.ts'
 * @param {number[]} bytes
 * @returns {DataReader}
 */
export function reader(bytes) {
  return { view: new DataView(new Uint8Array(bytes).buffer), offset: 0 }
}

/**
 * Wraps an AsyncBuffer to count the number of fetches made
 *
 * @import {AsyncBuffer} from '../src/types.js'
 * @param {AsyncBuffer} asyncBuffer
 * @returns {AsyncBuffer & {fetches: number}}
 */
export function countingBuffer(asyncBuffer) {
  return {
    ...asyncBuffer,
    fetches: 0,
    slice(start, end) {
      this.fetches++
      return asyncBuffer.slice(start, end)
    },
  }
}
