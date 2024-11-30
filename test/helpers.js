import fs from 'fs'

/**
 * Read .parquet file into JSON
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
