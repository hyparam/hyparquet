import fs from 'fs'

/**
 * Helper function to read .parquet file into ArrayBuffer
 *
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
export async function readFileToArrayBuffer(filePath) {
  const buffer = await fs.promises.readFile(filePath)
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
}

/**
 * Wrap .parquet file in an AsyncBuffer
 *
 * @param {string} filePath
 * @returns {import('../src/types.js').AsyncBuffer}
 */
export function fileToAsyncBuffer(filePath) {
  return {
    byteLength: fs.statSync(filePath).size,
    slice: async (start, end) => (await readFileToArrayBuffer(filePath)).slice(start, end),
  }
}

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
 * @param {number[]} bytes
 * @returns {import('../src/types.js').DataReader}
 */
export function reader(bytes) {
  return { view: new DataView(new Uint8Array(bytes).buffer), offset: 0 }
}
