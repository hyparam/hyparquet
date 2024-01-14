/**
 * Return an offset view into an existing array buffer.
 * If slice is called on data outside the original array buffer, an error is thrown.
 *
 * This is useful for pre-loading a section of a file into memory,
 * then reading slices from it, but indexed relative to the original file.
 *
 * @typedef {import('./types.js').ArrayBufferLike} ArrayBufferLike
 * @param {ArrayBuffer} arrayBuffer array buffer to place at offset
 * @param {number} offset offset in bytes
 * @returns {ArrayBufferLike} array buffer view with offset
 */
export function offsetArrayBuffer(arrayBuffer, offset) {
  if (offset < 0) throw new Error(`offset must be positive ${offset}`)
  return {
    byteLength: offset + arrayBuffer.byteLength,
    slice(start, end) {
      if (start < offset || start > offset + arrayBuffer.byteLength) {
        throw new Error(`start out of bounds: ${start} not in ${offset}..${offset + arrayBuffer.byteLength}`)
      }
      if (end) {
        if (end < offset || end > offset + arrayBuffer.byteLength) {
          throw new Error(`end out of bounds: ${end} not in ${offset}..${offset + arrayBuffer.byteLength}`)
        }
        end -= offset
      }
      return arrayBuffer.slice(start - offset, end)
    },
  }
}
