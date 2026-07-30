/**
 * Decompress gzip data using the native DecompressionStream API.
 *
 * @param {Uint8Array} input compressed data
 * @param {number} outputLength expected decompressed size
 * @returns {Promise<Uint8Array>} decompressed data
 */
export async function gzipUncompress(input, outputLength) {
  const stream = new DecompressionStream('gzip')
  const writer = stream.writable.getWriter()
  // eslint-disable-next-line no-extra-parens
  writer.write(/** @type {Uint8Array<ArrayBuffer>} */ (input)).catch(() => {})
  writer.close().catch(() => {})
  const output = new Uint8Array(outputLength)
  let offset = 0
  const reader = stream.readable.getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    if (offset + value.length > outputLength) {
      throw new Error(`parquet gzip decompressed data exceeds expected length ${outputLength}`)
    }
    output.set(value, offset)
    offset += value.length
  }
  return offset === outputLength ? output : output.subarray(0, offset)
}
