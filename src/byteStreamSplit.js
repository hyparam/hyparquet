/**
 * @param {import('./types.d.ts').DataReader} reader
 * @param {number} nValues
 * @param {Float32Array | Float64Array} output
 */
export function byteStreamSplit(reader, nValues, output) {
  const byteWidth = output instanceof Float32Array ? 4 : 8
  const bytes = new Uint8Array(output.buffer)
  for (let b = 0; b < byteWidth; b++) {
    for (let i = 0; i < nValues; i++) {
      bytes[i * byteWidth + b] = reader.view.getUint8(reader.offset++)
    }
  }
}
