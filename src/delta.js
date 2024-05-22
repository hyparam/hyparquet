import { readVarInt, readZigZagBigInt } from './thrift.js'

/**
 * @typedef {import('./types.d.ts').DataReader} DataReader
 * @param {DataReader} reader
 * @param {number} count number of values to read
 * @param {Int32Array | BigInt64Array} output
 */
export function deltaBinaryUnpack(reader, count, output) {
  const int32 = output instanceof Int32Array
  const blockSize = readVarInt(reader)
  const miniblockPerBlock = readVarInt(reader)
  readVarInt(reader) // assert(=== count)
  let value = readZigZagBigInt(reader) // first value
  let outputIndex = 0
  output[outputIndex++] = int32 ? Number(value) : value

  const valuesPerMiniblock = blockSize / miniblockPerBlock

  while (outputIndex < count) {
    // new block
    const minDelta = readZigZagBigInt(reader)
    const bitWidths = new Uint8Array(miniblockPerBlock)
    for (let i = 0; i < miniblockPerBlock; i++) {
      bitWidths[i] = reader.view.getUint8(reader.offset++)
    }

    for (let i = 0; i < miniblockPerBlock && outputIndex < count; i++) {
      // new miniblock
      const bitWidth = BigInt(bitWidths[i])
      if (bitWidth) {
        let bitpackPos = 0n
        let miniblockCount = valuesPerMiniblock
        const mask = (1n << bitWidth) - 1n
        while (miniblockCount && outputIndex < count) {
          let bits = BigInt(reader.view.getUint8(reader.offset)) >> bitpackPos & mask // TODO: don't re-read value every time
          bitpackPos += bitWidth
          while (bitpackPos >= 8) {
            bitpackPos -= 8n
            reader.offset++
            if (bitpackPos) {
              bits |= BigInt(reader.view.getUint8(reader.offset)) << bitWidth - bitpackPos & mask
            }
          }
          const delta = minDelta + bits
          value += delta
          output[outputIndex++] = int32 ? Number(value) : value
          miniblockCount--
        }
        if (miniblockCount) {
          // consume leftover miniblock
          reader.offset += Math.ceil((miniblockCount * Number(bitWidth) + Number(bitpackPos)) / 8)
        }
      } else {
        for (let j = 0; j < valuesPerMiniblock && outputIndex < count; j++) {
          value += minDelta
          output[outputIndex++] = int32 ? Number(value) : value
        }
      }
    }
  }
}

/**
 * @param {DataReader} reader
 * @param {number} count
 * @param {Uint8Array[]} output
 */
export function deltaLengthByteArray(reader, count, output) {
  const lengths = new Int32Array(count)
  deltaBinaryUnpack(reader, count, lengths)
  for (let i = 0; i < count; i++) {
    output[i] = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, lengths[i])
    reader.offset += lengths[i]
  }
}

/**
 * @param {DataReader} reader
 * @param {number} count
 * @param {Uint8Array[]} output
 */
export function deltaByteArray(reader, count, output) {
  const prefixData = new Int32Array(count)
  deltaBinaryUnpack(reader, count, prefixData)
  const suffixData = new Int32Array(count)
  deltaBinaryUnpack(reader, count, suffixData)

  for (let i = 0; i < count; i++) {
    const suffix = new Uint8Array(reader.view.buffer, reader.view.byteOffset + reader.offset, suffixData[i])
    if (prefixData[i]) {
      // copy from previous value
      output[i] = new Uint8Array(prefixData[i] + suffixData[i])
      output[i].set(output[i - 1].subarray(0, prefixData[i]))
      output[i].set(suffix, prefixData[i])
    } else {
      output[i] = suffix
    }
    reader.offset += suffixData[i]
  }
}
