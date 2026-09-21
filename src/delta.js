/**
 * @import {DataReader} from '../src/types.js'
 */

import { readVarInt, readZigZag, readZigZagBigInt } from './thrift.js'

/**
 * @param {DataReader} reader
 * @param {number} count number of values to read
 * @param {Int32Array | BigInt64Array} output
 */
export function deltaBinaryUnpack(reader, count, output) {
  if (output instanceof Int32Array) {
    deltaBinaryUnpackInt32(reader, count, output)
    return
  }
  const blockSize = readVarInt(reader)
  const miniblockPerBlock = readVarInt(reader)
  readVarInt(reader) // assert(=== count)
  let value = readZigZagBigInt(reader) // first value
  let outputIndex = 0
  output[outputIndex++] = value

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
          output[outputIndex++] = value
          miniblockCount--
        }
        if (miniblockCount) {
          // consume leftover miniblock
          reader.offset += Math.ceil((miniblockCount * Number(bitWidth) + Number(bitpackPos)) / 8)
        }
      } else {
        for (let j = 0; j < valuesPerMiniblock && outputIndex < count; j++) {
          value += minDelta
          output[outputIndex++] = value
        }
      }
    }
  }
}

/**
 * Decode INT32 without BigInt arithmetic in the per-value loop.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @param {Int32Array} output
 */
function deltaBinaryUnpackInt32(reader, count, output) {
  const blockSize = readVarInt(reader)
  const miniblockPerBlock = readVarInt(reader)
  readVarInt(reader)
  let value = readZigZag(reader)
  let outputIndex = 0
  output[outputIndex++] = value
  const valuesPerMiniblock = blockSize / miniblockPerBlock
  while (outputIndex < count) {
    const minDelta = readZigZag(reader)
    const bitWidthsOffset = reader.offset
    reader.offset += miniblockPerBlock
    for (let i = 0; i < miniblockPerBlock && outputIndex < count; i++) {
      const bitWidth = reader.view.getUint8(bitWidthsOffset + i)
      const end = reader.offset + valuesPerMiniblock * bitWidth / 8
      let bitOffset = 0
      for (let j = 0; j < valuesPerMiniblock && outputIndex < count; j++) {
        let residual = 0
        let bitsRead = 0
        while (bitsRead < bitWidth) {
          const bitsToRead = Math.min(8 - bitOffset, bitWidth - bitsRead)
          residual |= (reader.view.getUint8(reader.offset) >>> bitOffset & (1 << bitsToRead) - 1) << bitsRead
          bitsRead += bitsToRead
          bitOffset += bitsToRead
          if (bitOffset === 8) {
            bitOffset = 0
            reader.offset++
          }
        }
        // Parquet INT32 delta arithmetic wraps in two's complement.
        value = value + minDelta + residual | 0
        output[outputIndex++] = value
      }
      // Consume the entire padded miniblock, but not unused miniblock bodies.
      reader.offset = end
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
