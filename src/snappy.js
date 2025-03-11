/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Zhipeng Jia
 * https://github.com/zhipeng-jia/snappyjs
 */

const WORD_MASK = [0, 0xff, 0xffff, 0xffffff, 0xffffffff]

/**
 * Copy bytes from one array to another
 *
 * @param {Uint8Array} fromArray source array
 * @param {number} fromPos source position
 * @param {Uint8Array} toArray destination array
 * @param {number} toPos destination position
 * @param {number} length number of bytes to copy
 * @returns {void}
 */
function copyBytes(fromArray, fromPos, toArray, toPos, length) {
  for (let i = 0; i < length; i++) {
    toArray[toPos + i] = fromArray[fromPos + i]
  }
}

/**
 * Decompress snappy data.
 * Accepts an output buffer to avoid allocating a new buffer for each call.
 *
 * @param {Uint8Array} input compressed data
 * @param {Uint8Array} output output buffer
 * @returns {void}
 */
export function snappyUncompress(input, output) {
  const inputLength = input.byteLength
  const outputLength = output.byteLength
  let pos = 0
  let outPos = 0

  // skip preamble (contains uncompressed length as varint)
  while (pos < inputLength) {
    const c = input[pos]
    pos++
    if (c < 128) {
      break
    }
  }
  if (outputLength && pos >= inputLength) {
    throw new Error('invalid snappy length header')
  }

  while (pos < inputLength) {
    const c = input[pos]
    let len = 0
    pos++

    if (pos >= inputLength) {
      throw new Error('missing eof marker')
    }

    // There are two types of elements, literals and copies (back references)
    if ((c & 0x3) === 0) {
      // Literals are uncompressed data stored directly in the byte stream
      let len = (c >>> 2) + 1
      // Longer literal length is encoded in multiple bytes
      if (len > 60) {
        if (pos + 3 >= inputLength) {
          throw new Error('snappy error literal pos + 3 >= inputLength')
        }
        const lengthSize = len - 60 // length bytes - 1
        len = input[pos]
          + (input[pos + 1] << 8)
          + (input[pos + 2] << 16)
          + (input[pos + 3] << 24)
        len = (len & WORD_MASK[lengthSize]) + 1
        pos += lengthSize
      }
      if (pos + len > inputLength) {
        throw new Error('snappy error literal exceeds input length')
      }
      copyBytes(input, pos, output, outPos, len)
      pos += len
      outPos += len
    } else {
      // Copy elements
      let offset = 0 // offset back from current position to read
      switch (c & 0x3) {
      case 1:
        // Copy with 1-byte offset
        len = (c >>> 2 & 0x7) + 4
        offset = input[pos] + (c >>> 5 << 8)
        pos++
        break
      case 2:
        // Copy with 2-byte offset
        if (inputLength <= pos + 1) {
          throw new Error('snappy error end of input')
        }
        len = (c >>> 2) + 1
        offset = input[pos] + (input[pos + 1] << 8)
        pos += 2
        break
      case 3:
        // Copy with 4-byte offset
        if (inputLength <= pos + 3) {
          throw new Error('snappy error end of input')
        }
        len = (c >>> 2) + 1
        offset = input[pos]
          + (input[pos + 1] << 8)
          + (input[pos + 2] << 16)
          + (input[pos + 3] << 24)
        pos += 4
        break
      default:
        break
      }
      if (offset === 0 || isNaN(offset)) {
        throw new Error(`invalid offset ${offset} pos ${pos} inputLength ${inputLength}`)
      }
      if (offset > outPos) {
        throw new Error('cannot copy from before start of buffer')
      }
      copyBytes(output, outPos - offset, output, outPos, len)
      outPos += len
    }
  }

  if (outPos !== outputLength) throw new Error('premature end of input')
}
