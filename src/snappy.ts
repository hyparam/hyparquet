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
function copyBytes(fromArray: Uint8Array, fromPos: number, toArray: Uint8Array, toPos: number, length: number) {
  for (let i = 0; i < length; i++) {
    toArray[toPos + i] = fromArray[fromPos + i]
  }
}

/**
 * Copy bytes within an array
 *
 * @param {Uint8Array} array source and destination array
 * @param {number} pos source position
 * @param {number} offset offset back from current position to read
 * @param {number} length number of bytes to copy
 * @returns {void}
 */
function selfCopyBytes(array: Uint8Array, pos: number, offset: number, length: number) {
  for (let i = 0; i < length; i++) {
    array[pos + i] = array[pos - offset + i]
  }
}

/**
 * Decompress snappy data.
 * Accepts an output buffer to avoid allocating a new buffer for each call.
 *
 * @param {Uint8Array} inputArray compressed data
 * @param {Uint8Array} outputArray output buffer
 * @returns {boolean} true if successful
 */
export function snappyUncompress(inputArray: Uint8Array, outputArray: Uint8Array): boolean {
  const inputLength = inputArray.byteLength

  let pos = 0
  let outPos = 0

  // skip preamble (contains uncompressed length as varint)
  let uncompressedLength = 0
  let shift = 0
  while (pos < inputLength) {
    const c = inputArray[pos]
    pos += 1
    uncompressedLength |= (c & 0x7f) << shift
    if (c < 128) {
      break
    }
    shift += 7
  }

  while (pos < inputLength) {
    const c = inputArray[pos]
    let len = 0
    pos += 1

    // There are two types of elements, literals and copies (back references)
    if ((c & 0x3) === 0) {
      // Literals are uncompressed data stored directly in the byte stream
      let len = (c >>> 2) + 1
      // Longer literal length is encoded in multiple bytes
      if (len > 60) {
        if (pos + 3 >= inputLength) {
          console.warn('snappy error literal pos + 3 >= inputLength')
          return false
        }
        const lengthSize = len - 60 // length bytes - 1
        len = inputArray[pos]
          + (inputArray[pos + 1] << 8)
          + (inputArray[pos + 2] << 16)
          + (inputArray[pos + 3] << 24)
        len = (len & WORD_MASK[lengthSize]) + 1
        pos += lengthSize
      }
      if (pos + len > inputLength) {
        return false // literal exceeds input length
      }
      copyBytes(inputArray, pos, outputArray, outPos, len)
      pos += len
      outPos += len
    } else {
      // Copy elements
      let offset = 0 // offset back from current position to read
      switch (c & 0x3) {
      case 1:
        // Copy with 1-byte offset
        len = ((c >>> 2) & 0x7) + 4
        offset = inputArray[pos] + ((c >>> 5) << 8)
        pos += 1
        break
      case 2:
        // Copy with 2-byte offset
        if (inputLength <= pos + 1) {
          return false // end of input
        }
        len = (c >>> 2) + 1
        offset = inputArray[pos] + (inputArray[pos + 1] << 8)
        pos += 2
        break
      case 3:
        // Copy with 4-byte offset
        if (inputLength <= pos + 3) {
          return false // end of input
        }
        len = (c >>> 2) + 1
        offset = inputArray[pos]
          + (inputArray[pos + 1] << 8)
          + (inputArray[pos + 2] << 16)
          + (inputArray[pos + 3] << 24)
        pos += 4
        break
      default:
        break
      }
      if (offset === 0 || isNaN(offset)) {
        return false // invalid offset
      }
      if (offset > outPos) {
        return false // cannot copy from before start of buffer
      }
      selfCopyBytes(outputArray, outPos, offset, len)
      outPos += len
    }
  }
  return true
}
