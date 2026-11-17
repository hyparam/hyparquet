import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { alpDecode, alpDecodeDouble, alpDecodeFloat } from '../src/alp.js'
import { parquetReadObjects } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'

/**
 * @typedef {object} AlpVector
 * @property {number} exponent
 * @property {number} factor
 * @property {bigint | number} frameOfReference
 * @property {number} bitWidth
 * @property {(bigint | number)[]} deltas
 * @property {[number, number | bigint][]} [exceptions] - [position, value] pairs (bigint = raw IEEE-754 bits)
 */

/**
 * Build an ALP-encoded page per the parquet-format AlpEncoding.md layout.
 *
 * @param {'FLOAT' | 'DOUBLE'} type
 * @param {number} logVectorSize
 * @param {AlpVector[]} vectors
 * @param {object} [header]
 * @param {number} [header.compressionMode]
 * @param {number} [header.integerEncoding]
 * @param {number} [header.numElements]
 * @returns {import('../src/types.js').DataReader}
 */
function buildAlp(type, logVectorSize, vectors, header = {}) {
  const valueSize = type === 'FLOAT' ? 4 : 8
  const numElements = header.numElements ?? vectors.reduce((sum, v) => sum + v.deltas.length, 0)

  /** @type {Uint8Array[]} */
  const vectorBytes = vectors.map(v => {
    const { exponent, factor, frameOfReference, bitWidth, deltas, exceptions = [] } = v
    const packedBytes = Math.ceil(deltas.length * bitWidth / 8)
    const size = 4 + valueSize + 1 + packedBytes + exceptions.length * (2 + valueSize)
    const bytes = new Uint8Array(size)
    const view = new DataView(bytes.buffer)
    let offset = 0
    view.setUint8(offset++, exponent)
    view.setUint8(offset++, factor)
    view.setUint16(offset, exceptions.length, true)
    offset += 2
    if (type === 'FLOAT') {
      view.setInt32(offset, Number(frameOfReference), true)
      offset += 4
    } else {
      view.setBigInt64(offset, BigInt(frameOfReference), true)
      offset += 8
    }
    view.setUint8(offset++, bitWidth)
    // LSB-first bit packing
    let acc = 0n
    let bits = 0
    for (const delta of deltas) {
      acc |= BigInt(delta) << BigInt(bits)
      bits += bitWidth
      while (bits >= 8) {
        view.setUint8(offset++, Number(acc & 0xffn))
        acc >>= 8n
        bits -= 8
      }
    }
    if (bits > 0) view.setUint8(offset++, Number(acc & 0xffn))
    for (const [pos] of exceptions) {
      view.setUint16(offset, pos, true)
      offset += 2
    }
    for (const [, value] of exceptions) {
      // write raw bits when given a bigint, so NaN payloads are not canonicalized
      if (type === 'FLOAT') {
        if (typeof value === 'bigint') view.setUint32(offset, Number(value), true)
        else view.setFloat32(offset, value, true)
      } else {
        if (typeof value === 'bigint') view.setBigUint64(offset, value, true)
        else view.setFloat64(offset, value, true)
      }
      offset += valueSize
    }
    return bytes
  })

  const numVectors = vectorBytes.length
  const total = 7 + numVectors * 4 + vectorBytes.reduce((sum, b) => sum + b.length, 0)
  const bytes = new Uint8Array(total)
  const view = new DataView(bytes.buffer)
  view.setUint8(0, header.compressionMode ?? 0)
  view.setUint8(1, header.integerEncoding ?? 0)
  view.setUint8(2, logVectorSize)
  view.setInt32(3, numElements, true)
  let offset = numVectors * 4 // relative to start of offset array
  let pos = 7 + numVectors * 4
  vectorBytes.forEach((b, i) => {
    view.setUint32(7 + i * 4, offset, true)
    bytes.set(b, pos)
    offset += b.length
    pos += b.length
  })
  return { view, offset: 0 }
}

describe('alpDecodeFloat', () => {
  it('decodes decimal values without exceptions', () => {
    // 1.23, 4.56, 7.89, 0.12 with e=2, f=0 -> encoded [123, 456, 789, 12]
    const reader = buildAlp('FLOAT', 3, [
      { exponent: 2, factor: 0, frameOfReference: 12, bitWidth: 10, deltas: [111, 444, 777, 0] },
    ])
    const result = alpDecodeFloat(reader, 4)
    expect(result).toBeInstanceOf(Float32Array)
    expect(Array.from(result)).toEqual([1.23, 4.56, 7.89, 0.12].map(Math.fround))
    expect(reader.offset).toBe(reader.view.byteLength)
  })

  it('patches exceptions bit-exactly', () => {
    const nanBits = 0x7fc0dead
    const reader = buildAlp('FLOAT', 3, [
      {
        exponent: 1, factor: 0, frameOfReference: 10, bitWidth: 2, deltas: [0, 0, 2, 3],
        exceptions: [[1, BigInt(nanBits)], [3, -0]],
      },
    ])
    const result = alpDecodeFloat(reader, 4)
    expect(result[0]).toBe(1)
    expect(new Uint32Array(result.buffer)[1]).toBe(nanBits)
    expect(result[2]).toBeCloseTo(1.2)
    expect(Object.is(result[3], -0)).toBe(true)
  })

  it('handles wrapping int32 frame of reference', () => {
    // delta + FOR exceeds int32 max and must wrap
    const reader = buildAlp('FLOAT', 3, [
      { exponent: 0, factor: 0, frameOfReference: 0x7fffffff, bitWidth: 1, deltas: [0, 1] },
    ])
    const result = alpDecodeFloat(reader, 2)
    expect(result[0]).toBe(Math.fround(0x7fffffff))
    expect(result[1]).toBe(-2147483648)
  })
})

describe('alpDecodeDouble', () => {
  it('decodes the spec worked example with exceptions and non-zero factor', () => {
    // values [1500.0, NaN, 2500.0, 333.5], e=4, f=3, FOR=3335, bit_width=15
    const reader = buildAlp('DOUBLE', 10, [
      {
        exponent: 4, factor: 3, frameOfReference: 3335n, bitWidth: 15,
        deltas: [11665, 11665, 21665, 0], exceptions: [[1, NaN]],
      },
    ])
    expect(reader.view.byteLength).toBe(7 + 4 + 31)
    const result = alpDecodeDouble(reader, 4)
    expect(result).toBeInstanceOf(Float64Array)
    expect(Array.from(result)).toEqual([1500, NaN, 2500, 333.5])
    expect(reader.offset).toBe(reader.view.byteLength)
  })

  it('decodes multiple vectors using the offset array', () => {
    // log_vector_size=3 -> 8 per vector, 10 elements -> 2 vectors
    const reader = buildAlp('DOUBLE', 3, [
      { exponent: 1, factor: 0, frameOfReference: 10n, bitWidth: 3, deltas: [0, 1, 2, 3, 4, 5, 6, 7] },
      { exponent: 2, factor: 1, frameOfReference: -5n, bitWidth: 0, deltas: [0, 0] },
    ])
    const result = alpDecodeDouble(reader, 10)
    expect(Array.from(result)).toEqual([...[10, 11, 12, 13, 14, 15, 16, 17].map(x => x * 1 * 0.1), -5 * 10 * 0.01, -5 * 10 * 0.01])
    expect(reader.offset).toBe(reader.view.byteLength)
  })

  it('decodes 64-bit frame of reference and wide bit widths', () => {
    const reader = buildAlp('DOUBLE', 3, [
      {
        exponent: 0, factor: 0, frameOfReference: -8_000_000_000_000_000_000n, bitWidth: 64,
        deltas: [0n, 16_000_000_000_000_000_000n, 1n << 63n],
      },
    ])
    const result = alpDecodeDouble(reader, 3)
    expect(result[0]).toBe(-8e18)
    expect(result[1]).toBe(8e18)
    expect(result[2]).toBe(Number(BigInt.asIntN(64, (1n << 63n) - 8_000_000_000_000_000_000n)))
  })

  it('decodes with large but safe frame of reference on the fast path', () => {
    const reader = buildAlp('DOUBLE', 3, [
      { exponent: 3, factor: 0, frameOfReference: 2n ** 53n - 4n, bitWidth: 32, deltas: [0, 0xffffffff] },
    ])
    const result = alpDecodeDouble(reader, 2)
    expect(result[0]).toBe(Number(2n ** 53n - 4n) * 1e-3)
    expect(result[1]).toBe(Number(2n ** 53n - 4n + 0xffffffffn) * 1e-3)
  })
})

describe('alpDecode', () => {
  it('dispatches on type', () => {
    const vectors = [{ exponent: 0, factor: 0, frameOfReference: 7, bitWidth: 0, deltas: [0, 0] }]
    expect(alpDecode(buildAlp('FLOAT', 3, vectors), 2, 'FLOAT')).toBeInstanceOf(Float32Array)
    expect(alpDecode(buildAlp('DOUBLE', 3, vectors), 2, 'DOUBLE')).toBeInstanceOf(Float64Array)
  })

  it('throws for unsupported type', () => {
    const reader = buildAlp('DOUBLE', 3, [])
    expect(() => alpDecode(reader, 0, 'INT32')).toThrow('ALP encoding unsupported type: INT32')
  })

  it('throws for unsupported compression mode', () => {
    const reader = buildAlp('DOUBLE', 3, [], { compressionMode: 1 })
    expect(() => alpDecode(reader, 0, 'DOUBLE')).toThrow('ALP unsupported compression mode: 1')
  })

  it('throws for unsupported integer encoding', () => {
    const reader = buildAlp('FLOAT', 3, [], { integerEncoding: 1 })
    expect(() => alpDecode(reader, 0, 'FLOAT')).toThrow('ALP unsupported integer encoding: 1')
  })

  it('throws for invalid log_vector_size', () => {
    expect(() => alpDecode(buildAlp('FLOAT', 2, []), 0, 'FLOAT')).toThrow('ALP invalid log_vector_size: 2')
    expect(() => alpDecode(buildAlp('DOUBLE', 16, []), 0, 'DOUBLE')).toThrow('ALP invalid log_vector_size: 16')
  })

  it('throws when num_elements does not match expected count', () => {
    const vectors = [{ exponent: 0, factor: 0, frameOfReference: 7, bitWidth: 0, deltas: [0, 0] }]
    expect(() => alpDecode(buildAlp('DOUBLE', 3, vectors), 3, 'DOUBLE'))
      .toThrow('ALP num_elements 2 does not match expected 3')
  })
})

describe('alp_extended.zstd.parquet conformance', () => {
  /**
   * @param {number} x
   * @returns {number}
   */
  function bits32(x) {
    return new Uint32Array(new Float32Array([x]).buffer)[0]
  }
  /**
   * @param {number} x
   * @returns {bigint}
   */
  function bits64(x) {
    return new BigUint64Array(new Float64Array([x]).buffer)[0]
  }

  it('decodes ALP columns bit-identical to PLAIN columns', async () => {
    const file = await asyncBufferFromFile('test/files/alp_extended.zstd.parquet')
    const rows = await parquetReadObjects({ file, compressors })
    expect(rows.length).toBe(9032)

    for (const col of ['float_alp_1024', 'float_alp_4096', 'float_alp_32']) {
      for (let i = 0; i < rows.length; i++) {
        const value = rows[i][col]
        const expected = rows[i].float_plain
        if (expected === null) expect(value, `${col}[${i}]`).toBeNull()
        else expect(bits32(value), `${col}[${i}]`).toBe(bits32(expected))
      }
    }
    for (const col of ['double_alp_1024', 'double_alp_4096', 'double_alp_32']) {
      for (let i = 0; i < rows.length; i++) {
        const value = rows[i][col]
        const expected = rows[i].double_plain
        if (expected === null) expect(value, `${col}[${i}]`).toBeNull()
        else expect(bits64(value), `${col}[${i}]`).toBe(bits64(expected))
      }
    }

    // spot check documented edge cases
    expect(bits64(rows[1500].double_alp_1024)).toBe(0x7ff800deadbeef00n)
    expect(bits32(rows[2047].float_alp_32)).toBe(0xffc00001)
    expect(rows[2000].double_alp_4096).toBe(Infinity)
    expect(Object.is(rows[2002].float_alp_1024, -0)).toBe(true)
    expect(rows[2003].double_alp_32).toBe(5e-324)
    expect(rows[7777].double_alp_1024).toBe(7.77)
    expect(rows[8200].double_alp_1024).toBeNull()
    expect(rows[9000].double_alp_1024).toBe(-8e18)
    expect(rows[9001].double_alp_1024).toBe(8e18)
  })
})
