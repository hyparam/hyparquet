// XxHash64 (https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md)
// Used by parquet bloom filters: hashes the PLAIN encoding of column values with seed 0.

const MASK = 0xffffffffffffffffn
const PRIME1 = 0x9e3779b185ebca87n
const PRIME2 = 0xc2b2ae3d27d4eb4fn
const PRIME3 = 0x165667b19e3779f9n
const PRIME4 = 0x85ebca77c2b2ae63n
const PRIME5 = 0x27d4eb2f165667c5n

/**
 * @param {bigint} x
 * @param {bigint} r rotation amount in bits (1..63)
 * @returns {bigint}
 */
function rotl64(x, r) {
  return (x << r | x >> 64n - r) & MASK
}

/**
 * @param {bigint} acc
 * @param {bigint} val
 * @returns {bigint}
 */
function round(acc, val) {
  acc = acc + val * PRIME2 & MASK
  acc = rotl64(acc, 31n)
  return acc * PRIME1 & MASK
}

/**
 * @param {bigint} acc
 * @param {bigint} val
 * @returns {bigint}
 */
function mergeRound(acc, val) {
  acc ^= round(0n, val)
  return acc * PRIME1 + PRIME4 & MASK
}

/**
 * Compute the 64-bit xxHash of a byte buffer.
 *
 * @param {Uint8Array} input
 * @param {bigint} [seed]
 * @returns {bigint} 64-bit hash
 */
export function xxhash64(input, seed = 0n) {
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength)
  const len = input.byteLength
  let offset = 0
  let h64

  if (len >= 32) {
    let v1 = seed + PRIME1 + PRIME2 & MASK
    let v2 = seed + PRIME2 & MASK
    let v3 = seed
    let v4 = seed - PRIME1 & MASK

    while (offset + 32 <= len) {
      v1 = round(v1, view.getBigUint64(offset, true)); offset += 8
      v2 = round(v2, view.getBigUint64(offset, true)); offset += 8
      v3 = round(v3, view.getBigUint64(offset, true)); offset += 8
      v4 = round(v4, view.getBigUint64(offset, true)); offset += 8
    }

    h64 = rotl64(v1, 1n) + rotl64(v2, 7n) + rotl64(v3, 12n) + rotl64(v4, 18n) & MASK
    h64 = mergeRound(h64, v1)
    h64 = mergeRound(h64, v2)
    h64 = mergeRound(h64, v3)
    h64 = mergeRound(h64, v4)
  } else {
    h64 = seed + PRIME5 & MASK
  }

  h64 = h64 + BigInt(len) & MASK

  while (offset + 8 <= len) {
    h64 ^= round(0n, view.getBigUint64(offset, true))
    h64 = rotl64(h64, 27n) * PRIME1 + PRIME4 & MASK
    offset += 8
  }

  if (offset + 4 <= len) {
    h64 ^= BigInt(view.getUint32(offset, true)) * PRIME1 & MASK
    h64 = rotl64(h64, 23n) * PRIME2 + PRIME3 & MASK
    offset += 4
  }

  while (offset < len) {
    h64 ^= BigInt(view.getUint8(offset)) * PRIME5 & MASK
    h64 = rotl64(h64, 11n) * PRIME1 & MASK
    offset += 1
  }

  h64 ^= h64 >> 33n
  h64 = h64 * PRIME2 & MASK
  h64 ^= h64 >> 29n
  h64 = h64 * PRIME3 & MASK
  h64 ^= h64 >> 32n
  return h64
}
