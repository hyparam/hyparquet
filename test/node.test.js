import { describe, expect, it } from 'vitest'
import { asyncBufferFromFile } from '../src/node.js'

describe('asyncBufferFromFile', () => {
  it('uses end-exclusive slice ranges', async () => {
    const file = await asyncBufferFromFile('test/files/alpha.parquet')

    await expect(file.slice(0, 1)).resolves.toHaveProperty('byteLength', 1)
    await expect(file.slice(file.byteLength - 1, file.byteLength)).resolves.toHaveProperty('byteLength', 1)
  })

  it('returns an empty ArrayBuffer for zero-length slices', async () => {
    const file = await asyncBufferFromFile('test/files/alpha.parquet')

    await expect(file.slice(0, 0)).resolves.toEqual(new ArrayBuffer(0))
    await expect(file.slice(file.byteLength, file.byteLength)).resolves.toEqual(new ArrayBuffer(0))
  })
})
