import { afterEach, describe, expect, it, vi } from 'vitest'
import { asyncBufferFromFile } from '../src/node.js'

describe('asyncBufferFromFile', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

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

  it('returns large slices without copying them again', async () => {
    const file = await asyncBufferFromFile('test/files/delta_binary_packed.parquet')
    const sliceSpy = vi.spyOn(ArrayBuffer.prototype, 'slice')
    const buffer = await file.slice(0, file.byteLength)
    expect(sliceSpy).not.toHaveBeenCalled()
    expect(buffer.byteLength).toBe(file.byteLength)
    expect(new Uint8Array(buffer, 0, 4)).toEqual(new Uint8Array([80, 65, 82, 49]))
  })

  it('copies small slices out of the shared allocation pool', async () => {
    const file = await asyncBufferFromFile('test/files/alpha.parquet')
    const buffer = await file.slice(0, 4)
    expect(buffer.byteLength).toBe(4)
    expect(new Uint8Array(buffer)).toEqual(new Uint8Array([80, 65, 82, 49]))
  })
})
