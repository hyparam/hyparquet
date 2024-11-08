import { describe, expect, it, vi } from 'vitest'
import { cachedAsyncBuffer } from '../src/asyncBuffer.js'

describe('cachedAsyncBuffer', () => {
  it('caches slices of a file to avoid multiple reads', async () => {
    const slice = vi.fn(async (start, end) => {
      // Simulate an async slice operation
      await new Promise(resolve => setTimeout(resolve, 10))
      if (end === undefined) end = 1000
      if (start < 0) start = Math.max(0, 1000 + start)
      const buffer = new ArrayBuffer(end - start)
      return buffer
    })
    const cachedFile = cachedAsyncBuffer({
      byteLength: 1000,
      slice,
    })

    // Test cache miss
    const slice1 = await cachedFile.slice(0, 100)
    expect(slice).toHaveBeenCalledTimes(1)
    expect(slice1.byteLength).toBe(100)

    // Test cache hit for the same range
    const slice2 = await cachedFile.slice(0, 100)
    expect(slice).toHaveBeenCalledTimes(1) // No additional call
    expect(slice2).toBe(slice1) // Exact same object from cache

    // Test cache with undefined end, should use byteLength as end
    const slice3 = await cachedFile.slice(900)
    expect(slice).toHaveBeenCalledTimes(2)
    expect(slice3.byteLength).toBe(100)

    // Test cache hit for suffix-range
    const slice4 = await cachedFile.slice(-100)
    expect(slice).toHaveBeenCalledTimes(2) // Still no additional call
    expect(slice4).toBe(slice3) // Cached result reused

    // Verify that asking for the same end implicitly gets from cache
    const slice5 = await cachedFile.slice(900, 1000)
    expect(slice).toHaveBeenCalledTimes(2) // Still no additional call
    expect(slice5).toBe(slice3) // Cached result reused
  })
})
