import { describe, expect, it } from 'vitest'
import { offsetArrayBuffer } from '../src/asyncbuffer.js'

describe('offsetArrayBuffer', () => {
  it('creates a valid offset array buffer', () => {
    const buffer = new ArrayBuffer(10)
    const offsetBuffer = offsetArrayBuffer(buffer, 5)
    expect(offsetBuffer.byteLength).toBe(15)
  })

  it('correctly slices the array buffer with offset', () => {
    const buffer = new ArrayBuffer(10)
    const offsetBuffer = offsetArrayBuffer(buffer, 5)
    const view = new Uint8Array(buffer)
    for (let i = 0; i < view.length; i++) {
      view[i] = i // Populate the buffer with data [0, 1, 2, ...]
    }

    const slicedBuffer = offsetBuffer.slice(5, 10) // This should give us [0, 1, 2, 3, 4] from the original buffer
    const slicedView = new Uint8Array(slicedBuffer)

    for (let i = 0; i < slicedView.length; i++) {
      expect(slicedView[i]).toBe(i) // Each item should match its index
    }
  })

  it('throws error for negative offset', () => {
    const buffer = new ArrayBuffer(10)
    expect(() => offsetArrayBuffer(buffer, -5)).toThrow('offset must be positive')
  })

  it('throws error for out of bounds slice', () => {
    const buffer = new ArrayBuffer(10)
    const offsetBuffer = offsetArrayBuffer(buffer, 5)
    expect(() => offsetBuffer.slice(3, 7)).toThrow('start out of bounds')
    expect(() => offsetBuffer.slice(5, 20)).toThrow('end out of bounds')
  })
})
