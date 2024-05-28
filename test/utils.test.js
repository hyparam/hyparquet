import { describe, expect, it } from 'vitest'
import { toJson } from '../src/utils.js'

describe('toJson', () => {
  it('convert undefined to null', () => {
    expect(toJson(undefined)).toBe(null)
    expect(toJson(null)).toBe(null)
  })

  it('convert bigint to number', () => {
    expect(toJson(BigInt(123))).toBe(123)
    expect(toJson([BigInt(123), BigInt(456)])).toEqual([123, 456])
    expect(toJson({ a: BigInt(123), b: { c: BigInt(456) } })).toEqual({ a: 123, b: { c: 456 } })
  })

  it('convert Uint8Array to array of numbers', () => {
    expect(toJson(new Uint8Array([1, 2, 3]))).toEqual([1, 2, 3])
  })

  it('convert Date to ISO string', () => {
    const date = new Date('2023-05-27T00:00:00Z')
    expect(toJson(date)).toBe(date.toISOString())
  })

  it('ignore undefined properties in objects', () => {
    expect(toJson({ a: undefined, b: BigInt(123) })).toEqual({ b: 123 })
  })

  it('return null in objects unchanged', () => {
    expect(toJson({ a: null })).toEqual({ a: null })
    expect(toJson([null])).toEqual([null])
  })

  it('return other types unchanged', () => {
    expect(toJson('string')).toBe('string')
    expect(toJson(123)).toBe(123)
    expect(toJson(true)).toBe(true)
  })
})
