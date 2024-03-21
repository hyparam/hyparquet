import { describe, expect, it } from 'vitest'
import { assembleObjects } from '../src/assemble.js'

describe('assembleObjects', () => {
  it('should assemble objects with non-null values', () => {
    const repetitionLevels = [0, 1]
    const values = ['a', 'b']
    const result = assembleObjects([], repetitionLevels, values, false, 3, 1)
    expect(result).toEqual([['a', 'b']])
  })

  it('should handle null values', () => {
    const definitionLevels = [3, 0, 3]
    const repetitionLevels = [0, 1, 1]
    const values = ['a', 'c']
    const result = assembleObjects(definitionLevels, repetitionLevels, values, true, 3, 1)
    expect(result).toEqual([['a', undefined, 'c']])
  })

  it('should handle empty lists', () => {
    const result = assembleObjects([], [], [], false, 3, 0)
    expect(result).toEqual([])
  })

  it('should handle multiple lists', () => {
    const repetitionLevels = [0, 0]
    const values = [22, 33]
    const result = assembleObjects([], repetitionLevels, values, false, 3, 1)
    expect(result).toEqual([[22], [33]])
  })

  it('should handle multiple lists (6)', () => {
    const repetitionLevels = [0, 1, 1, 0, 1, 1]
    const values = [1, 2, 3, 4, 5, 6]
    const result = assembleObjects([], repetitionLevels, values, false, 3, 1)
    expect(result).toEqual([[1, 2, 3], [4, 5, 6]])
  })

  it('should assemble multiple lists with nulls', () => {
    const definitionLevels = [3, 3, 0, 3, 3]
    const repetitionLevels = [0, 1, 0, 0, 1]
    const values = ['a', 'b', 'd', 'e']
    const result = assembleObjects(definitionLevels, repetitionLevels, values, true, 3, 1)
    expect(result).toEqual([['a', 'b'], [undefined], ['d', 'e']])
  })

  // it('should handle continuing a row from the previous page', () => {
  //   const definitionLevels = [3, 3, 3, 1]
  //   const repetitionLevels = [1, 0, 1, 0]
  //   const values = ['a', 'b', 'c', 'd']
  //   const result = assembleObjects(definitionLevels, repetitionLevels, values, false, 3, 1)
  //   expect(result).toEqual([['b', 'c'], [undefined]])
  // })

  it('should handle nested arrays', () => {
    // from nullable.impala.parquet
    const repetitionLevels = [0, 2, 1, 2]
    const values = [1, 2, 3, 4]
    const result = assembleObjects([], repetitionLevels, values, false, 3, 2)
    expect(result).toEqual([[[1, 2], [3, 4]]])
  })
})
