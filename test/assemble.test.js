import { describe, expect, it } from 'vitest'
import { assembleLists } from '../src/assemble.js'

/** @typedef {import('../src/types.js').FieldRepetitionType | undefined} FieldRepetitionType */

describe('assembleLists', () => {
  /** @type {FieldRepetitionType[]} */
  const nonnullable = [undefined, 'REQUIRED', 'REPEATED', 'REQUIRED']
  /** @type {FieldRepetitionType[]} */
  const nullable = [undefined, 'OPTIONAL', 'REPEATED', 'OPTIONAL']
  /** @type {FieldRepetitionType[]} */
  const nestedRequired = [undefined, 'REQUIRED', 'REPEATED', 'REQUIRED', 'REPEATED', 'REQUIRED']
  /** @type {FieldRepetitionType[]} */
  const nestedOptional = [undefined, 'OPTIONAL', 'REPEATED', 'OPTIONAL', 'REPEATED', 'OPTIONAL']

  it('should not change flat objects', () => {
    const values = ['a', 'b']
    const result = assembleLists([], [], values, [undefined, 'REQUIRED'], 0, 0)
    expect(result).toEqual(['a', 'b'])
  })

  it('should not change nested required objects', () => {
    const values = ['a', 'b']
    const result = assembleLists([], [], values, [undefined, 'REQUIRED', 'REQUIRED'], 0, 0)
    expect(result).toEqual(['a', 'b'])
  })

  it('should assemble objects with non-null values', () => {
    const repetitionLevels = [0, 1]
    const values = ['a', 'b']
    const result = assembleLists([], repetitionLevels, values, nonnullable, 1, 1)
    expect(result).toEqual([['a', 'b']])
  })

  it('should handle null values', () => {
    const definitionLevels = [3, 0, 3]
    const repetitionLevels = [0, 1, 1]
    const values = ['a', 'c']
    const result = assembleLists(definitionLevels, repetitionLevels, values, nullable, 3, 1)
    expect(result).toEqual([[['a', null, 'c']]])
  })

  it('should handle empty lists', () => {
    expect(assembleLists([], [], [], nonnullable, 0, 0)).toEqual([])
    expect(assembleLists([], [], [], nonnullable, 1, 0)).toEqual([[]])
  })

  it('should handle multiple lists', () => {
    const repetitionLevels = [0, 0]
    const values = [22, 33]
    const result = assembleLists([], repetitionLevels, values, nonnullable, 1, 1)
    expect(result).toEqual([[22], [33]])
  })

  it('should handle multiple lists (6)', () => {
    const repetitionLevels = [0, 1, 1, 0, 1, 1]
    const values = [1, 2, 3, 4, 5, 6]
    const result = assembleLists([], repetitionLevels, values, nonnullable, 1, 1)
    expect(result).toEqual([[1, 2, 3], [4, 5, 6]])
  })

  it('should assemble multiple lists with nulls', () => {
    const definitionLevels = [3, 3, 0, 3, 3]
    const repetitionLevels = [0, 1, 0, 0, 1]
    const values = ['a', 'b', 'd', 'e']
    const result = assembleLists(definitionLevels, repetitionLevels, values, nullable, 3, 1)
    expect(result).toEqual([[['a', 'b']], [], [['d', 'e']]])
  })

  // it('should handle continuing a row from the previous page', () => {
  //   const definitionLevels = [3, 3, 3, 1]
  //   const repetitionLevels = [1, 0, 1, 0]
  //   const values = ['a', 'b', 'c', 'd']
  //   const result = assembleObjects(definitionLevels, repetitionLevels, values, nullable, 3, 1)
  //   expect(result).toEqual([['b', 'c'], [undefined]])
  // })

  it('should handle nested arrays', () => {
    // from nullable.impala.parquet
    const repetitionLevels = [0, 2, 1, 2]
    const values = [1, 2, 3, 4]
    const result = assembleLists([], repetitionLevels, values, nestedRequired, 2, 2)
    expect(result).toEqual([[[1, 2], [3, 4]]])
  })

  it('should handle top repetition level', () => {
    // from int_map.parquet
    const definitionLevels = [2, 2, 2, 2, 1, 1, 1, 0, 2, 2]
    const repetitionLevels = [0, 1, 0, 1, 0, 0, 0, 0, 0, 1]
    const values = ['k1', 'k2', 'k1', 'k2', 'k1', 'k3']
    /** @type {FieldRepetitionType[]} */
    const repetitionPath = ['REQUIRED', 'OPTIONAL', 'REPEATED', 'REQUIRED'] // map key required
    const result = assembleLists(definitionLevels, repetitionLevels, values, repetitionPath, 2, 1)
    expect(result).toEqual([
      [['k1', 'k2']],
      [['k1', 'k2']],
      [[]],
      [[]],
      [[]],
      [],
      [['k1', 'k3']],
    ])
  })

  it('should handle empty lists with definition level', () => {
    // from nonnullable.impala.parquet
    expect(assembleLists([0], [0], [], nonnullable, 1, 2)).toEqual([[]])
  })

  it('should handle nonnullable lists', () => {
    // from nonnullable.impala.parquet
    expect(assembleLists([1], [0], [-1], nonnullable, 1, 2)).toEqual([[-1]])
  })

  it('should handle nullable int_array', () => {
    // from nullable.impala.parquet int_array
    //                       [1  2  3][N  1  2  N  3  N][ ] N  N
    const definitionLevels = [3, 3, 3, 2, 3, 3, 2, 3, 2, 1, 0, 0]
    const repetitionLevels = [0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0]
    const values = [1, 2, 3, 1, 2, 3]
    const result = assembleLists(definitionLevels, repetitionLevels, values, nullable, 3, 1)
    expect(result).toEqual([
      [[1, 2, 3]],
      [[null, 1, 2, null, 3, null]],
      [[]],
      [],
      [],
    ])
  })

  it('should handle nullable int_array_Array', () => {
    // from nullable.impala.parquet int_array_Array
    //                       [1  2][3  4][[N 1  2  N][3  N  4] [] N][N] [] N  N [N  5  6]
    const definitionLevels = [5, 5, 5, 5, 4, 5, 5, 4, 5, 4, 5, 3, 2, 2, 1, 0, 0, 2, 5, 5]
    const repetitionLevels = [0, 2, 1, 2, 0, 2, 2, 2, 1, 2, 2, 1, 1, 0, 0, 0, 0, 0, 1, 2]
    const values = [1, 2, 3, 4, 1, 2, 3, 4, 5, 6]
    const result = assembleLists(definitionLevels, repetitionLevels, values, nestedOptional, 5, 2)
    expect(result).toEqual([
      [[[[1, 2]], [[3, 4]]]],
      [[[[null, 1, 2, null]], [[3, null, 4]], [[]], []]],
      [[[]]],
      [[]],
      [],
      [],
      [[[], [[5, 6]]]],
    ])
  })

  it('should handle nonnullable int_map_array keys', () => {
    const definitionLevels = [3, 4, 3, 3]
    const repetitionLevels = [0, 1, 1, 1]
    const values = ['k1']
    const result = assembleLists(definitionLevels, repetitionLevels, values, nullable, 4, 2)
    expect(result).toEqual([[[null, 'k1', null, null]]])
  })

  it('should handle nonnullable int_map_array values', () => {
    const definitionLevels = [3, 5, 3, 3]
    const repetitionLevels = [0, 1, 1, 1]
    const values = ['v1']
    const result = assembleLists(definitionLevels, repetitionLevels, values, nullable, 5, 2)
    expect(result).toEqual([[[null, 'v1', null, null]]])
  })

  it('should handle mixed optional and required', () => {
    // from datapage_v2.snappy.parquet e
    const definitionLevels = [2, 2, 2, 0, 0, 2, 2, 2, 2, 2]
    const repetitionLevels = [0, 1, 1, 0, 0, 0, 1, 1, 0, 1]
    const values = [1, 2, 3, 1, 2, 3, 1, 2]
    /** @type {FieldRepetitionType[]} */
    const repetitionPath = [undefined, 'OPTIONAL', 'REPEATED', 'REQUIRED']
    const result = assembleLists(definitionLevels, repetitionLevels, values, repetitionPath, 2, 1)
    expect(result).toEqual([[[1, 2, 3]], [], [], [[1, 2, 3]], [[1, 2]]])
  })

  it('should handle nested required', () => {
    // from nonnullable.impala.parquet nested_Struct i
    const definitionLevels = [0]
    const repetitionLevels = [0]
    /** @type {FieldRepetitionType[]} */
    const repetitionPath = [undefined, 'REQUIRED', 'REQUIRED', 'REPEATED', 'REQUIRED', 'REQUIRED', 'REPEATED', 'REQUIRED']
    const result = assembleLists(definitionLevels, repetitionLevels, [], repetitionPath, 2, 2)
    expect(result).toEqual([[]])
  })

  it('should handle dzenilee', () => {
    const repetitionLevels = [0, 1, 1, 0, 1, 1]
    const values = ['a', 'b', 'c', 'd', 'e', 'f']
    const result = assembleLists([], repetitionLevels, values, nullable, 3, 1)
    expect(result).toEqual([[['a', 'b', 'c']], [['d', 'e', 'f']]])
  })

  it('handle complex.parquet with nested require', () => {
    const definitionLevels = [1, 1]
    const values = ['a', 'b']
    const result = assembleLists(definitionLevels, [], values, [undefined, 'OPTIONAL', 'REQUIRED', 'REQUIRED'], 1, 0)
    expect(result).toEqual([['a'], ['b']])
  })
})
