import { describe, expect, it } from 'vitest'
import { assembleLists } from '../src/assemble.js'

describe('assembleLists', () => {
  const nonnullable = toSchemaPath([undefined, 'REQUIRED', 'REPEATED', 'REQUIRED'])
  const nullable = toSchemaPath([undefined, 'OPTIONAL', 'REPEATED', 'OPTIONAL'])
  const nestedRequired = toSchemaPath([undefined, 'REQUIRED', 'REPEATED', 'REQUIRED', 'REPEATED', 'REQUIRED'])
  const nestedOptional = toSchemaPath([undefined, 'OPTIONAL', 'REPEATED', 'OPTIONAL', 'REPEATED', 'OPTIONAL'])

  /**
   * @import {FieldRepetitionType, SchemaTree} from '../src/types.js'
   * @param {(FieldRepetitionType | undefined)[]} repetitionPath
   * @returns {SchemaTree[]}
   */
  function toSchemaPath(repetitionPath) {
    return repetitionPath.map(repetition_type => ({
      element: {
        name: 'name',
        repetition_type,
      },
      count: 1,
      children: [],
      path: [],
    }))
  }

  it('should assemble objects with non-null values', () => {
    const repetitionLevels = [0, 1]
    const values = ['a', 'b']
    const result = assembleLists([], [], repetitionLevels, values, nonnullable)
    expect(result).toEqual([['a', 'b']])
  })

  it('should handle null values', () => {
    const definitionLevels = [3, 0, 3]
    const repetitionLevels = [0, 1, 1]
    const values = ['a', 'c']
    const result = assembleLists([], definitionLevels, repetitionLevels, values, nullable)
    expect(result).toEqual([[['a', null, 'c']]])
  })

  it('should handle empty lists', () => {
    expect(assembleLists([], [], [], [], nonnullable)).toEqual([])
    expect(assembleLists([], [], [], [], nullable)).toEqual([])
  })

  it('should handle multiple lists', () => {
    const repetitionLevels = [0, 0]
    const values = [22, 33]
    const result = assembleLists([], [], repetitionLevels, values, nonnullable)
    expect(result).toEqual([[22], [33]])
  })

  it('should handle multiple lists (6)', () => {
    const repetitionLevels = [0, 1, 1, 0, 1, 1]
    const values = [1, 2, 3, 4, 5, 6]
    const result = assembleLists([], [], repetitionLevels, values, nonnullable)
    expect(result).toEqual([[1, 2, 3], [4, 5, 6]])
  })

  it('should assemble multiple lists with nulls', () => {
    const definitionLevels = [3, 3, 0, 3, 3]
    const repetitionLevels = [0, 1, 0, 0, 1]
    const values = ['a', 'b', 'd', 'e']
    const result = assembleLists([], definitionLevels, repetitionLevels, values, nullable)
    expect(result).toEqual([[['a', 'b']], [], [['d', 'e']]])
  })

  it('should continue from the previous page', () => {
    const definitionLevels = [3, 3, 3, 1]
    const repetitionLevels = [1, 0, 1, 0]
    const values = ['b', 'c', 'd', 'e']
    const prev = [[['a']]]
    const result = assembleLists(prev, definitionLevels, repetitionLevels, values, nullable)
    expect(result).toEqual([[['a', 'b']], [['c', 'd']], [[]]])
  })

  it('should continue from the previous page (depth 2)', () => {
    const repetitionLevels = [2, 0, 2, 0]
    const values = ['b', 'c', 'd', 'e']
    const prev = [[['a']]]
    const result = assembleLists(prev, [], repetitionLevels, values, nestedRequired)
    expect(result).toEqual([[['a', 'b']], [['c', 'd']], [['e']]])
  })

  it('should handle nested arrays', () => {
    // from nullable.impala.parquet
    const repetitionLevels = [0, 2, 1, 2]
    const values = [1, 2, 3, 4]
    const result = assembleLists([], [], repetitionLevels, values, nestedRequired)
    expect(result).toEqual([[[1, 2], [3, 4]]])
  })

  it('should handle top repetition level', () => {
    // from int_map.parquet
    const definitionLevels = [2, 2, 2, 2, 1, 1, 1, 0, 2, 2]
    const repetitionLevels = [0, 1, 0, 1, 0, 0, 0, 0, 0, 1]
    const values = ['k1', 'k2', 'k1', 'k2', 'k1', 'k3']
    const schemaPath = toSchemaPath(['REQUIRED', 'OPTIONAL', 'REPEATED', 'REQUIRED'])
    const result = assembleLists([], definitionLevels, repetitionLevels, values, schemaPath)
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
    expect(assembleLists([], [0], [0], [], nonnullable)).toEqual([[]])
  })

  it('should handle nonnullable lists', () => {
    // from nonnullable.impala.parquet
    expect(assembleLists([], [1], [0], [-1], nonnullable)).toEqual([[-1]])
  })

  it('should handle nullable int_array', () => {
    // from nullable.impala.parquet int_array
    //                       [1  2  3][N  1  2  N  3  N][ ] N  N
    const definitionLevels = [3, 3, 3, 2, 3, 3, 2, 3, 2, 1, 0, 0]
    const repetitionLevels = [0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0]
    const values = [1, 2, 3, 1, 2, 3]
    const result = assembleLists([], definitionLevels, repetitionLevels, values, nullable)
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
    const result = assembleLists([], definitionLevels, repetitionLevels, values, nestedOptional)
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
    const schemaPath = toSchemaPath([undefined, 'OPTIONAL', 'REPEATED', 'OPTIONAL', 'REPEATED', 'REQUIRED'])
    const result = assembleLists([], definitionLevels, repetitionLevels, values, schemaPath)
    expect(result).toEqual([[[[[]], [['k1']], [[]], [[]]]]])
  })

  it('should handle nonnullable int_map_array values', () => {
    const definitionLevels = [3, 5, 3, 3]
    const repetitionLevels = [0, 1, 1, 1]
    const values = [1]
    const schemaPath = toSchemaPath([undefined, 'OPTIONAL', 'REPEATED', 'OPTIONAL', 'REPEATED', 'OPTIONAL'])
    const result = assembleLists([], definitionLevels, repetitionLevels, values, schemaPath)
    expect(result).toEqual([[[[[]], [[1]], [[]], [[]]]]])
  })

  it('should handle mixed optional and required', () => {
    // from datapage_v2.snappy.parquet e
    const definitionLevels = [2, 2, 2, 0, 0, 2, 2, 2, 2, 2]
    const repetitionLevels = [0, 1, 1, 0, 0, 0, 1, 1, 0, 1]
    const values = [1, 2, 3, 1, 2, 3, 1, 2]
    const schemaPath = toSchemaPath([undefined, 'OPTIONAL', 'REPEATED', 'REQUIRED'])
    const result = assembleLists([], definitionLevels, repetitionLevels, values, schemaPath)
    expect(result).toEqual([[[1, 2, 3]], [], [], [[1, 2, 3]], [[1, 2]]])
  })

  it('should handle nested required', () => {
    // from nonnullable.impala.parquet nested_Struct i
    const definitionLevels = [0]
    const repetitionLevels = [0]
    const schemaPath = toSchemaPath([
      undefined, 'REQUIRED', 'REQUIRED', 'REPEATED', 'REQUIRED', 'REQUIRED', 'REPEATED', 'REQUIRED',
    ])
    const result = assembleLists([], definitionLevels, repetitionLevels, [], schemaPath)
    expect(result).toEqual([[]])
  })

  it('should handle dzenilee', () => {
    const repetitionLevels = [0, 1, 1, 0, 1, 1]
    const values = ['a', 'b', 'c', 'd', 'e', 'f']
    const result = assembleLists([], [], repetitionLevels, values, nullable)
    expect(result).toEqual([[['a', 'b', 'c']], [['d', 'e', 'f']]])
  })

  it('handle complex.parquet with nested require', () => {
    const definitionLevels = [1, 1]
    const values = ['a', 'b']
    const schemaPath = toSchemaPath([undefined, 'OPTIONAL', 'REQUIRED', 'REQUIRED'])
    const result = assembleLists([], definitionLevels, [], values, schemaPath)
    expect(result).toEqual([['a'], ['b']])
  })
})
