import { describe, expect, it } from 'vitest'
import { assembleColumn } from '../src/assemble.js'
import { DEFAULT_PARSERS } from '../src/convert.js'
import { parquetSchema } from '../src/metadata.js'
import { getMaxDefinitionLevel } from '../src/schema.js'

/** @import {AsyncPages, FieldRepetitionType, SchemaElement, SchemaTree} from '../src/types.js' */

/**
 * @param {number[]} definitionLevels
 * @param {number[]} repetitionLevels
 * @param {any[][]} data pages of non-null values
 * @param {number} [skipped]
 * @returns {AsyncPages}
 */
function column(definitionLevels, repetitionLevels, data, skipped = 0) {
  return {
    skipped,
    data,
    definitionLevels: Uint8Array.from(definitionLevels),
    repetitionLevels: Uint8Array.from(repetitionLevels),
  }
}

describe('assembleColumn lists of a leaf', () => {
  /** @type {FieldRepetitionType[]} */
  const nonnullable = ['REQUIRED', 'REPEATED', 'REQUIRED']
  /** @type {FieldRepetitionType[]} */
  const nullable = ['OPTIONAL', 'REPEATED', 'OPTIONAL']
  /** @type {FieldRepetitionType[]} */
  const nestedRequired = ['REQUIRED', 'REPEATED', 'REQUIRED', 'REPEATED', 'REQUIRED']
  /** @type {FieldRepetitionType[]} */
  const nestedOptional = ['OPTIONAL', 'REPEATED', 'OPTIONAL', 'REPEATED', 'OPTIONAL']

  /**
   * Assemble the lists of a single leaf with the given repetition types.
   *
   * @param {FieldRepetitionType[]} repetitionPath
   * @param {number[] | undefined} definitionLevels defaults to all defined
   * @param {number[]} repetitionLevels
   * @param {any[][]} data
   * @returns {any[]}
   */
  function assembleLeaf(repetitionPath, definitionLevels, repetitionLevels, ...data) {
    /** @type {SchemaTree[]} */
    const schemaPath = [{ element: { name: 'root' }, count: 1, children: [], path: [] }]
    repetitionPath.forEach((repetition_type, i) => {
      /** @type {SchemaTree} */
      const node = {
        element: { name: 'name', repetition_type },
        count: 1,
        children: [],
        path: new Array(i + 1).fill('name'),
      }
      schemaPath[i].children.push(node)
      schemaPath.push(node)
    })
    const path = schemaPath[schemaPath.length - 1].path.join('.')
    definitionLevels ??= new Array(repetitionLevels.length).fill(getMaxDefinitionLevel(schemaPath))
    const leaf = column(definitionLevels, repetitionLevels, data)
    const numRows = repetitionLevels.length
      ? repetitionLevels.filter(rep => !rep).length
      : definitionLevels.length
    return assembleColumn(schemaPath[1], new Map([[path, leaf]]), DEFAULT_PARSERS, 0, numRows, schemaPath)
  }

  it('should assemble objects with non-null values', () => {
    expect(assembleLeaf(nonnullable, undefined, [0, 1], ['a', 'b'])).toEqual([['a', 'b']])
  })

  it('should handle null values', () => {
    expect(assembleLeaf(nullable, [3, 2, 3], [0, 1, 1], ['a', 'c'])).toEqual([['a', null, 'c']])
  })

  it('should handle empty columns', () => {
    expect(assembleLeaf(nonnullable, [], [])).toEqual([])
    expect(assembleLeaf(nullable, [], [])).toEqual([])
  })

  it('should handle multiple lists', () => {
    expect(assembleLeaf(nonnullable, undefined, [0, 0], [22, 33])).toEqual([[22], [33]])
  })

  it('should handle multiple lists (6)', () => {
    expect(assembleLeaf(nonnullable, undefined, [0, 1, 1, 0, 1, 1], [1, 2, 3, 4, 5, 6]))
      .toEqual([[1, 2, 3], [4, 5, 6]])
  })

  it('should assemble multiple lists with nulls', () => {
    expect(assembleLeaf(nullable, [3, 3, 0, 3, 3], [0, 1, 0, 0, 1], ['a', 'b', 'd', 'e']))
      .toEqual([['a', 'b'], null, ['d', 'e']])
  })

  it('should continue a row from the previous page', () => {
    expect(assembleLeaf(nullable, [3, 3, 3, 3, 1], [0, 1, 0, 1, 0], ['a'], ['b', 'c', 'd']))
      .toEqual([['a', 'b'], ['c', 'd'], []])
  })

  it('should continue a row from the previous page (depth 2)', () => {
    expect(assembleLeaf(nestedRequired, undefined, [0, 2, 0, 2, 0], ['a'], [], ['b', 'c', 'd', 'e']))
      .toEqual([[['a', 'b']], [['c', 'd']], [['e']]])
  })

  it('should handle nested arrays', () => {
    // from nullable.impala.parquet
    expect(assembleLeaf(nestedRequired, undefined, [0, 2, 1, 2], [1, 2, 3, 4])).toEqual([[[1, 2], [3, 4]]])
  })

  it('should handle top repetition level', () => {
    // from int_map.parquet
    const definitionLevels = [2, 2, 2, 2, 1, 1, 1, 0, 2, 2]
    const repetitionLevels = [0, 1, 0, 1, 0, 0, 0, 0, 0, 1]
    const values = ['k1', 'k2', 'k1', 'k2', 'k1', 'k3']
    expect(assembleLeaf(['OPTIONAL', 'REPEATED', 'REQUIRED'], definitionLevels, repetitionLevels, values))
      .toEqual([['k1', 'k2'], ['k1', 'k2'], [], [], [], null, ['k1', 'k3']])
  })

  it('should handle empty lists with definition level', () => {
    // from nonnullable.impala.parquet
    expect(assembleLeaf(nonnullable, [0], [0], [])).toEqual([[]])
  })

  it('should handle nonnullable lists', () => {
    // from nonnullable.impala.parquet
    expect(assembleLeaf(nonnullable, [1], [0], [-1])).toEqual([[-1]])
  })

  it('should handle nullable int_array', () => {
    // from nullable.impala.parquet int_array
    //                       [1  2  3][N  1  2  N  3  N][ ] N  N
    const definitionLevels = [3, 3, 3, 2, 3, 3, 2, 3, 2, 1, 0, 0]
    const repetitionLevels = [0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0]
    expect(assembleLeaf(nullable, definitionLevels, repetitionLevels, [1, 2, 3, 1, 2, 3])).toEqual([
      [1, 2, 3],
      [null, 1, 2, null, 3, null],
      [],
      null,
      null,
    ])
  })

  it('should handle nullable int_array_Array', () => {
    // from nullable.impala.parquet int_array_Array
    //                       [1  2][3  4][[N 1  2  N][3  N  4] [] N][N] [] N  N [N  5  6]
    const definitionLevels = [5, 5, 5, 5, 4, 5, 5, 4, 5, 4, 5, 3, 2, 2, 1, 0, 0, 2, 5, 5]
    const repetitionLevels = [0, 2, 1, 2, 0, 2, 2, 2, 1, 2, 2, 1, 1, 0, 0, 0, 0, 0, 1, 2]
    const values = [1, 2, 3, 4, 1, 2, 3, 4, 5, 6]
    expect(assembleLeaf(nestedOptional, definitionLevels, repetitionLevels, values)).toEqual([
      [[1, 2], [3, 4]],
      [[null, 1, 2, null], [3, null, 4], [], null],
      [null],
      [],
      null,
      null,
      [null, [5, 6]],
    ])
  })

  it('should handle nonnullable int_map_array keys', () => {
    /** @type {FieldRepetitionType[]} */
    const repetitionPath = ['OPTIONAL', 'REPEATED', 'OPTIONAL', 'REPEATED', 'REQUIRED']
    expect(assembleLeaf(repetitionPath, [3, 4, 3, 3], [0, 1, 1, 1], ['k1'])).toEqual([[[], ['k1'], [], []]])
  })

  it('should handle nonnullable int_map_array values', () => {
    /** @type {FieldRepetitionType[]} */
    const repetitionPath = ['OPTIONAL', 'REPEATED', 'OPTIONAL', 'REPEATED', 'OPTIONAL']
    expect(assembleLeaf(repetitionPath, [3, 5, 3, 3], [0, 1, 1, 1], [1])).toEqual([[[], [1], [], []]])
  })

  it('should handle mixed optional and required', () => {
    // from datapage_v2.snappy.parquet e
    const definitionLevels = [2, 2, 2, 0, 0, 2, 2, 2, 2, 2]
    const repetitionLevels = [0, 1, 1, 0, 0, 0, 1, 1, 0, 1]
    const values = [1, 2, 3, 1, 2, 3, 1, 2]
    expect(assembleLeaf(['OPTIONAL', 'REPEATED', 'REQUIRED'], definitionLevels, repetitionLevels, values))
      .toEqual([[1, 2, 3], null, null, [1, 2, 3], [1, 2]])
  })

  it('should handle nested required', () => {
    // from nonnullable.impala.parquet nested_Struct i
    /** @type {FieldRepetitionType[]} */
    const repetitionPath = ['REQUIRED', 'REQUIRED', 'REPEATED', 'REQUIRED', 'REQUIRED', 'REPEATED', 'REQUIRED']
    expect(assembleLeaf(repetitionPath, [0], [0], [])).toEqual([[]])
  })

  it('should handle dzenilee', () => {
    expect(assembleLeaf(nullable, undefined, [0, 1, 1, 0, 1, 1], ['a', 'b', 'c', 'd', 'e', 'f']))
      .toEqual([['a', 'b', 'c'], ['d', 'e', 'f']])
  })

  it('handle complex.parquet with nested require', () => {
    expect(assembleLeaf(['OPTIONAL', 'REQUIRED', 'REQUIRED'], [1, 1], [], ['a', 'b'])).toEqual(['a', 'b'])
  })
})

describe('assembleColumn', () => {
  /**
   * @param {SchemaElement[]} schema
   * @param {Record<string, AsyncPages>} columns
   * @param {number} rowStart
   * @param {number} rowEnd
   * @returns {any[]}
   */
  function assemble(schema, columns, rowStart, rowEnd) {
    const tree = parquetSchema({ schema: [{ name: 'root', num_children: 1 }, ...schema] })
    return assembleColumn(tree.children[0], new Map(Object.entries(columns)), DEFAULT_PARSERS, rowStart, rowEnd)
  }

  /** @type {SchemaElement[]} */
  const struct = [
    { name: 's', repetition_type: 'OPTIONAL', num_children: 2 },
    { name: 'a', repetition_type: 'OPTIONAL', type: 'INT32' },
    { name: 'b', repetition_type: 'REQUIRED', type: 'INT32' },
  ]

  it('distinguishes a null struct from a struct of nulls', () => {
    expect(assemble(struct, {
      's.a': column([2, 1, 0], [], [[1]]),
      's.b': column([1, 1, 0], [], [[10, 20]]),
    }, 0, 3)).toEqual([{ a: 1, b: 10 }, { a: null, b: 20 }, null])
  })

  it('aligns leaves that skipped different rows and spans pages', () => {
    expect(assemble(struct, {
      's.a': column([2, 0, 2, 1, 2], [], [[1, 3], [5]], 1),
      's.b': column([1, 1, 1], [], [[], [30], [40, 50]], 3),
    }, 3, 6)).toEqual([{ a: 3, b: 30 }, { a: null, b: 40 }, { a: 5, b: 50 }])
  })

  it('assembles lists of structs with nested lists', () => {
    // [{a: 1, b: [1, 2]}, {a: null, b: []}], null, [], [{a: 3, b: null}]
    expect(assemble([
      { name: 'l', repetition_type: 'OPTIONAL', converted_type: 'LIST', num_children: 1 },
      { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
      { name: 'element', repetition_type: 'REQUIRED', num_children: 2 },
      { name: 'a', repetition_type: 'OPTIONAL', type: 'INT32' },
      { name: 'b', repetition_type: 'OPTIONAL', converted_type: 'LIST', num_children: 1 },
      { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
      { name: 'element', repetition_type: 'REQUIRED', type: 'INT32' },
    ], {
      'l.list.element.a': column([3, 2, 0, 1, 3], [0, 1, 0, 0, 0], [[1, 3]]),
      'l.list.element.b.list.element': column([4, 4, 3, 0, 1, 2], [0, 2, 1, 0, 0, 0], [[1, 2]]),
    }, 0, 4)).toEqual([
      [{ a: 1, b: [1, 2] }, { a: null, b: [] }],
      null,
      [],
      [{ a: 3, b: null }],
    ])
  })

  it('assembles maps', () => {
    expect(assemble([
      { name: 'm', repetition_type: 'OPTIONAL', converted_type: 'MAP', num_children: 1 },
      { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
      { name: 'key', repetition_type: 'REQUIRED', type: 'BYTE_ARRAY' },
      { name: 'value', repetition_type: 'OPTIONAL', type: 'INT32' },
    ], {
      'm.key_value.key': column([2, 2, 1, 0], [0, 1, 0, 0], [['a', 'b']]),
      'm.key_value.value': column([3, 2, 1, 0], [0, 1, 0, 0], [[1]]),
    }, 0, 3)).toEqual([{ a: 1, b: null }, {}, null])
  })

  it('assembles repeated fields without a list annotation', () => {
    expect(assemble([
      { name: 's', repetition_type: 'REQUIRED', num_children: 1 },
      { name: 'r', repetition_type: 'REPEATED', type: 'INT32' },
    ], { 's.r': column([1, 1, 0], [0, 1, 0], [[1, 2]]) }, 0, 2)).toEqual([{ r: [1, 2] }, { r: [] }])
  })

  it('throws if a leaf column is missing', () => {
    expect(() => assemble(struct, { 's.a': column([], [], []) }, 0, 0))
      .toThrow('parquet column missing levels: s.b')
  })
})
