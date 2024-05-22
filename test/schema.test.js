import { describe, expect, it } from 'vitest'
import {
  getMaxDefinitionLevel,
  getMaxRepetitionLevel,
  getSchemaPath,
  isListLike,
  isMapLike,
} from '../src/schema.js'

describe('Parquet schema utils', () => {
  /**
   * @typedef {import('../src/types.js').SchemaElement} SchemaElement
   * @type {SchemaElement[]}
   */
  const schema = [
    { name: 'root', num_children: 7 },
    { name: 'flat', repetition_type: 'OPTIONAL' },
    { name: 'listy', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
    { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
    { name: 'element', repetition_type: 'REQUIRED' },
    { name: 'mappy', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
    { name: 'map', repetition_type: 'REPEATED', num_children: 2 },
    { name: 'key', repetition_type: 'REQUIRED' },
    { name: 'value', repetition_type: 'OPTIONAL' },
    { name: 'invalid_list', repetition_type: 'OPTIONAL', num_children: 2, converted_type: 'LIST' },
    { name: 'list1', repetition_type: 'REPEATED' },
    { name: 'list2', repetition_type: 'REPEATED' },
    { name: 'structy', repetition_type: 'OPTIONAL', num_children: 2, converted_type: 'LIST' },
    { name: 'element1', repetition_type: 'REQUIRED' },
    { name: 'element2', repetition_type: 'REQUIRED' },
    { name: 'list_structy', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
    { name: 'list', repetition_type: 'REPEATED', num_children: 2 },
    { name: 'element1', repetition_type: 'REQUIRED' },
    { name: 'element2', repetition_type: 'REQUIRED' },
    { name: 'invalid_list', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
    { name: 'list', repetition_type: 'OPTIONAL', num_children: 1 },
    { name: 'element', repetition_type: 'OPTIONAL' },
  ]

  describe('getSchemaPath', () => {
    it('return the root schema path', () => {
      const root = getSchemaPath(schema, []).at(-1)
      expect(root?.children.length).toEqual(7)
      expect(root).containSubset({
        count: 22,
        element: { name: 'root', num_children: 7 },
        path: [],
      })
    })

    it('return the schema path', () => {
      expect(getSchemaPath(schema, ['flat']).at(-1)).toEqual({
        children: [],
        count: 1,
        element: { name: 'flat', repetition_type: 'OPTIONAL' },
        path: ['flat'],
      })
    })

    it('throw an error if element not found', () => {
      expect(() => getSchemaPath(schema, ['nonexistent']))
        .toThrow('parquet schema element not found: nonexistent')
    })
  })

  it('getMaxRepetitionLevel', () => {
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['flat']))).toBe(0)
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['listy']))).toBe(0)
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['listy', 'list', 'element']))).toBe(1)
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['mappy']))).toBe(0)
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['mappy', 'map', 'key']))).toBe(1)
  })

  it('getMaxDefinitionLevel', () => {
    expect(getMaxDefinitionLevel(getSchemaPath(schema, ['flat']))).toBe(1)
    expect(getMaxDefinitionLevel(getSchemaPath(schema, ['listy']))).toBe(1)
    expect(getMaxDefinitionLevel(getSchemaPath(schema, ['mappy']))).toBe(1)
  })

  it('isListLike', () => {
    expect(isListLike(getSchemaPath(schema, [])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['flat'])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['listy'])[1])).toBe(true)
    expect(isListLike(getSchemaPath(schema, ['listy', 'list', 'element'])[1])).toBe(true)
    expect(isListLike(getSchemaPath(schema, ['mappy'])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['mappy', 'map', 'key'])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['invalid_list'])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['invalid_list', 'list1'])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['invalid_list', 'list2'])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['structy'])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['list_structy'])[1])).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['invalid_list'])[1])).toBe(false)
  })

  it('isMapLike', () => {
    expect(isMapLike(getSchemaPath(schema, [])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['flat'])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['listy'])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['listy', 'list', 'element'])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['mappy'])[1])).toBe(true)
    expect(isMapLike(getSchemaPath(schema, ['mappy', 'map', 'key'])[1])).toBe(true)
    expect(isMapLike(getSchemaPath(schema, ['mappy', 'map', 'value'])[1])).toBe(true)
    expect(isMapLike(getSchemaPath(schema, ['invalid_list'])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['invalid_list', 'list1'])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['invalid_list', 'list2'])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['structy'])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['list_structy'])[1])).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['invalid_list'])[1])).toBe(false)
  })
})
