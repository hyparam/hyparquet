import { describe, expect, it } from 'vitest'
import {
  getMaxDefinitionLevel,
  getMaxRepetitionLevel,
  getSchemaPath,
  isListLike,
  isMapLike,
  isRequired,
} from '../src/schema.js'

describe('Parquet schema utils', () => {
  /**
   * @typedef {import('../src/types.js').SchemaElement} SchemaElement
   * @type {SchemaElement[]}
   */
  const schema = [
    { name: 'root', num_children: 3 },
    { name: 'child1', repetition_type: 'OPTIONAL' },
    { name: 'child2', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
    { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
    { name: 'element', repetition_type: 'REQUIRED' },
    { name: 'child3', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
    { name: 'map', repetition_type: 'REPEATED', num_children: 2 },
    { name: 'key', repetition_type: 'REQUIRED' },
    { name: 'value', repetition_type: 'OPTIONAL' },
  ]

  describe('getSchemaPath', () => {
    it('should return the schema path', () => {
      const path = getSchemaPath(schema, ['child1'])
      expect(path[path.length - 1]).toEqual({
        children: [],
        count: 1,
        element: { name: 'child1', repetition_type: 'OPTIONAL' },
      })
    })

    it('should throw an error if element not found', () => {
      expect(() => getSchemaPath(schema, ['nonexistent']))
        .toThrow('parquet schema element not found: nonexistent')
    })
  })

  it('isRequired', () => {
    expect(isRequired(getSchemaPath(schema, []))).toBe(true)
    expect(isRequired(getSchemaPath(schema, ['child1']))).toBe(false)
    expect(isRequired(getSchemaPath(schema, ['child2']))).toBe(false)
    expect(isRequired(getSchemaPath(schema, ['child3']))).toBe(false)
  })

  it('getMaxRepetitionLevel', () => {
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['child1']))).toBe(0)
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['child2']))).toBe(0)
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['child2', 'list', 'element']))).toBe(1)
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['child3']))).toBe(0)
    expect(getMaxRepetitionLevel(getSchemaPath(schema, ['child3', 'map', 'key']))).toBe(1)
  })

  it('getMaxDefinitionLevel', () => {
    expect(getMaxDefinitionLevel(getSchemaPath(schema, ['child1']))).toBe(1)
    expect(getMaxDefinitionLevel(getSchemaPath(schema, ['child2']))).toBe(1)
    expect(getMaxDefinitionLevel(getSchemaPath(schema, ['child3']))).toBe(1)
  })

  it('isListLike', () => {
    expect(isListLike(getSchemaPath(schema, []))).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['child1']))).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['child2']))).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['child2', 'list', 'element']))).toBe(true)
    expect(isListLike(getSchemaPath(schema, ['child3']))).toBe(false)
    expect(isListLike(getSchemaPath(schema, ['child3', 'map', 'key']))).toBe(false)
  })

  it('isMapLike', () => {
    expect(isMapLike(getSchemaPath(schema, []))).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['child1']))).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['child2']))).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['child2', 'list', 'element']))).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['child3']))).toBe(false)
    expect(isMapLike(getSchemaPath(schema, ['child3', 'map', 'key']))).toBe(true)
    expect(isMapLike(getSchemaPath(schema, ['child3', 'map', 'value']))).toBe(true)
  })
})
