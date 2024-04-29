import { describe, expect, it } from 'vitest'
import {
  getMaxDefinitionLevel,
  getMaxRepetitionLevel,
  isListLike,
  isMapLike,
  isRequired,
  schemaElement,
  skipDefinitionBytes,
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

  describe('schemaElement', () => {
    it('should return the schema element', () => {
      expect(schemaElement(schema, ['child1'])).toEqual({
        children: [],
        count: 1,
        element: { name: 'child1', repetition_type: 'OPTIONAL' },
      })
    })

    it('should throw an error if element not found', () => {
      expect(() => schemaElement(schema, ['nonexistent']))
        .toThrow('parquet schema element not found: nonexistent')
    })
  })

  it('isRequired', () => {
    expect(isRequired(schema, [])).toBe(true)
    expect(isRequired(schema, ['child1'])).toBe(false)
    expect(isRequired(schema, ['child2'])).toBe(false)
    expect(isRequired(schema, ['child3'])).toBe(false)
  })

  it('getMaxRepetitionLevel', () => {
    expect(getMaxRepetitionLevel(schema, ['child1'])).toBe(0)
    expect(getMaxRepetitionLevel(schema, ['child2'])).toBe(0)
    expect(getMaxRepetitionLevel(schema, ['child2', 'list', 'element'])).toBe(1)
    expect(getMaxRepetitionLevel(schema, ['child3'])).toBe(0)
    expect(getMaxRepetitionLevel(schema, ['child3', 'map', 'key'])).toBe(1)
  })

  it('getMaxDefinitionLevel', () => {
    expect(getMaxDefinitionLevel(schema, ['child1'])).toBe(1)
    expect(getMaxDefinitionLevel(schema, ['child2'])).toBe(1)
    expect(getMaxDefinitionLevel(schema, ['child3'])).toBe(1)
  })

  it('skipDefinitionBytes', () => {
    expect(skipDefinitionBytes(100)).toBe(6)
    expect(skipDefinitionBytes(1000)).toBe(7)
  })

  it('isListLike', () => {
    expect(isListLike(schema, [])).toBe(false)
    expect(isListLike(schema, ['child1'])).toBe(false)
    expect(isListLike(schema, ['child2'])).toBe(false)
    expect(isListLike(schema, ['child2', 'list', 'element'])).toBe(true)
    expect(isListLike(schema, ['child3'])).toBe(false)
    expect(isListLike(schema, ['child3', 'map', 'key'])).toBe(false)
  })

  it('isMapLike', () => {
    expect(isMapLike(schema, [])).toBe(false)
    expect(isMapLike(schema, ['child1'])).toBe(false)
    expect(isMapLike(schema, ['child2'])).toBe(false)
    expect(isMapLike(schema, ['child2', 'list', 'element'])).toBe(false)
    expect(isMapLike(schema, ['child3'])).toBe(false)
    expect(isMapLike(schema, ['child3', 'map', 'key'])).toBe(true)
    expect(isMapLike(schema, ['child3', 'map', 'value'])).toBe(true)
  })
})
