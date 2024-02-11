import { describe, expect, it } from 'vitest'
import {
  getMaxDefinitionLevel,
  getMaxRepetitionLevel,
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
    { name: 'root', num_children: 2, repetition_type: 'REQUIRED' },
    { name: 'child1', repetition_type: 'OPTIONAL' },
    { name: 'child2', repetition_type: 'REPEATED' },
  ]

  describe('schemaElement', () => {
    it('should return the correct schema element', () => {
      expect(schemaElement(schema, ['child1'])).toEqual(schema[1])
    })

    it('should throw an error if element not found', () => {
      expect(() => schemaElement(schema, ['nonexistent']))
        .toThrow('parquet schema element not found: nonexistent')
    })
  })

  describe('isRequired', () => {
    it('should return true for required elements', () => {
      expect(isRequired(schema, [])).toBe(true)
    })

    it('should return false for optional or repeated elements', () => {
      expect(isRequired(schema, ['child1'])).toBe(false)
    })
  })

  it('getMaxRepetitionLevel should return the correct max repetition level', () => {
    expect(getMaxRepetitionLevel(schema, ['child2'])).toBe(1)
  })

  it('getMaxDefinitionLevel should return the correct max definition level', () => {
    expect(getMaxDefinitionLevel(schema, ['child1'])).toBe(1)
  })

  it('skipDefinitionBytes should return the correct number of bytes to skip', () => {
    expect(skipDefinitionBytes(100)).toBe(6)
    expect(skipDefinitionBytes(1000)).toBe(7)
  })
})
