import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { assembleNested } from '../src/assemble.js'
import { DEFAULT_PARSERS } from '../src/convert.js'

/** @import {SchemaTree} from '../src/types.d.ts' */

/** @type {SchemaTree} */
const variantMetadataSchema = {
  path: ['variant', 'metadata'],
  element: {
    name: 'metadata',
    type: 'BYTE_ARRAY',
    repetition_type: 'REQUIRED',
  },
  children: [],
  count: 1,
}

/** @type {SchemaTree} */
const variantValueSchema = {
  path: ['variant', 'value'],
  element: {
    name: 'value',
    type: 'BYTE_ARRAY',
    repetition_type: 'REQUIRED',
  },
  children: [],
  count: 1,
}

/** @type {SchemaTree} */
const variantSchema = {
  path: ['variant'],
  element: {
    name: 'variant',
    repetition_type: 'REQUIRED',
    logical_type: { type: 'VARIANT' },
    num_children: 2,
  },
  children: [variantMetadataSchema, variantValueSchema],
  count: 3,
}

/**
 * Load and decode a variant from test fixture files.
 *
 * @param {string} name base name of the fixture (without extension)
 * @returns {any} decoded variant value
 */
function decodeFixture(name) {
  const metadataBytes = new Uint8Array(fs.readFileSync(`test/variant/${name}.metadata`))
  const valueBytes = new Uint8Array(fs.readFileSync(`test/variant/${name}.value`))

  const subcolumnData = new Map()
  subcolumnData.set('variant.metadata', [metadataBytes])
  subcolumnData.set('variant.value', [valueBytes])

  assembleNested(subcolumnData, variantSchema, 0, DEFAULT_PARSERS)
  return subcolumnData.get('variant')?.[0]
}

describe('variant decoding', () => {
  it('decodes object variants with dictionary metadata', () => {
    const metadata = Uint8Array.from([0x11, 0x01, 0x00, 0x03, 0x66, 0x6f, 0x6f])
    const value = Uint8Array.from([0x02, 0x01, 0x00, 0x00, 0x05, 0x14, 0x2a, 0x00, 0x00, 0x00])

    const subcolumnData = new Map()
    subcolumnData.set('variant.metadata', [metadata])
    subcolumnData.set('variant.value', [value])

    assembleNested(subcolumnData, variantSchema, 0, DEFAULT_PARSERS)

    expect(subcolumnData.has('variant.metadata')).toBe(false)
    expect(subcolumnData.has('variant.value')).toBe(false)
    expect(subcolumnData.get('variant')).toEqual([{ foo: 42 }])
  })

  it('decodes primitive and array variants', () => {
    const metadata = Uint8Array.from([0x11, 0x00, 0x00])
    const stringValue = Uint8Array.from([0x09, 0x68, 0x69])
    const arrayValue = Uint8Array.from([
      0x03,
      0x02,
      0x00,
      0x05,
      0x09,
      0x14,
      0x01,
      0x00,
      0x00,
      0x00,
      0x0d,
      0x66,
      0x6f,
      0x6f,
    ])

    const subcolumnData = new Map()
    subcolumnData.set('variant.metadata', [metadata, Uint8Array.from(metadata)])
    subcolumnData.set('variant.value', [stringValue, arrayValue])

    assembleNested(subcolumnData, variantSchema, 0, DEFAULT_PARSERS)

    expect(subcolumnData.get('variant')).toEqual(['hi', [1, 'foo']])
  })
})

describe('variant binary encoding', () => {
  it('decodes primitive_null', () => {
    expect(decodeFixture('primitive_null')).toBe(null)
  })

  it('decodes primitive_int32', () => {
    expect(decodeFixture('primitive_int32')).toBe(123456)
  })

  it('decodes primitive_time as microseconds', () => {
    // TIME type returns BigInt microseconds since midnight
    // 12:33:54.123456 = 45234123456 microseconds
    expect(decodeFixture('primitive_time')).toBe(45234123456n)
  })

  it('decodes primitive_string (long UTF-8 with emojis)', () => {
    const result = decodeFixture('primitive_string')
    expect(result).toContain('This string is longer than 64 bytes')
    expect(result).toContain('\ud83d\udc22') // turtle emoji
  })

  it('decodes short_string', () => {
    expect(decodeFixture('short_string')).toBe('Less than 64 bytes (\u2764\ufe0f with utf8)')
  })

  it('decodes array_primitive', () => {
    expect(decodeFixture('array_primitive')).toEqual([2, 1, 5, 9])
  })

  // TODO: object_primitive test - the fixture uses a different binary format
  // that needs investigation (offsets are not monotonically increasing)
})
