import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { assembleNested } from '../src/assemble.js'
import { DEFAULT_PARSERS } from '../src/convert.js'
import { parquetReadObjects } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'

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

  assembleNested(subcolumnData, variantSchema, DEFAULT_PARSERS)
  return subcolumnData.get('variant')?.[0]
}

describe('variant decoding', () => {
  it('decodes object variants with dictionary metadata', () => {
    const metadata = Uint8Array.from([0x11, 0x01, 0x00, 0x03, 0x66, 0x6f, 0x6f])
    const value = Uint8Array.from([0x02, 0x01, 0x00, 0x00, 0x05, 0x14, 0x2a, 0x00, 0x00, 0x00])

    const subcolumnData = new Map()
    subcolumnData.set('variant.metadata', [metadata])
    subcolumnData.set('variant.value', [value])

    assembleNested(subcolumnData, variantSchema, DEFAULT_PARSERS)

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

    assembleNested(subcolumnData, variantSchema, DEFAULT_PARSERS)

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

  it('decodes object_primitive', () => {
    const result = decodeFixture('object_primitive')
    expect(result).toHaveProperty('int_field', 1)
    expect(result).toHaveProperty('double_field')
    expect(result).toHaveProperty('boolean_true_field', true)
    expect(result).toHaveProperty('boolean_false_field', false)
    expect(result).toHaveProperty('string_field', 'Apache Parquet')
    expect(result).toHaveProperty('null_field', null)
    expect(result).toHaveProperty('timestamp_field')
  })

  it('decodes object_nested', () => {
    const result = decodeFixture('object_nested')
    expect(result).toHaveProperty('id', 1)
    expect(result).toHaveProperty('species')
    expect(result.species).toHaveProperty('name', 'lava monster')
    expect(result.species).toHaveProperty('population', 6789)
    expect(result).toHaveProperty('observation')
    expect(result.observation).toHaveProperty('location', 'In the Volcano')
  })
})

describe('shredded variant', () => {
  it('reads shredded boolean variant', async () => {
    const file = await asyncBufferFromFile('test/files/shredded-bool.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows.length).toBe(1)
    expect(rows[0].var).toBe(true)
  })

  it('reads shredded int32 variant', async () => {
    const file = await asyncBufferFromFile('test/files/shredded-int.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows.length).toBe(1)
    expect(rows[0].var).toBe(12345)
  })

  it('reads shredded string array variant', async () => {
    const file = await asyncBufferFromFile('test/files/shredded-array.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows.length).toBe(1)
    expect(rows[0].var).toEqual(['comedy', 'drama'])
  })

  it('reads shredded object variant', async () => {
    // case-046: testShreddedObject
    const file = await asyncBufferFromFile('test/files/shredded-object.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows.length).toBe(1)
    expect(rows[0].var).toEqual({ a: null, b: '' })
  })

  it('reads nested shredded object variant', async () => {
    // case-044: testShreddedObjectWithinShreddedObject
    const file = await asyncBufferFromFile('test/files/shredded-nested-object.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows.length).toBe(1)
    expect(rows[0].var).toEqual({
      c: { a: 34, b: 'iceberg' },
      d: -0,
    })
  })

  it('reads partially shredded object variant', async () => {
    // case-134: testPartiallyShreddedObject
    const file = await asyncBufferFromFile('test/files/shredded-partial-object.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows.length).toBe(1)
    expect(rows[0].var.a).toBe(null)
    expect(rows[0].var.b).toBe('iceberg')
    expect(rows[0].var.d).toBeInstanceOf(Date)
    expect(rows[0].var.d.getTime()).toBe(new Date('2024-01-30').getTime())
  })
})
