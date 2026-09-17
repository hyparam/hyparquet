import { describe, expect, it } from 'vitest'
import { assembleAsync, asyncGroupToRows } from '../src/rowgroup.js'
import { parquetSchema } from '../src/metadata.js'

/** @import {SchemaTree} from '../src/types.js' */

describe('assembleAsync', () => {
  it('slices physical children before decoding nested variants', async () => {
    const schemaTree = parquetSchema({ schema: [
      { name: 'schema', num_children: 1 },
      { name: 'payload', num_children: 2, repetition_type: 'REQUIRED', logical_type: { type: 'VARIANT' } },
      { name: 'metadata', type: 'BYTE_ARRAY', repetition_type: 'REQUIRED' },
      { name: 'value', type: 'BYTE_ARRAY', repetition_type: 'REQUIRED' },
    ] })
    const valid = Uint8Array.from([0x11, 0x00, 0x00])
    // These unselected values must never reach the VARIANT decoder. This
    // also catches an implementation that decodes everything and slices later.
    const invalid = Uint8Array.from([0xff])
    const hi = Uint8Array.from([0x09, 0x68, 0x69])
    const group = {
      groupStart: 100,
      groupRows: 5,
      selectStart: 2,
      selectEnd: 3,
      asyncColumns: [
        { pathInSchema: ['payload', 'metadata'], data: Promise.resolve({ skipped: 1, data: [[invalid, valid, invalid, invalid]] }) },
        { pathInSchema: ['payload', 'value'], data: Promise.resolve({ skipped: 0, data: [[hi, hi, hi, hi, hi]] }) },
      ],
    }
    const assembled = assembleAsync(group, schemaTree)
    const column = await assembled.asyncColumns[0].data
    expect(column).toEqual({ skipped: 2, data: [['hi']] })
    expect(await asyncGroupToRows(assembled, 2, 3, undefined, 'object'))
      .toEqual([{ payload: 'hi' }])
  })

  it('aligns nested child columns and preserves their skipped row offset', async () => {
    /** @type {SchemaTree} */
    const schemaTree = {
      count: 4,
      element: { name: 'schema', num_children: 1, repetition_type: 'REQUIRED' },
      path: [],
      children: [{
        count: 3,
        element: { name: 'details', num_children: 2, repetition_type: 'REQUIRED' },
        path: ['details'],
        children: [
          {
            count: 1,
            element: { name: 'left', repetition_type: 'REQUIRED', type: 'INT32' },
            path: ['details', 'left'],
            children: [],
          },
          {
            count: 1,
            element: { name: 'right', repetition_type: 'REQUIRED', type: 'INT32' },
            path: ['details', 'right'],
            children: [],
          },
        ],
      }],
    }
    const asyncRowGroup = {
      groupStart: 0,
      groupRows: 5,
      selectStart: 2,
      selectEnd: 5,
      asyncColumns: [
        {
          pathInSchema: ['details', 'left'],
          data: Promise.resolve({ skipped: 2, data: [[20, 30, 40]] }),
        },
        {
          pathInSchema: ['details', 'right'],
          data: Promise.resolve({ skipped: 1, data: [[10, 20, 30, 40]] }),
        },
      ],
    }

    const assembled = assembleAsync(asyncRowGroup, schemaTree)
    const rows = await asyncGroupToRows(assembled, 2, 5, undefined, 'object')

    expect(rows).toEqual([
      { details: { left: 20, right: 20 } },
      { details: { left: 30, right: 30 } },
      { details: { left: 40, right: 40 } },
    ])
  })
})
