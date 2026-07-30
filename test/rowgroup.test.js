import { describe, expect, it } from 'vitest'
import { assembleAsync, asyncGroupToRows } from '../src/rowgroup.js'

/** @import {SchemaTree} from '../src/types.js' */

describe('assembleAsync', () => {
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
