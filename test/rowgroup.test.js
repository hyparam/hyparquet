import { describe, expect, it, vi } from 'vitest'
import { assembleAsync, asyncGroupToRows, readRowGroup } from '../src/rowgroup.js'
import { parquetSchema } from '../src/metadata.js'
import { parquetMetadataAsync, parquetRead } from '../src/index.js'

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

describe('readRowGroup synchronous failure cleanup', () => {
  /** @type {import('../src/types.js').FileMetaData} */
  const metadata = {
    version: 1,
    num_rows: 1n,
    row_groups: [],
    metadata_length: 0,
    schema: [
      { name: 'schema', num_children: 2 },
      { name: 'first', type: 'INT32', repetition_type: 'OPTIONAL' },
      { name: 'second', type: 'INT32', repetition_type: 'OPTIONAL' },
    ],
  }
  /**
   * @param {string} name
   * @param {number} startByte
   */
  const makeChunk = (name, startByte) => ({
    columnMetadata: { path_in_schema: [name] },
    range: { startByte, endByte: startByte + 1 },
  })
  /**
   * @param {ReturnType<typeof makeChunk>[]} chunks
   * @returns {import('../src/types.js').QueryPlan['groups'][number]}
   */
  const makePlan = chunks => /** @type {import('../src/types.js').QueryPlan['groups'][number]} */ (
    /** @type {unknown} */ ({
      chunks,
      groupStart: 0,
      groupRows: 1,
      selectStart: 0,
      selectEnd: 1,
    })
  )

  it('validates every schema path before issuing any column reads', () => {
    const slice = vi.fn(() => Promise.reject(new Error('should never be fetched')))
    const plan = makePlan([makeChunk('first', 0), makeChunk('missing', 1)])

    expect(() => readRowGroup({ file: { byteLength: 2, slice } }, { metadata }, plan))
      .toThrow('parquet schema element not found: missing')
    expect(slice).not.toHaveBeenCalled()
  })

  it('observes earlier rejected reads when a later slice throws synchronously', async () => {
    const slice = vi.fn((/** @type {number} */ start) => {
      if (start === 0) return Promise.reject(new Error('earlier read failed'))
      throw new Error('synchronous slice failure')
    })
    /** @type {unknown[]} */
    const unhandled = []
    /** @param {unknown} reason */
    const onUnhandled = reason => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      const plan = makePlan([makeChunk('first', 0), makeChunk('second', 1)])
      expect(() => readRowGroup({ file: { byteLength: 2, slice } }, { metadata }, plan))
        .toThrow('synchronous slice failure')
      expect(slice).toHaveBeenCalledTimes(2)
      await new Promise(resolve => setTimeout(resolve, 50))
      expect(unhandled).toEqual([])
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })

  it('does not consume failures that callers need to observe', async () => {
    const failure = new Error('decode failure')
    const slice = vi.fn(() => Promise.reject(failure))
    const plan = makePlan([makeChunk('first', 0)])
    const result = readRowGroup({ file: { byteLength: 2, slice } }, { metadata }, plan)
    await expect(result.asyncColumns[0].data).rejects.toBe(failure)
  })
})

describe('corrupt Parquet file with mismatched column schema (issue #197)', () => {
  it('rejects the original failing file without an unhandled read error', async () => {
    // Byte-for-byte fixture reported in https://github.com/hyparam/hyparquet/issues/197
    const bytes = Buffer.from('UEFSMRUAFcACFcACFYGj5Y8JHBUoFQAVCBUIAAAAAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAADAAAAAAAAAAQAAAAAAAAABQAAAAAAAAAGAAAAAAAAAAcAAAAAAAAACAAAAAAAAAAJAAAAAAAAAAoAAAAAAAAACwAAAAAAAAAMAAAAAAAAAA0AAAAAAAAADgAAAAAAAAAPAAAAAAAAABAAAAAAAAAAEQAAAAAAAAASAAAAAAAAABMAAAAAAAAAFQAVwAIVwAIVr6HOyA4cFSiNABUGFQgAAAQAAAAH3nsPBQAAAHJvdy0xBQAAAHJvdy0yBQAAAHJvdy0zBQAAAHJvdy00BQAAAHJvdy02BQAAAHJvdy03BQAAAHJvdy04BQAAAHJvdy05BgAAAHJvd8UxMQYAAAByb3ctMTIGAAAAcm93LTEzBgAAAHJvdy0xNAYAAAByb3ctMTYGAAAAcm93LTE3BgAAAHJvdy0xOAYAAAByb3ctMTkVABXgARXgARWI2rT4DBwVKBUAFQYVCAAABAAAAAe2bQsAAAAAAADgPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAEQAAAAAAAAAxAAAAAAAAAEEAAAAAAAAAUQAAAAAAAABZAAAAAAAAAGkAAAAAAAAAcQAAAAAAAACBAAAAAAAAAIUAAAAAnAAAjQBUAFRQVFBXm86DUCRwVKBUAFQYVCAAABAAAAAdVVQVVARkRAhkYCAAAAAAAAAAAGRgIEwAAAAAAAAAVAhkWABkWKBkWKAAZEQIZGAVyb3ctMRkYBXJvdy05FQIZFggZFigZJgggABkRAhkYCAAAAAAAAOA/GRgIAAAAAAAAI0AVAhkWDhkWKBkmDhoAGRECGRgBABkYAQEVAhkWFBkWKBkmFBQAGRwWCBXyAhYAAAAZHBb6AhXyAhYAABkWsAGUGRwW7AUVkgIWAAAAGRwW/gcVQhYAAAAVAhlcSAViYXNpYxUIABUEJQAYAmlkABUMJQIYBG5hbWUlAEwcAAAAFQolAhgFc2NvcmUAFQAlAhgEZmxhNAAWKBkcGUwmABwVBBklCAAZGAJpZBUAFigW8gIW8gImCDwYCEEAAAAAAAAAGAgAAAAAAAAAABYAKAgTAAAAAAAAABgIAAAAAAAAAAAAGRwVABUAFQIAPCkWKBkWKAAAFsYKFRYWwAgVSgAmABwVDBk1CAYAGRgEbmFtZRUAFigW8gIW8gIm+gI8NggoBXJvdy05GAVyb3ctMQAZHBUAFQAVAgA8FrABGRYoGSYIIAAAFtwKFSAWigkVQAAmABwVChk1CAYAGRgFc2NvcmUVABYoFpICFpICJuwFPBgIAAAAAAAAI0AYCAAAAAAAAOA/Fg4oCAAAAAAAACNAGAgAAAAAAADgPwAZHBUAFQAVAgA8KRYoGSYOGgAAFvwKFRgWygkVTAAmABwVABk1CAYAGRgEZmxhZxUAFigWQhZCJv4HPBgBARgBABYUKAEBGAEAABkcFQAVABUCADwpFigZJhQUAAAWlAsVFhaWChUwABa4CBYoJggWuAgUAAAZHBgRd3JpdGVyLm1vZGVsLm5hbWUYB2V4YW1wbGUAGEpwYXJxdWV0LW1yIHZlcnNpb24gMS4xNS4yIChidWlsZCA4NTllYWMxNjViMDhmOTI3ZmExNDU5MGMzM2JjNWY0NzY0MDVmYjY4KRlMHAAAHAAAHAAAHAAAADMCAABQQVIx', 'base64')
    const file = {
      byteLength: bytes.length,
      /**
       * @param {number} start
       * @param {number} [end]
       */
      slice(start, end) {
        return bytes.buffer.slice(bytes.byteOffset + start, bytes.byteOffset + (end ?? bytes.length))
      },
    }
    const metadata = await parquetMetadataAsync(file)
    /** @type {unknown[]} */
    const unhandled = []
    /** @param {unknown} reason */
    const onUnhandled = reason => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      await expect(parquetRead({ file, metadata, onComplete: () => {} }))
        .rejects.toThrow('parquet schema element not found: flag')
      await new Promise(resolve => setTimeout(resolve, 50))
      expect(unhandled).toEqual([])
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })
})
