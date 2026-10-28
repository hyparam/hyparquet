import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetMetadata, toJson } from '../src/index.js'
import { readColumnIndex, readOffsetIndex } from '../src/indexes.js'
import { asyncBufferFromFile } from '../src/node.js'
import { getSchemaPath } from '../src/schema.js'
import { fileToJson } from './helpers.js'

describe('readColumnIndex', () => {
  const columnIndexesFiles = fs.readdirSync('test/files').filter(f => f.endsWith('.column_indexes.json'))
  const parquetFiles = columnIndexesFiles.map(f => f.replace(/.column_indexes.json$/i, '.parquet'))

  parquetFiles.forEach((file, i) => {
    it(`parse column indexes from ${file}`, async () => {
      const arrayBuffer = await readFileToArrayBuffer(`test/files/${file}`)
      const metadata = parquetMetadata(arrayBuffer)

      const result = metadata.row_groups.map((rowGroup) => rowGroup.columns.map((column) => {
        if (column.column_index_offset === undefined || column.column_index_length === undefined) return null
        const columnIndexOffset = Number(column.column_index_offset)
        const columnIndexLength = Number(column.column_index_length)
        const columnIndexArrayBuffer = arrayBuffer.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
        const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
        const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
        return readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })
      }))
      const expected = fileToJson(`test/files/${columnIndexesFiles[i]}`)
      expect(toJson(result)).toEqual(expected)
    })
  })

  it('decodes unsigned INT32 and INT64 bounds using their logical types', () => {
    const uint32Index = columnIndexBuffer(new Uint8Array(4).fill(0xff))
    const uint64Index = columnIndexBuffer(new Uint8Array(8).fill(0xff))

    expect(readColumnIndex(
      { view: new DataView(uint32Index), offset: 0 },
      { name: 'uint32', type: 'INT32', logical_type: { type: 'INTEGER', bitWidth: 32, isSigned: false } }
    ).min_values).toEqual([4294967295])
    expect(readColumnIndex(
      { view: new DataView(uint64Index), offset: 0 },
      { name: 'uint64', type: 'INT64', converted_type: 'UINT_64' }
    ).max_values).toEqual([18446744073709551615n])
  })

  it('scales integer-backed DECIMAL bounds into the decoded value domain', () => {
    const int32 = new Uint8Array(4)
    new DataView(int32.buffer).setInt32(0, 500, true)
    const int64 = new Uint8Array(8)
    new DataView(int64.buffer).setBigInt64(0, 500n, true)

    expect(readColumnIndex(
      { view: new DataView(columnIndexBuffer(int32)), offset: 0 },
      { name: 'decimal32', type: 'INT32', converted_type: 'DECIMAL', precision: 9, scale: 2 }
    ).min_values).toEqual([5])
    expect(readColumnIndex(
      { view: new DataView(columnIndexBuffer(int64)), offset: 0 },
      { name: 'decimal64', type: 'INT64', converted_type: 'DECIMAL', precision: 18, scale: 2 }
    ).max_values).toEqual([5])
  })

  it('preserves subnormal DECIMAL bounds when the scale exceeds 308', () => {
    const positive = new Uint8Array(129)
    positive[128] = 1
    const negative = new Uint8Array(129).fill(255)
    /** @type {[Uint8Array, number][]} */
    const cases = [[positive, 1e-309], [negative, -1e-309]]
    for (const [bytes, expected] of cases) {
      const index = readColumnIndex(
        { view: new DataView(columnIndexBuffer(bytes)), offset: 0 },
        { name: 'decimal', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 129, converted_type: 'DECIMAL', precision: 310, scale: 309 }
      )
      expect(index.min_values).toEqual([expected])
      expect(index.max_values).toEqual([expected])
    }
  })

  it('scales DECIMAL bounds without rounding error', () => {
    const int32 = new Uint8Array(4)
    new DataView(int32.buffer).setInt32(0, 3, true)
    const int64 = new Uint8Array(8)
    new DataView(int64.buffer).setBigInt64(0, 150000n, true)
    const fixed = new Uint8Array([0, 115])

    expect(readColumnIndex(
      { view: new DataView(columnIndexBuffer(int32)), offset: 0 },
      { name: 'decimal32', type: 'INT32', converted_type: 'DECIMAL', precision: 9, scale: 1 }
    ).min_values).toEqual([0.3])
    expect(readColumnIndex(
      { view: new DataView(columnIndexBuffer(int64)), offset: 0 },
      { name: 'decimal64', type: 'INT64', converted_type: 'DECIMAL', precision: 18, scale: 5 }
    ).max_values).toEqual([1.5])
    expect(readColumnIndex(
      { view: new DataView(columnIndexBuffer(fixed)), offset: 0 },
      { name: 'decimal', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 2, converted_type: 'DECIMAL', precision: 4, scale: 2 }
    ).min_values).toEqual([1.15])
  })
})

describe('readOffsetIndex', () => {
  const offsetIndexesFiles = fs.readdirSync('test/files').filter(f => f.endsWith('.offset_indexes.json'))
  const parquetFiles = offsetIndexesFiles.map(f => f.replace(/.offset_indexes.json$/i, '.parquet'))

  parquetFiles.forEach((file, i) => {
    it(`parse offset indexes from ${file}`, async () => {
      const arrayBuffer = await readFileToArrayBuffer(`test/files/${file}`)
      const metadata = parquetMetadata(arrayBuffer)

      const result = metadata.row_groups.map((rowGroup) => rowGroup.columns.map((column) => {
        if (column.offset_index_offset === undefined || column.offset_index_length === undefined) return null
        const offsetIndexOffset = Number(column.offset_index_offset)
        const offsetIndexLength = Number(column.offset_index_length)
        const offsetIndexArrayBuffer = arrayBuffer.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
        const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
        return readOffsetIndex(offsetIndexReader)
      }))
      const expected = fileToJson(`test/files/${offsetIndexesFiles[i]}`)
      expect(toJson(result)).toEqual(expected)
    })
  })
})

/**
 * @param {string} filename
 * @returns {Promise<ArrayBuffer>}
 */
function readFileToArrayBuffer(filename) {
  return asyncBufferFromFile(filename).then((buffer) => buffer.slice(0))
}

/**
 * Make a compact-protocol ColumnIndex with one non-null page whose min and
 * max are both the supplied physical value.
 *
 * @param {Uint8Array} value
 * @returns {ArrayBuffer}
 */
function columnIndexBuffer(value) {
  const length = []
  let remaining = value.length
  while (remaining >= 128) {
    length.push(remaining & 127 | 128)
    remaining >>>= 7
  }
  length.push(remaining)
  const bytes = Uint8Array.from([
    0x19, 0x12, 0, // field 1: list<bool> null_pages = [false]
    0x19, 0x18, ...length, ...value, // field 2: list<binary> min_values
    0x19, 0x18, ...length, ...value, // field 3: list<binary> max_values
    0x15, 0, // field 4: boundary_order = UNORDERED
    0, // struct stop
  ])
  return bytes.buffer
}
