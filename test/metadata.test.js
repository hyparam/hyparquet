import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetMetadata, parquetMetadataAsync, parquetSchema } from '../src/hyparquet.js'
import { toJson } from '../src/toJson.js'

/**
 * Helper function to read .parquet file into ArrayBuffer
 *
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
async function readFileToArrayBuffer(filePath) {
  const buffer = await fs.promises.readFile(filePath)
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
}

/**
 * Wrap .parquet file in an AsyncBuffer
 *
 * @typedef {import('../src/types.js').AsyncBuffer} AsyncBuffer
 * @param {string} filePath
 * @returns {AsyncBuffer}
 */
function fileToAsyncBuffer(filePath) {
  return {
    byteLength: fs.statSync(filePath).size,
    slice: async (start, end) => (await readFileToArrayBuffer(filePath)).slice(start, end),
  }
}

describe('parquetMetadata', () => {
  it('should parse metadata from addrtype-missing-value.parquet', async () => {
    const arrayBuffer = await readFileToArrayBuffer('test/files/addrtype-missing-value.parquet')
    const result = parquetMetadata(arrayBuffer)
    expect(toJson(result)).toEqual(addrtypeMetadata)
  })

  it('should parse metadata from rowgroups.parquet', async () => {
    const arrayBuffer = await readFileToArrayBuffer('test/files/rowgroups.parquet')
    const result = parquetMetadata(arrayBuffer)
    expect(toJson(result)).containSubset(rowgroupsMetadata)
  })

  it('should throw an error for a too short file', () => {
    const arrayBuffer = new ArrayBuffer(0)
    expect(() => parquetMetadata(arrayBuffer)).toThrow('parquet file is too short')
  })

  it('should throw an error for invalid metadata length', () => {
    const arrayBuffer = new ArrayBuffer(12)
    const view = new DataView(arrayBuffer)
    view.setUint32(0, 0x31524150, true) // magic number PAR1
    view.setUint32(4, 1000, true) // 1000 bytes exceeds buffer
    view.setUint32(8, 0x31524150, true) // magic number PAR1
    expect(() => parquetMetadata(arrayBuffer))
      .toThrow('parquet metadata length 1000 exceeds available buffer 4')
  })

  it('should throw an error for invalid magic number', () => {
    const arrayBuffer = new ArrayBuffer(8)
    expect(() => parquetMetadata(arrayBuffer)).toThrow('parquet file invalid magic number')
  })
})

describe('parquetMetadataAsync', () => {
  it('should parse metadata asynchronously from addrtype-missing-value.parquet', async () => {
    const asyncBuffer = fileToAsyncBuffer('test/files/addrtype-missing-value.parquet')
    const result = await parquetMetadataAsync(asyncBuffer)
    expect(toJson(result)).toEqual(addrtypeMetadata)
  })

  it('should parse metadata asynchronously from rowgroups.parquet', async () => {
    const asyncBuffer = fileToAsyncBuffer('test/files/rowgroups.parquet')
    // force two fetches
    const result = await parquetMetadataAsync(asyncBuffer, 1609)
    expect(toJson(result)).containSubset(rowgroupsMetadata)
  })
})

describe('parquetSchema', () => {
  it('should parse schema from addrtype-missing-value.parquet', async () => {
    const arrayBuffer = await readFileToArrayBuffer('test/files/addrtype-missing-value.parquet')
    const metadata = parquetMetadata(arrayBuffer)
    const result = parquetSchema(metadata)
    expect(toJson(result)).toEqual({
      children: [
        {
          children: [],
          count: 1,
          element: {
            converted_type: 0,
            name: 'ADDRTYPE',
            repetition_type: 1,
            type: 6,
          },
        },
      ],
      count: 2,
      element: {
        name: 'duckdb_schema',
        num_children: 1,
        repetition_type: 0,
      },
    })
  })

  it('should parse schema from rowgroups.parquet', async () => {
    const arrayBuffer = await readFileToArrayBuffer('test/files/rowgroups.parquet')
    const metadata = parquetMetadata(arrayBuffer)
    const result = parquetSchema(metadata)
    expect(toJson(result)).toEqual({
      children: [
        {
          children: [],
          count: 1,
          element: {
            name: 'numbers',
            repetition_type: 1,
            type: 2,
          },
        },
      ],
      count: 2,
      element: {
        name: 'schema',
        num_children: 1,
        repetition_type: 0,
      },
    })
  })
})

// Parquet v1 from DuckDB
const addrtypeMetadata = {
  version: 1,
  created_by: 'DuckDB',
  metadata_length: 149,
  schema: [
    { repetition_type: 0, name: 'duckdb_schema', num_children: 1 },
    { type: 6, repetition_type: 1, name: 'ADDRTYPE', converted_type: 0 },
  ],
  num_rows: 10,
  row_groups: [
    {
      columns: [
        {
          file_offset: 0,
          meta_data: {
            type: 6,
            encodings: [0, 8],
            path_in_schema: ['ADDRTYPE'],
            codec: 1,
            num_values: 10,
            total_uncompressed_size: 78,
            total_compressed_size: 82,
            data_page_offset: 31,
            dictionary_page_offset: 4,
            statistics: {
              max: 'Intersection',
              min: 'Block',
              null_count: 1,
              distinct_count: 2,
            },
          },
        },
      ],
      total_byte_size: 33024,
      num_rows: 10,
    },
  ],
}

// Parquet v2 from pandas with 2 row groups
const rowgroupsMetadata = {
  version: 2,
  created_by: 'parquet-cpp-arrow version 14.0.2',
  metadata_length: 1602,
  schema: [
    {
      repetition_type: 0,
      name: 'schema',
      num_children: 1,
    },
    {
      type: 2,
      repetition_type: 1,
      name: 'numbers',
    },
  ],
  num_rows: 15,
  row_groups: [
    {
      columns: [
        {
          file_offset: 150,
          file_path: undefined,
          meta_data: {
            codec: 1,
            data_page_offset: 71,
            dictionary_page_offset: 4,
            encoding_stats: [
              { count: 1, encoding: 0, page_type: 2 },
              { count: 1, encoding: 8, page_type: 0 },
            ],
            encodings: [0, 3, 8],
            num_values: 10,
            path_in_schema: ['numbers'],
            statistics: {
              max: '\n\x00\x00\x00\x00\x00\x00\x00',
              min: '\x01\x00\x00\x00\x00\x00\x00\x00',
              null_count: 0,
            },
            total_compressed_size: 146,
            total_uncompressed_size: 172,
            type: 2,
          },
        },
      ],
      total_byte_size: 172,
      num_rows: 10,
    },
    {
      columns: [
        {
          file_offset: 368,
          meta_data: {
            codec: 1,
            data_page_offset: 294,
            dictionary_page_offset: 248,
            encoding_stats: [
              { count: 1, encoding: 0, page_type: 2 },
              { count: 1, encoding: 8, page_type: 0 },
            ],
            encodings: [0, 3, 8],
            num_values: 5,
            path_in_schema: ['numbers'],
            statistics: {
              max: '\x0F\x00\x00\x00\x00\x00\x00\x00',
              min: '\x0B\x00\x00\x00\x00\x00\x00\x00',
              null_count: 0,
            },
            total_compressed_size: 120,
            total_uncompressed_size: 126,
            type: 2,
          },
        },
      ],
      total_byte_size: 126,
      num_rows: 5,
    },
  ],
  key_value_metadata: [
    {
      key: 'pandas',
      // value: json
    },
    {
      key: 'ARROW:schema',
      // value: base64
    },
  ],
}
