import { promises as fs } from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetMetadata } from '../src/metadata.js'
import { toJson } from '../src/toJson.js'

/**
 * Helper function to read .parquet file into ArrayBuffer
 *
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
async function readFileToArrayBuffer(filePath) {
  const buffer = await fs.readFile(filePath)
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
}

describe('parquetMetadata', () => {
  it('should correctly decode metadata from addrtype-missing-value.parquet', async () => {
    const arrayBuffer = await readFileToArrayBuffer('test/files/addrtype-missing-value.parquet')
    const result = parquetMetadata(arrayBuffer)

    // Parquet v1 from DuckDB
    const expectedMetadata = {
      version: 1,
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
      created_by: 'DuckDB',
    }

    const casted = toJson(result)
    expect(casted).toEqual(expectedMetadata)
  })

  it('should correctly decode metadata from rowgroups.parquet', async () => {
    const arrayBuffer = await readFileToArrayBuffer('test/files/rowgroups.parquet')
    const result = parquetMetadata(arrayBuffer)

    // Parquet v2 from pandas with 2 row groups
    const expectedMetadata = {
      version: 2,
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
      created_by: 'parquet-cpp-arrow version 14.0.2',
    }

    const casted = toJson(result)
    expect(casted).containSubset(expectedMetadata)
  })

  it('should throw an error for a too short file', () => {
    const arrayBuffer = new ArrayBuffer(0)
    expect(() => parquetMetadata(arrayBuffer)).toThrow('parquet file is too short')
  })

  it('should throw an error for invalid magic number', () => {
    const arrayBuffer = new ArrayBuffer(8)
    expect(() => parquetMetadata(arrayBuffer)).toThrow('parquet file invalid magic number')
  })
})
