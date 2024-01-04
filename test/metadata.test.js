import { promises as fs } from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetMetadata, toJson } from '../src/metadata'

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

  it('should throw an error for a too short file', () => {
    const arrayBuffer = new ArrayBuffer(0)
    expect(() => parquetMetadata(arrayBuffer)).toThrow('parquet file is too short')
  })

  it('should throw an error for invalid magic number', () => {
    const arrayBuffer = new ArrayBuffer(8)
    expect(() => parquetMetadata(arrayBuffer)).toThrow('parquet file invalid magic number')
  })
})
