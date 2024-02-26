import { deserializeTCompactProtocol } from './thrift.js'

/**
 * Return type with bytes read.
 * This is useful to advance an offset through a buffer.
 *
 * @typedef {import("./types.d.ts").Decoded<T>} Decoded
 * @template T
 */

/**
 * Read parquet header from a buffer.
 *
 * @typedef {import("./types.d.ts").ArrayBufferLike} ArrayBufferLike
 * @typedef {import("./types.d.ts").PageHeader} PageHeader
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @param {number} offset offset to start reading from
 * @returns {Decoded<PageHeader>} metadata object and bytes read
 */
export function parquetHeader(arrayBuffer, offset) {
  const { value: header, byteLength } = deserializeTCompactProtocol(arrayBuffer, offset)

  // Parse parquet header from thrift data
  const type = header.field_1
  const uncompressed_page_size = header.field_2
  const compressed_page_size = header.field_3
  const crc = header.field_4
  const data_page_header = header.field_5 && {
    num_values: header.field_5.field_1,
    encoding: header.field_5.field_2,
    definition_level_encoding: header.field_5.field_3,
    repetition_level_encoding: header.field_5.field_4,
    statistics: header.field_5.field_5 && {
      max: header.field_5.field_5.field_1,
      min: header.field_5.field_5.field_2,
      null_count: header.field_5.field_5.field_3,
      distinct_count: header.field_5.field_5.field_4,
      max_value: header.field_5.field_5.field_5,
      min_value: header.field_5.field_5.field_6,
    },
  }
  const index_page_header = header.field_6
  const dictionary_page_header = header.field_7 && {
    num_values: header.field_7.field_1,
    encoding: header.field_7.field_2,
    is_sorted: header.field_7.field_3,
  }
  const data_page_header_v2 = header.field_8 && {
    num_values: header.field_8.field_1,
    num_nulls: header.field_8.field_2,
    num_rows: header.field_8.field_3,
    encoding: header.field_8.field_4,
    definition_levels_byte_length: header.field_8.field_5,
    repetition_levels_byte_length: header.field_8.field_6,
    is_compressed: header.field_8.field_7 === undefined ? true : header.field_8.field_7, // default to true
    statistics: header.field_8.field_8,
  }

  return {
    byteLength,
    value: {
      type,
      uncompressed_page_size,
      compressed_page_size,
      crc,
      data_page_header,
      index_page_header,
      dictionary_page_header,
      data_page_header_v2,
    },
  }
}
