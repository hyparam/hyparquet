import { deserializeTCompactProtocol } from './thrift.js'
import type { FileMetaData, SchemaElement } from './types.ts'

/**
 * Read parquet header, metadata, and schema information from a file
 * @param arrayBuffer parquet file contents
 * @returns metadata object
 */
export function parquetMetadata(arrayBuffer: ArrayBuffer): FileMetaData {
  // DataView for easier manipulation of the buffer
  const view = new DataView(arrayBuffer)

  // Validate footer magic number "PAR1"
  if (view.byteLength < 8) {
    throw new Error('parquet file is too short')
  }
  if (view.getUint32(view.byteLength - 4, true) !== 0x31524150) {
    throw new Error('parquet file invalid magic number')
  }

  // Parquet files store metadata at the end of the file
  // Metadata length is 4 bytes before the last PAR1
  const metadataLengthOffset = view.byteLength - 8
  const metadataLength = view.getUint32(view.byteLength - 8, true)
  if (metadataLength <= 0 || metadataLength > metadataLengthOffset) {
    throw new Error('parquet file invalid metadata length')
  }

  const metadataOffset = metadataLengthOffset - metadataLength
  const metadataBuffer = view.buffer.slice(metadataOffset, metadataLengthOffset)
  const { value: metadata } = deserializeTCompactProtocol(metadataBuffer)

  // Parse parquet metadata from thrift data
  const version = metadata.field_1
  const schema = metadata.field_2.map((field: any) => ({
    type: field.field_1,
    type_length: field.field_2,
    repetition_type: field.field_3,
    name: field.field_4,
    num_children: field.field_5,
    converted_type: field.field_6,
    scale: field.field_7,
    precision: field.field_8,
    field_id: field.field_9,
  }))
  const num_rows = metadata.field_3
  const row_groups = metadata.field_4.map((rowGroup: any) => ({
    columns: rowGroup.field_1.map((column: any) => ({
      file_path: column.field_1,
      file_offset: column.field_2,
      meta_data: column.field_3 && {
        type: column.field_3.field_1,
        encodings: column.field_3.field_2,
        path_in_schema: column.field_3.field_3,
        codec: column.field_3.field_4,
        num_values: column.field_3.field_5,
        total_uncompressed_size: column.field_3.field_6,
        total_compressed_size: column.field_3.field_7,
        key_value_metadata: column.field_3.field_8,
        data_page_offset: column.field_3.field_9,
        index_page_offset: column.field_3.field_10,
        dictionary_page_offset: column.field_3.field_11,
        statistics: column.field_3.field_12 && {
          max: column.field_3.field_12.field_1,
          min: column.field_3.field_12.field_2,
          null_count: column.field_3.field_12.field_3,
          distinct_count: column.field_3.field_12.field_4,
        },
        encoding_stats: column.field_3.field_13?.map((encodingStat: any) => ({
          page_type: encodingStat.field_1,
          encoding: encodingStat.field_2,
          count: encodingStat.field_3,
        })),
      },
    })),
    total_byte_size: rowGroup.field_2,
    num_rows: rowGroup.field_3,
    sorting_columns: rowGroup.field_4?.map((sortingColumn: any) => ({
      column_idx: sortingColumn.field_1,
      descending: sortingColumn.field_2,
      nulls_first: sortingColumn.field_3,
    })),
  }))
  const key_value_metadata = metadata.field_5?.map((keyValue: any) => ({
    key: keyValue.field_1,
    value: keyValue.field_2,
  }))
  const created_by = metadata.field_6

  return {
    version,
    schema,
    num_rows,
    row_groups,
    key_value_metadata,
    created_by,
  }
}

/**
 * Get the schema element with the given name.
 */
export function schemaElement(schema: SchemaElement[], name: string[]): any {
  function key(name: string[]) { return name.join('.') }
  const schemaElementByName = new Map(schema.map(se => [se.name, se]))
  const element = schemaElementByName.get(key(name))
  if (!element) {
    throw new Error(`schema element not found: ${name}`)
  }
  return element
}

/**
 * Replace bigints with numbers.
 * When parsing parquet files, bigints are used to represent 64-bit integers.
 * However, JSON does not support bigints, so it's helpful to convert to numbers.
 */
export function toJson(obj: unknown): unknown {
  if (typeof obj === 'bigint') {
    return Number(obj)
  } else if (Array.isArray(obj)) {
    return obj.map(toJson)
  } else if (typeof obj === 'object') {
    const newObj = {}
    for (const key of Object.keys(obj)) {
      newObj[key] = toJson(obj[key])
    }
    return newObj
  } else {
    return obj
  }
}
