/**
 * File-like object that can read slices of a file asynchronously.
 */
export interface AsyncBuffer {
  byteLength: number
  slice(start: number, end?: number): Promise<ArrayBuffer>
}

/**
 * Just like an ArrayBuffer, but an interface
 */
export interface ArrayBufferLike {
  byteLength: number
  slice(start: number, end?: number): ArrayBuffer
}

/**
 * Represents a decoded value, and includes the number of bytes read.
 * This is used to read data from the file and advance a virtual file pointer.
 */
export interface Decoded<T> {
  value: T
  byteLength: number
}

// Parquet file metadata types
export interface FileMetaData {
  version: number
  schema: SchemaElement[]
  num_rows: number
  row_groups: RowGroup[]
  key_value_metadata?: KeyValue[]
  created_by?: string
  metadata_length: number
}

export interface SchemaTree {
  element: SchemaElement
  children: SchemaTree[]
  count: number
}

export interface SchemaElement {
  type?: ParquetType
  type_length?: number
  repetition_type?: FieldRepetitionType
  name: string
  num_children?: number
  converted_type?: ConvertedType
  scale?: number
  precision?: number
  field_id?: number
}

export enum ParquetType {
  BOOLEAN = 0,
  INT32 = 1,
  INT64 = 2,
  INT96 = 3, // deprecated
  FLOAT = 4,
  DOUBLE = 5,
  BYTE_ARRAY = 6,
  FIXED_LEN_BYTE_ARRAY = 7,
}

export type FieldRepetitionType =
  'REQUIRED' |
  'OPTIONAL' |
  'REPEATED'

export type ConvertedType =
  'UTF8' |
  'MAP' |
  'MAP_KEY_VALUE' |
  'LIST' |
  'ENUM' |
  'DECIMAL' |
  'DATE' |
  'TIME_MILLIS' |
  'TIME_MICROS' |
  'TIMESTAMP_MILLIS' |
  'TIMESTAMP_MICROS' |
  'UINT_8' |
  'UINT_16' |
  'UINT_32' |
  'UINT_64' |
  'INT_8' |
  'INT_16' |
  'INT_32' |
  'INT_64' |
  'JSON' |
  'BSON' |
  'INTERVAL'

export interface RowGroup {
  columns: ColumnChunk[]
  total_byte_size: number
  num_rows: number
  sorting_columns?: SortingColumn[]
}

export interface ColumnChunk {
  file_path?: string
  file_offset: number
  meta_data?: ColumnMetaData
}

export interface ColumnMetaData {
  type: ParquetType
  encodings: Encoding[]
  path_in_schema: string[]
  codec: CompressionCodec
  num_values: number
  total_uncompressed_size: number
  total_compressed_size: number
  key_value_metadata?: KeyValue[]
  data_page_offset: number
  index_page_offset?: number
  dictionary_page_offset?: number
  statistics?: Statistics
  encoding_stats?: PageEncodingStats[]
}

export enum Encoding {
  PLAIN = 0,
  PLAIN_DICTIONARY = 2,
  RLE = 3,
  BIT_PACKED = 4, // deprecated
  DELTA_BINARY_PACKED = 5,
  DELTA_LENGTH_BYTE_ARRAY = 6,
  DELTA_BYTE_ARRAY = 7,
  RLE_DICTIONARY = 8,
  BYTE_STREAM_SPLIT = 9,
}

export type CompressionCodec =
  'UNCOMPRESSED' |
  'SNAPPY' |
  'GZIP' |
  'LZO' |
  'BROTLI' |
  'LZ4' |
  'ZSTD' |
  'LZ4_RAW'

interface KeyValue {
  key: string
  value?: string
}

export interface Statistics {
  max?: Uint8Array // binary representation
  min?: Uint8Array // binary representation
  null_count?: number
  distinct_count?: number
}

interface PageEncodingStats {
  page_type: PageType
  encoding: Encoding
  count: number
}

export enum PageType {
  DATA_PAGE = 0,
  INDEX_PAGE = 1,
  DICTIONARY_PAGE = 2,
  DATA_PAGE_V2 = 3,
}

interface SortingColumn {
  column_idx: number
  descending: boolean
  nulls_first: boolean
}

// Parquet file header types
export interface PageHeader {
  type: PageType
  uncompressed_page_size: number
  compressed_page_size: number
  crc?: number
  data_page_header?: DataPageHeader
  index_page_header?: IndexPageHeader
  dictionary_page_header?: DictionaryPageHeader
  data_page_header_v2?: DataPageHeaderV2
}

export interface DataPageHeader {
  num_values: number
  encoding: Encoding
  definition_level_encoding: Encoding
  repetition_level_encoding: Encoding
  statistics?: Statistics
}

interface IndexPageHeader {}

export interface DictionaryPageHeader {
  num_values: number
  encoding: Encoding
  is_sorted?: boolean
}

interface DataPageHeaderV2 {
  num_values: number
  num_nulls: number
  num_rows: number
  encoding: Encoding
  definition_levels_byte_length: number
  repetition_levels_byte_length: number
  is_compressed?: boolean
  statistics?: Statistics
}

type DecodedArray = any[] | Uint8Array

interface DataPage {
  definitionLevels: number[] | undefined
  repetitionLevels: number[]
  value: any[]
}
