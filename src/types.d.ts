export type Awaitable<T> = T | Promise<T>

/**
 * File-like object that can read slices of a file asynchronously.
 */
export interface AsyncBuffer {
  byteLength: number
  slice(start: number, end?: number): Awaitable<ArrayBuffer>
}

export interface DataReader {
  view: DataView
  offset: number
}

// Parquet file metadata types
export interface FileMetaData {
  version: number
  schema: SchemaElement[]
  num_rows: bigint
  row_groups: RowGroup[]
  key_value_metadata?: KeyValue[]
  created_by?: string
  // column_orders?: ColumnOrder[]
  // encryption_algorithm?: EncryptionAlgorithm
  // footer_signing_key_metadata?: Uint8Array
  metadata_length: number
}

export interface SchemaTree {
  children: SchemaTree[]
  count: number
  element: SchemaElement
  path: string[]
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
  logical_type?: LogicalType
}

export type ParquetType =
  'BOOLEAN' |
  'INT32' |
  'INT64' |
  'INT96' | // deprecated
  'FLOAT' |
  'DOUBLE' |
  'BYTE_ARRAY' |
  'FIXED_LEN_BYTE_ARRAY'

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

type LogicalDecimalType = {
  type: 'DECIMAL'
  precision: number
  scale: number
}

export type TimeUnit = 'MILLIS' | 'MICROS' | 'NANOS'

type LogicalTimeType = {
  type: 'TIME'
  isAdjustedToUTC: boolean
  unit: TimeUnit
}

type LogicalTimestampType = {
  type: 'TIMESTAMP'
  isAdjustedToUTC: boolean
  unit: TimeUnit
}

type LogicalIntType = {
  type: 'INTEGER'
  bitWidth: number
  isSigned: boolean
}

export type LogicalType =
  { type: LogicalTypeSimple } |
  LogicalDecimalType |
  LogicalTimeType |
  LogicalTimestampType |
  LogicalIntType

type LogicalTypeSimple =
  'STRING' |
  'MAP' |
  'LIST' |
  'ENUM' |
  'DECIMAL' |
  'DATE' |
  'INTERVAL' |
  'NULL' |
  'JSON' |
  'BSON' |
  'UUID' |
  'FLOAT16'

export type LogicalTypeType = LogicalTypeSimple |
  'TIME' | // convertedType TIME_MILLIS or TIME_MICROS
  'TIMESTAMP' | // convertedType TIMESTAMP_MILLIS or TIMESTAMP_MICROS
  'INTEGER' // convertedType INT or UINT

export interface RowGroup {
  columns: ColumnChunk[]
  total_byte_size: bigint
  num_rows: bigint
  sorting_columns?: SortingColumn[]
  file_offset?: bigint
  total_compressed_size?: bigint
  ordinal?: number
}

export interface ColumnChunk {
  file_path?: string
  file_offset: bigint
  meta_data?: ColumnMetaData
  offset_index_offset?: bigint
  offset_index_length?: number
  column_index_offset?: bigint
  column_index_length?: number
  crypto_metadata?: ColumnCryptoMetaData
  encrypted_column_metadata?: Uint8Array
}

export interface ColumnMetaData {
  type: ParquetType
  encodings: Encoding[]
  path_in_schema: string[]
  codec: CompressionCodec
  num_values: bigint
  total_uncompressed_size: bigint
  total_compressed_size: bigint
  key_value_metadata?: KeyValue[]
  data_page_offset: bigint
  index_page_offset?: bigint
  dictionary_page_offset?: bigint
  statistics?: Statistics
  encoding_stats?: PageEncodingStats[]
  bloom_filter_offset?: bigint
  bloom_filter_length?: number
  size_statistics?: SizeStatistics
}

interface ColumnCryptoMetaData {}

export type Encoding =
  'PLAIN' |
  'PLAIN_DICTIONARY' |
  'RLE' |
  'BIT_PACKED' | // deprecated
  'DELTA_BINARY_PACKED' |
  'DELTA_LENGTH_BYTE_ARRAY' |
  'DELTA_BYTE_ARRAY' |
  'RLE_DICTIONARY' |
  'BYTE_STREAM_SPLIT'

export type CompressionCodec =
  'UNCOMPRESSED' |
  'SNAPPY' |
  'GZIP' |
  'LZO' |
  'BROTLI' |
  'LZ4' |
  'ZSTD' |
  'LZ4_RAW'

export type Compressors = {
  [K in CompressionCodec]?: (input: Uint8Array, outputLength: number) => Uint8Array
}

interface KeyValue {
  key: string
  value?: string
}

type MinMaxType = bigint | boolean | number | string | Date | Uint8Array

export interface Statistics {
  max?: MinMaxType
  min?: MinMaxType
  null_count?: bigint
  distinct_count?: bigint
  max_value?: string
  min_value?: string
  is_max_value_exact?: boolean
  is_min_value_exact?: boolean
}

interface SizeStatistics {
  unencoded_byte_array_data_bytes?: bigint
  repetition_level_histogram?: bigint[]
  definition_level_histogram?: bigint[]
}

interface PageEncodingStats {
  page_type: PageType
  encoding: Encoding
  count: number
}

export type PageType =
  'DATA_PAGE' |
  'INDEX_PAGE' |
  'DICTIONARY_PAGE' |
  'DATA_PAGE_V2'

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

interface DataPage {
  definitionLevels: number[] | undefined
  repetitionLevels: number[]
  dataPage: DecodedArray
}

export type DecodedArray =
  Uint8Array |
  Int32Array |
  BigInt64Array |
  BigUint64Array |
  Float32Array |
  Float64Array |
  any[]

export interface OffsetIndex {
  page_locations: PageLocation[]
  unencoded_byte_array_data_bytes?: bigint[]
}

interface PageLocation {
  offset: bigint
  compressed_page_size: number
  first_row_index: bigint
}

export interface ColumnIndex {
  null_pages: boolean[]
  min_values: MinMaxType[]
  max_values: MinMaxType[]
  boundary_order: BoundaryOrder
  null_counts?: bigint[]
  repetition_level_histograms?: bigint[]
  definition_level_histograms?: bigint[]
}

export type BoundaryOrder = 'UNORDERED' | 'ASCENDING' | 'DESCENDING'
