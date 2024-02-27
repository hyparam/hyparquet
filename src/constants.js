export const ParquetType = {
  BOOLEAN: 0,
  INT32: 1,
  INT64: 2,
  INT96: 3, // deprecated
  FLOAT: 4,
  DOUBLE: 5,
  BYTE_ARRAY: 6,
  FIXED_LEN_BYTE_ARRAY: 7,
}

export const Encoding = [
  'PLAIN',
  undefined,
  'PLAIN_DICTIONARY',
  'RLE',
  'BIT_PACKED', // deprecated
  'DELTA_BINARY_PACKED',
  'DELTA_LENGTH_BYTE_ARRAY',
  'DELTA_BYTE_ARRAY',
  'RLE_DICTIONARY',
  'BYTE_STREAM_SPLIT',
]

export const FieldRepetitionType = [
  'REQUIRED',
  'OPTIONAL',
  'REPEATED',
]

export const ConvertedType = [
  'UTF8',
  'MAP',
  'MAP_KEY_VALUE',
  'LIST',
  'ENUM',
  'DECIMAL',
  'DATE',
  'TIME_MILLIS',
  'TIME_MICROS',
  'TIMESTAMP_MILLIS',
  'TIMESTAMP_MICROS',
  'UINT_8',
  'UINT_16',
  'UINT_32',
  'UINT_64',
  'INT_8',
  'INT_16',
  'INT_32',
  'INT_64',
  'JSON',
  'BSON',
  'INTERVAL',
]

export const CompressionCodec = [
  'UNCOMPRESSED',
  'SNAPPY',
  'GZIP',
  'LZO',
  'BROTLI',
  'LZ4',
  'ZSTD',
  'LZ4_RAW',
]

export const PageType = {
  DATA_PAGE: 0,
  INDEX_PAGE: 1,
  DICTIONARY_PAGE: 2,
  DATA_PAGE_V2: 3,
}
