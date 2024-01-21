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

export const ParquetEncoding = {
  PLAIN: 0,
  PLAIN_DICTIONARY: 2,
  RLE: 3,
  BIT_PACKED: 4, // deprecated
  DELTA_BINARY_PACKED: 5,
  DELTA_LENGTH_BYTE_ARRAY: 6,
  DELTA_BYTE_ARRAY: 7,
  RLE_DICTIONARY: 8,
  BYTE_STREAM_SPLIT: 9,
}

export const FieldRepetitionType = {
  REQUIRED: 0,
  OPTIONAL: 1,
  REPEATED: 2,
}

export const ConvertedType = {
  UTF8: 0,
  MAP: 1,
  MAP_KEY_VALUE: 2,
  LIST: 3,
  ENUM: 4,
  DECIMAL: 5,
  DATE: 6,
  TIME_MILLIS: 7,
  TIME_MICROS: 8,
  TIMESTAMP_MILLIS: 9,
  TIMESTAMP_MICROS: 10,
  UINT_8: 11,
  UINT_16: 12,
  UINT_32: 13,
  UINT_64: 14,
  INT_8: 15,
  INT_16: 16,
  INT_32: 17,
  INT_64: 18,
  JSON: 19,
  BSON: 20,
  INTERVAL: 21,
}

export const CompressionCodec = {
  UNCOMPRESSED: 0,
  SNAPPY: 1,
  GZIP: 2,
  LZO: 3,
  BROTLI: 4,
  LZ4: 5,
  ZSTD: 6,
  LZ4_RAW: 7,
}

export const PageType = {
  DATA_PAGE: 0,
  INDEX_PAGE: 1,
  DICTIONARY_PAGE: 2,
  DATA_PAGE_V2: 3,
}

export const Encoding = {
  PLAIN: 0,
  PLAIN_DICTIONARY: 2,
  RLE: 3,
  BIT_PACKED: 4, // deprecated
  DELTA_BINARY_PACKED: 5,
  DELTA_LENGTH_BYTE_ARRAY: 6,
  DELTA_BYTE_ARRAY: 7,
  RLE_DICTIONARY: 8,
  BYTE_STREAM_SPLIT: 9,
}
