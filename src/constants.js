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
