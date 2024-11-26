import { CompressionCodec, ConvertedType, Encoding, FieldRepetitionType, PageType, ParquetType } from './constants.js'
import { parseDecimal, parseFloat16 } from './convert.js'
import { getSchemaPath } from './schema.js'
import { deserializeTCompactProtocol } from './thrift.js'

/**
 * Read parquet metadata from an async buffer.
 *
 * An AsyncBuffer is like an ArrayBuffer, but the slices are loaded
 * asynchronously, possibly over the network.
 *
 * You must provide the byteLength of the buffer, typically from a HEAD request.
 *
 * In theory, you could use suffix-range requests to fetch the end of the file,
 * and save a round trip. But in practice, this doesn't work because chrome
 * deems suffix-range requests as a not-safe-listed header, and will require
 * a pre-flight. So the byteLength is required.
 *
 * To make this efficient, we initially request the last 512kb of the file,
 * which is likely to contain the metadata. If the metadata length exceeds the
 * initial fetch, 512kb, we request the rest of the metadata from the AsyncBuffer.
 *
 * This ensures that we either make one 512kb initial request for the metadata,
 * or a second request for up to the metadata size.
 *
 * @typedef {import("../src/types.d.ts").AsyncBuffer} AsyncBuffer
 * @typedef {import("../src/types.d.ts").FileMetaData} FileMetaData
 * @typedef {import("../src/types.d.ts").SchemaElement} SchemaElement
 * @param {AsyncBuffer} asyncBuffer parquet file contents
 * @param {number} initialFetchSize initial fetch size in bytes
 * @returns {Promise<FileMetaData>} parquet metadata object
 */
export async function parquetMetadataAsync(asyncBuffer, initialFetchSize = 1 << 19 /* 512kb */) {
  if (!asyncBuffer) throw new Error('parquet file is required')
  if (!(asyncBuffer.byteLength >= 0)) throw new Error('parquet file byteLength is required')

  // fetch last bytes (footer) of the file
  const footerOffset = Math.max(0, asyncBuffer.byteLength - initialFetchSize)
  const footerBuffer = await asyncBuffer.slice(footerOffset, asyncBuffer.byteLength)

  // Check for parquet magic number "PAR1"
  const footerView = new DataView(footerBuffer)
  if (footerView.getUint32(footerBuffer.byteLength - 4, true) !== 0x31524150) {
    throw new Error('parquet file invalid (footer != PAR1)')
  }

  // Parquet files store metadata at the end of the file
  // Metadata length is 4 bytes before the last PAR1
  const metadataLength = footerView.getUint32(footerBuffer.byteLength - 8, true)
  if (metadataLength > asyncBuffer.byteLength - 8) {
    throw new Error(`parquet metadata length ${metadataLength} exceeds available buffer ${asyncBuffer.byteLength - 8}`)
  }

  // check if metadata size fits inside the initial fetch
  if (metadataLength + 8 > initialFetchSize) {
    // fetch the rest of the metadata
    const metadataOffset = asyncBuffer.byteLength - metadataLength - 8
    const metadataBuffer = await asyncBuffer.slice(metadataOffset, footerOffset)
    // combine initial fetch with the new slice
    const combinedBuffer = new ArrayBuffer(metadataLength + 8)
    const combinedView = new Uint8Array(combinedBuffer)
    combinedView.set(new Uint8Array(metadataBuffer))
    combinedView.set(new Uint8Array(footerBuffer), footerOffset - metadataOffset)
    return parquetMetadata(combinedBuffer)
  } else {
    // parse metadata from the footer
    return parquetMetadata(footerBuffer)
  }
}

/**
 * Read parquet metadata from a buffer synchronously.
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {FileMetaData} parquet metadata object
 */
export function parquetMetadata(arrayBuffer) {
  if (!arrayBuffer) throw new Error('parquet file is required')
  const view = new DataView(arrayBuffer)

  // Validate footer magic number "PAR1"
  if (view.byteLength < 8) {
    throw new Error('parquet file is too short')
  }
  if (view.getUint32(view.byteLength - 4, true) !== 0x31524150) {
    throw new Error('parquet file invalid (footer != PAR1)')
  }

  // Parquet files store metadata at the end of the file
  // Metadata length is 4 bytes before the last PAR1
  const metadataLengthOffset = view.byteLength - 8
  const metadataLength = view.getUint32(metadataLengthOffset, true)
  if (metadataLength > view.byteLength - 8) {
    // {metadata}, metadata_length, PAR1
    throw new Error(`parquet metadata length ${metadataLength} exceeds available buffer ${view.byteLength - 8}`)
  }

  const metadataOffset = metadataLengthOffset - metadataLength
  const reader = { view, offset: metadataOffset }
  const metadata = deserializeTCompactProtocol(reader)
  const decoder = new TextDecoder()
  function decode(/** @type {Uint8Array} */ value) {
    return value && decoder.decode(value)
  }

  // Parse metadata from thrift data
  const version = metadata.field_1
  /** @type {SchemaElement[]} */
  const schema = metadata.field_2.map((/** @type {any} */ field) => ({
    type: ParquetType[field.field_1],
    type_length: field.field_2,
    repetition_type: FieldRepetitionType[field.field_3],
    name: decode(field.field_4),
    num_children: field.field_5,
    converted_type: ConvertedType[field.field_6],
    scale: field.field_7,
    precision: field.field_8,
    field_id: field.field_9,
    logical_type: logicalType(field.field_10),
  }))
  // schema element per column index
  const columnSchema = schema.filter(e => e.type)
  const num_rows = metadata.field_3
  const row_groups = metadata.field_4.map((/** @type {any} */ rowGroup) => ({
    columns: rowGroup.field_1.map((/** @type {any} */ column, /** @type {number} */ columnIndex) => ({
      file_path: decode(column.field_1),
      file_offset: column.field_2,
      meta_data: column.field_3 && {
        type: ParquetType[column.field_3.field_1],
        encodings: column.field_3.field_2?.map((/** @type {number} */ e) => Encoding[e]),
        path_in_schema: column.field_3.field_3.map(decode),
        codec: CompressionCodec[column.field_3.field_4],
        num_values: column.field_3.field_5,
        total_uncompressed_size: column.field_3.field_6,
        total_compressed_size: column.field_3.field_7,
        key_value_metadata: column.field_3.field_8,
        data_page_offset: column.field_3.field_9,
        index_page_offset: column.field_3.field_10,
        dictionary_page_offset: column.field_3.field_11,
        statistics: convertStats(column.field_3.field_12, columnSchema[columnIndex]),
        encoding_stats: column.field_3.field_13?.map((/** @type {any} */ encodingStat) => ({
          page_type: PageType[encodingStat.field_1],
          encoding: Encoding[encodingStat.field_2],
          count: encodingStat.field_3,
        })),
        bloom_filter_offset: column.field_3.field_14,
        bloom_filter_length: column.field_3.field_15,
        size_statistics: column.field_3.field_16 && {
          unencoded_byte_array_data_bytes: column.field_3.field_16.field_1,
          repetition_level_histogram: column.field_3.field_16.field_2,
          definition_level_histogram: column.field_3.field_16.field_3,
        },
      },
      offset_index_offset: column.field_4,
      offset_index_length: column.field_5,
      column_index_offset: column.field_6,
      column_index_length: column.field_7,
      crypto_metadata: column.field_7,
      encrypted_column_metadata: column.field_8,
    })),
    total_byte_size: rowGroup.field_2,
    num_rows: rowGroup.field_3,
    sorting_columns: rowGroup.field_4?.map((/** @type {any} */ sortingColumn) => ({
      column_idx: sortingColumn.field_1,
      descending: sortingColumn.field_2,
      nulls_first: sortingColumn.field_3,
    })),
    file_offset: rowGroup.field_5,
    total_compressed_size: rowGroup.field_6,
    ordinal: rowGroup.field_7,
  }))
  const key_value_metadata = metadata.field_5?.map((/** @type {any} */ keyValue) => ({
    key: decode(keyValue.field_1),
    value: decode(keyValue.field_2),
  }))
  const created_by = decode(metadata.field_6)

  return {
    version,
    schema,
    num_rows,
    row_groups,
    key_value_metadata,
    created_by,
    metadata_length: metadataLength,
  }
}

/**
 * Return a tree of schema elements from parquet metadata.
 *
 * @param {FileMetaData} metadata parquet metadata object
 * @returns {import("../src/types.d.ts").SchemaTree} tree of schema elements
 */
export function parquetSchema(metadata) {
  return getSchemaPath(metadata.schema, [])[0]
}

/**
 * @param {any} logicalType
 * @returns {import("../src/types.d.ts").LogicalType | undefined}
 */
function logicalType(logicalType) {
  if (logicalType?.field_1) return { type: 'STRING' }
  if (logicalType?.field_2) return { type: 'MAP' }
  if (logicalType?.field_3) return { type: 'LIST' }
  if (logicalType?.field_4) return { type: 'ENUM' }
  if (logicalType?.field_5) return {
    type: 'DECIMAL',
    scale: logicalType.field_5.field_1,
    precision: logicalType.field_5.field_2,
  }
  if (logicalType?.field_6) return { type: 'DATE' }
  if (logicalType?.field_7) return {
    type: 'TIME',
    isAdjustedToUTC: logicalType.field_7.field_1,
    unit: timeUnit(logicalType.field_7.field_2),
  }
  if (logicalType?.field_8) return {
    type: 'TIMESTAMP',
    isAdjustedToUTC: logicalType.field_8.field_1,
    unit: timeUnit(logicalType.field_8.field_2),
  }
  if (logicalType?.field_10) return {
    type: 'INTEGER',
    bitWidth: logicalType.field_10.field_1,
    isSigned: logicalType.field_10.field_2,
  }
  if (logicalType?.field_11) return { type: 'NULL' }
  if (logicalType?.field_12) return { type: 'JSON' }
  if (logicalType?.field_13) return { type: 'BSON' }
  if (logicalType?.field_14) return { type: 'UUID' }
  if (logicalType?.field_15) return { type: 'FLOAT16' }
  return logicalType
}

/**
 * @param {any} unit
 * @returns {import("../src/types.d.ts").TimeUnit}
 */
function timeUnit(unit) {
  if (unit.field_1) return 'MILLIS'
  if (unit.field_2) return 'MICROS'
  if (unit.field_3) return 'NANOS'
  throw new Error('parquet time unit required')
}

/**
 * Convert column statistics based on column type.
 *
 * @param {any} stats
 * @param {SchemaElement} schema
 * @returns {import("../src/types.d.ts").Statistics}
 */
function convertStats(stats, schema) {
  return stats && {
    max: convertMetadata(stats.field_1, schema),
    min: convertMetadata(stats.field_2, schema),
    null_count: stats.field_3,
    distinct_count: stats.field_4,
    max_value: convertMetadata(stats.field_5, schema),
    min_value: convertMetadata(stats.field_6, schema),
    is_max_value_exact: stats.field_7,
    is_min_value_exact: stats.field_8,
  }
}

/**
 * @param {Uint8Array | undefined} value
 * @param {SchemaElement} schema
 * @returns {import('../src/types.d.ts').MinMaxType | undefined}
 */
export function convertMetadata(value, schema) {
  const { type, converted_type, logical_type } = schema
  if (value === undefined) return value
  if (type === 'BOOLEAN') return value[0] === 1
  if (type === 'BYTE_ARRAY') return new TextDecoder().decode(value)
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength)
  if (type === 'FLOAT' && view.byteLength === 4) return view.getFloat32(0, true)
  if (type === 'DOUBLE' && view.byteLength === 8) return view.getFloat64(0, true)
  if (type === 'INT32' && converted_type === 'DATE') return new Date(view.getInt32(0, true) * 86400000)
  if (type === 'INT64' && converted_type === 'TIMESTAMP_MICROS') return new Date(Number(view.getBigInt64(0, true) / 1000n))
  if (type === 'INT64' && converted_type === 'TIMESTAMP_MILLIS') return new Date(Number(view.getBigInt64(0, true)))
  if (type === 'INT64' && logical_type?.type === 'TIMESTAMP') return new Date(Number(view.getBigInt64(0, true)))
  if (type === 'INT32' && view.byteLength === 4) return view.getInt32(0, true)
  if (type === 'INT64' && view.byteLength === 8) return view.getBigInt64(0, true)
  if (converted_type === 'DECIMAL') return parseDecimal(value) * Math.pow(10, -(schema.scale || 0))
  if (logical_type?.type === 'FLOAT16') return parseFloat16(value)
  if (type === 'FIXED_LEN_BYTE_ARRAY') return value
  // assert(false)
  return value
}
