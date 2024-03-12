import { CompressionCodec, ConvertedType, Encoding, FieldRepetitionType, ParquetType } from './constants.js'
import { schemaTree } from './schema.js'
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
 * @typedef {import("./types.d.ts").AsyncBuffer} AsyncBuffer
 * @typedef {import("./types.d.ts").FileMetaData} FileMetaData
 * @param {AsyncBuffer} asyncBuffer parquet file contents
 * @param {number} initialFetchSize initial fetch size in bytes
 * @returns {Promise<FileMetaData>} parquet metadata object
 */
export async function parquetMetadataAsync(asyncBuffer, initialFetchSize = 1 << 19 /* 512kb */) {
  // fetch last bytes (footer) of the file
  const footerOffset = Math.max(0, asyncBuffer.byteLength - initialFetchSize)
  const footerBuffer = await asyncBuffer.slice(footerOffset)

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
    combinedView.set(new Uint8Array(metadataBuffer), 0)
    combinedView.set(new Uint8Array(footerBuffer), footerOffset - metadataOffset)
    return parquetMetadata(combinedBuffer)
  } else {
    // parse metadata from the footer
    return parquetMetadata(footerBuffer)
  }
}

/**
 * Read parquet metadata from a buffer
 *
 * @param {ArrayBuffer} arrayBuffer parquet file contents
 * @returns {FileMetaData} parquet metadata object
 */
export function parquetMetadata(arrayBuffer) {
  // DataView for easier manipulation of the buffer
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
  const { value: metadata } = deserializeTCompactProtocol(view.buffer, view.byteOffset + metadataOffset)

  // Parse parquet metadata from thrift data
  const version = metadata.field_1
  const schema = metadata.field_2.map((/** @type {any} */ field) => ({
    type: ParquetType[field.field_1],
    type_length: field.field_2,
    repetition_type: FieldRepetitionType[field.field_3],
    name: field.field_4,
    num_children: field.field_5,
    converted_type: ConvertedType[field.field_6],
    scale: field.field_7,
    precision: field.field_8,
    field_id: field.field_9,
    logical_type: logicalType(field.field_10),
  }))
  const num_rows = metadata.field_3
  const row_groups = metadata.field_4.map((/** @type {any} */ rowGroup) => ({
    columns: rowGroup.field_1.map((/** @type {any} */ column) => ({
      file_path: column.field_1,
      file_offset: column.field_2,
      meta_data: column.field_3 && {
        type: ParquetType[column.field_3.field_1],
        encodings: column.field_3.field_2?.map((/** @type {number} */ e) => Encoding[e]),
        path_in_schema: column.field_3.field_3,
        codec: CompressionCodec[column.field_3.field_4],
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
        encoding_stats: column.field_3.field_13?.map((/** @type {any} */ encodingStat) => ({
          page_type: encodingStat.field_1,
          encoding: Encoding[encodingStat.field_2],
          count: encodingStat.field_3,
        })),
      },
    })),
    total_byte_size: rowGroup.field_2,
    num_rows: rowGroup.field_3,
    sorting_columns: rowGroup.field_4?.map((/** @type {any} */ sortingColumn) => ({
      column_idx: sortingColumn.field_1,
      descending: sortingColumn.field_2,
      nulls_first: sortingColumn.field_3,
    })),
  }))
  const key_value_metadata = metadata.field_5?.map((/** @type {any} */ keyValue) => ({
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
    metadata_length: metadataLength,
  }
}

/**
 * Return a tree of schema elements from parquet metadata.
 *
 * @typedef {import("./types.d.ts").SchemaTree} SchemaTree
 * @param {FileMetaData} metadata parquet metadata object
 * @returns {SchemaTree} tree of schema elements
 */
export function parquetSchema(metadata) {
  return schemaTree(metadata.schema, 0)
}

/**
 * Parse logical type by type.
 *
 * @typedef {import("./types.d.ts").LogicalType} LogicalType
 * @param {any} logicalType
 * @returns {LogicalType | undefined}
 */
function logicalType(logicalType) {
  if (logicalType?.field_5) {
    return {
      logicalType: 'DECIMAL',
      scale: logicalType.field_5.field_1,
      precision: logicalType.field_5.field_2,
    }
  }
  // TODO: TimestampType
  // TOFO: TimeType
  if (logicalType?.field_10) {
    return {
      logicalType: 'INTEGER',
      bitWidth: logicalType.field_10.field_1,
      isSigned: logicalType.field_10.field_2,
    }
  }
  if (logicalType) {
    return logicalType
  }
}
