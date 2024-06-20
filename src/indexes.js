import { BoundaryOrder } from './constants.js'
import { convertMetadata } from './metadata.js'
import { deserializeTCompactProtocol } from './thrift.js'

/**
 * @typedef {import('./types.d.ts').DataReader} DataReader
 * @param {DataReader} reader
 * @param {import('./types.d.ts').SchemaElement} schema
 * @returns {import('./types.d.ts').ColumnIndex}
 */
export function readColumnIndex(reader, schema) {
  const thrift = deserializeTCompactProtocol(reader)
  return {
    null_pages: thrift.field_1,
    min_values: thrift.field_2.map((/** @type {any} */ m) => convertMetadata(m, schema)),
    max_values: thrift.field_3.map((/** @type {any} */ m) => convertMetadata(m, schema)),
    boundary_order: BoundaryOrder[thrift.field_4],
    null_counts: thrift.field_5,
    repetition_level_histograms: thrift.field_6,
    definition_level_histograms: thrift.field_7,
  }
}

/**
 * @param {DataReader} reader
 * @returns {import('./types.d.ts').OffsetIndex}
 */
export function readOffsetIndex(reader) {
  const thrift = deserializeTCompactProtocol(reader)
  return {
    page_locations: thrift.field_1.map(pageLocation),
    unencoded_byte_array_data_bytes: thrift.field_2,
  }
}

/**
 * @param {any} loc
 * @returns {import('./types.d.ts').PageLocation}
 */
function pageLocation(loc) {
  return {
    offset: loc.field_1,
    compressed_page_size: loc.field_2,
    first_row_index: loc.field_3,
  }
}
