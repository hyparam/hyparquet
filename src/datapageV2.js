import { decompressPage } from './column.js'
import { deltaBinaryUnpack, deltaByteArray, deltaLengthByteArray } from './delta.js'
import { bitWidth, byteStreamSplit, readRleBitPackedHybrid } from './encoding.js'
import { readPlain } from './plain.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'

/**
 * Read a data page from the given Uint8Array.
 *
 * @typedef {import("./types.d.ts").DataPage} DataPage
 * @typedef {import("./types.d.ts").ColumnMetaData} ColumnMetaData
 * @typedef {import("./types.d.ts").Compressors} Compressors
 * @typedef {import("./types.d.ts").DataPageHeaderV2} DataPageHeaderV2
 * @typedef {import("./types.d.ts").SchemaTree} SchemaTree
 * @param {Uint8Array} compressedBytes raw page data
 * @param {import("./types.d.ts").PageHeader} ph page header
 * @param {SchemaTree[]} schemaPath
 * @param {ColumnMetaData} columnMetadata
 * @param {Compressors | undefined} compressors
 * @returns {DataPage} definition levels, repetition levels, and array of values
 */
export function readDataPageV2(compressedBytes, ph, schemaPath, columnMetadata, compressors) {
  const view = new DataView(compressedBytes.buffer, compressedBytes.byteOffset, compressedBytes.byteLength)
  const reader = { view, offset: 0 }
  const { codec, type } = columnMetadata
  const daph2 = ph.data_page_header_v2
  if (!daph2) throw new Error('parquet data page header v2 is undefined')

  // repetition levels
  const repetitionLevels = readRepetitionLevelsV2(reader, daph2, schemaPath)
  // assert(reader.offset === daph2.repetition_levels_byte_length)

  // definition levels
  const definitionLevels = readDefinitionLevelsV2(reader, daph2, schemaPath)
  // assert(reader.offset === daph2.repetition_levels_byte_length + daph2.definition_levels_byte_length)

  const uncompressedPageSize = ph.uncompressed_page_size - daph2.definition_levels_byte_length - daph2.repetition_levels_byte_length

  let page = compressedBytes.subarray(reader.offset)
  page = decompressPage(page, uncompressedPageSize, codec, compressors)
  const pageView = new DataView(page.buffer, page.byteOffset, page.byteLength)
  const pageReader = { view: pageView, offset: 0 }

  // read values based on encoding
  /** @type {import('./types.d.ts').DecodedArray} */
  let dataPage
  const nValues = daph2.num_values - daph2.num_nulls
  if (daph2.encoding === 'PLAIN') {
    const { type_length } = schemaPath[schemaPath.length - 1].element
    dataPage = readPlain(pageReader, type, nValues, type_length)
  } else if (daph2.encoding === 'RLE') {
    // assert(columnMetadata.type === 'BOOLEAN')
    dataPage = new Array(nValues)
    readRleBitPackedHybrid(pageReader, 1, 0, dataPage)
    dataPage = dataPage.map(x => !!x)
  } else if (
    daph2.encoding === 'PLAIN_DICTIONARY' ||
    daph2.encoding === 'RLE_DICTIONARY'
  ) {
    const bitWidth = pageView.getUint8(pageReader.offset++)
    dataPage = new Array(nValues)
    readRleBitPackedHybrid(pageReader, bitWidth, uncompressedPageSize - 1, dataPage)
  } else if (daph2.encoding === 'DELTA_BINARY_PACKED') {
    const int32 = type === 'INT32'
    dataPage = int32 ? new Int32Array(nValues) : new BigInt64Array(nValues)
    deltaBinaryUnpack(pageReader, nValues, dataPage)
  } else if (daph2.encoding === 'DELTA_LENGTH_BYTE_ARRAY') {
    dataPage = new Array(nValues)
    deltaLengthByteArray(pageReader, nValues, dataPage)
  } else if (daph2.encoding === 'DELTA_BYTE_ARRAY') {
    dataPage = new Array(nValues)
    deltaByteArray(pageReader, nValues, dataPage)
  } else if (daph2.encoding === 'BYTE_STREAM_SPLIT') {
    const { type_length } = schemaPath[schemaPath.length - 1].element
    dataPage = byteStreamSplit(reader, nValues, type, type_length)
  } else {
    throw new Error(`parquet unsupported encoding: ${daph2.encoding}`)
  }

  return { definitionLevels, repetitionLevels, dataPage }
}

/**
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader
 * @param {DataPageHeaderV2} daph2 data page header v2
 * @param {SchemaTree[]} schemaPath
 * @returns {any[]} repetition levels
 */
export function readRepetitionLevelsV2(reader, daph2, schemaPath) {
  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  if (!maxRepetitionLevel) return []

  const values = new Array(daph2.num_values)
  readRleBitPackedHybrid(
    reader, bitWidth(maxRepetitionLevel), daph2.repetition_levels_byte_length, values
  )
  return values
}

/**
 * @param {DataReader} reader
 * @param {DataPageHeaderV2} daph2 data page header v2
 * @param {SchemaTree[]} schemaPath
 * @returns {number[] | undefined} definition levels
 */
function readDefinitionLevelsV2(reader, daph2, schemaPath) {
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  if (maxDefinitionLevel) {
    // V2 we know the length
    const values = new Array(daph2.num_values)
    readRleBitPackedHybrid(reader, bitWidth(maxDefinitionLevel), daph2.definition_levels_byte_length, values)
    return values
  }
}
