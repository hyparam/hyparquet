import { decompressPage } from './column.js'
import { readPlain, readRleBitPackedHybrid, widthFromMaxInt } from './encoding.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'
import { readVarInt, readZigZag } from './thrift.js'

/**
 * Read a data page from the given Uint8Array.
 *
 * @typedef {import("./types.d.ts").DataPage} DataPage
 * @typedef {import("./types.d.ts").ColumnMetaData} ColumnMetaData
 * @typedef {import("./types.d.ts").Compressors} Compressors
 * @typedef {import("./types.d.ts").DataPageHeaderV2} DataPageHeaderV2
 * @typedef {import("./types.d.ts").SchemaTree} SchemaTree
 * @param {Uint8Array} compressedBytes raw page data (should already be decompressed)
 * @param {import("./types.d.ts").PageHeader} ph page header
 * @param {SchemaTree[]} schemaPath schema path for the column
 * @param {ColumnMetaData} columnMetadata metadata for the column
 * @param {Compressors | undefined} compressors
 * @returns {DataPage} definition levels, repetition levels, and array of values
 */
export function readDataPageV2(compressedBytes, ph, schemaPath, columnMetadata, compressors) {
  const view = new DataView(compressedBytes.buffer, compressedBytes.byteOffset, compressedBytes.byteLength)
  const reader = { view, offset: 0 }
  /** @type {any} */
  let values = []

  const daph2 = ph.data_page_header_v2
  if (!daph2) throw new Error('parquet data page header v2 is undefined')

  // repetition levels
  const repetitionLevels = readRepetitionLevelsV2(reader, daph2, schemaPath)

  if (reader.offset !== daph2.repetition_levels_byte_length) {
    throw new Error(`parquet repetition levels byte length ${reader.offset} does not match expected ${daph2.repetition_levels_byte_length}`)
  }

  // definition levels
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  const definitionLevels = readDefinitionLevelsV2(reader, daph2, maxDefinitionLevel)

  if (reader.offset !== daph2.repetition_levels_byte_length + daph2.definition_levels_byte_length) {
    throw new Error(`parquet definition levels byte length ${reader.offset} does not match expected ${daph2.repetition_levels_byte_length + daph2.definition_levels_byte_length}`)
  }

  const uncompressedPageSize = ph.uncompressed_page_size - daph2.definition_levels_byte_length - daph2.repetition_levels_byte_length

  // read values based on encoding
  const nValues = daph2.num_values - daph2.num_nulls
  if (daph2.encoding === 'PLAIN') {
    const { element } = schemaPath[schemaPath.length - 1]
    const utf8 = element.converted_type === 'UTF8'
    let page = compressedBytes.slice(reader.offset)
    if (daph2.is_compressed && columnMetadata.codec !== 'UNCOMPRESSED') {
      page = decompressPage(page, uncompressedPageSize, columnMetadata.codec, compressors)
    }
    const pageView = new DataView(page.buffer, page.byteOffset, page.byteLength)
    const pageReader = { view: pageView, offset: 0 }
    values = readPlain(pageReader, columnMetadata.type, nValues, utf8)
  } else if (daph2.encoding === 'RLE') {
    const page = decompressPage(compressedBytes, uncompressedPageSize, columnMetadata.codec, compressors)
    const pageView = new DataView(page.buffer, page.byteOffset, page.byteLength)
    const bitWidth = 1
    if (daph2.num_nulls) {
      throw new Error('parquet RLE encoding with nulls not supported')
    } else {
      const pageReader = { view: pageView, offset: 4 }
      values = readRleBitPackedHybrid(
        pageReader, bitWidth, uncompressedPageSize, nValues
      )
    }
  } else if (
    daph2.encoding === 'PLAIN_DICTIONARY' ||
    daph2.encoding === 'RLE_DICTIONARY'
  ) {
    compressedBytes = compressedBytes.subarray(reader.offset)
    const page = decompressPage(compressedBytes, uncompressedPageSize, columnMetadata.codec, compressors)
    const pageView = new DataView(page.buffer, page.byteOffset, page.byteLength)
    const bitWidth = pageView.getUint8(0)
    const pageReader = { view: pageView, offset: 1 }
    const value = readRleBitPackedHybrid(
      pageReader, bitWidth, uncompressedPageSize, nValues
    )
    values = value
  } else if (daph2.encoding === 'DELTA_BINARY_PACKED') {
    if (daph2.num_nulls) throw new Error('parquet delta-int not supported')
    const codec = daph2.is_compressed ? columnMetadata.codec : 'UNCOMPRESSED'
    const page = decompressPage(compressedBytes, uncompressedPageSize, codec, compressors)
    deltaBinaryUnpack(page, nValues, values)
  } else {
    throw new Error(`parquet unsupported encoding: ${daph2.encoding}`)
  }

  return { definitionLevels, repetitionLevels, value: values }
}

/**
 * Read the repetition levels from this page, if any.
 *
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader data view for the page
 * @param {DataPageHeaderV2} daph2 data page header
 * @param {SchemaTree[]} schemaPath schema path for the column
 * @returns {any[]} repetition levels and number of bytes read
 */
export function readRepetitionLevelsV2(reader, daph2, schemaPath) {
  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  if (!maxRepetitionLevel) return []

  const bitWidth = widthFromMaxInt(maxRepetitionLevel)
  // num_values is index 1 for either type of page header
  return readRleBitPackedHybrid(
    reader, bitWidth, daph2.repetition_levels_byte_length, daph2.num_values
  )
}

/**
 * Read the definition levels from this page, if any.
 *
 * @param {DataReader} reader data view for the page
 * @param {DataPageHeaderV2} daph2 data page header v2
 * @param {number} maxDefinitionLevel maximum definition level for this column
 * @returns {number[] | undefined} definition levels and number of bytes read
 */
function readDefinitionLevelsV2(reader, daph2, maxDefinitionLevel) {
  if (maxDefinitionLevel) {
    // not the same as V1, because we know the length
    const bitWidth = widthFromMaxInt(maxDefinitionLevel)
    return readRleBitPackedHybrid(
      reader, bitWidth, daph2.definition_levels_byte_length, daph2.num_values
    )
  }
}

/**
 * Unpack the delta binary packed encoding.
 *
 * @param {Uint8Array} page page data
 * @param {number} nValues number of values to read
 * @param {any[]} values array to write to
 */
function deltaBinaryUnpack(page, nValues, values) {
  const dataView = new DataView(page.buffer, page.byteOffset, page.byteLength)
  const [blockSize, index1] = readVarInt(dataView, 0)
  const [miniblockPerBlock, index2] = readVarInt(dataView, index1)
  const [count, index3] = readVarInt(dataView, index2)
  let [value, offset] = readZigZag(dataView, index3)

  const valuesPerMiniblock = blockSize / miniblockPerBlock

  for (let valueIndex = 0; valueIndex < nValues;) {
    const [minDelta, index4] = readZigZag(dataView, offset)
    offset = index4
    const bitWidths = new Uint8Array(miniblockPerBlock)
    for (let i = 0; i < miniblockPerBlock; i++, offset++) {
      bitWidths[i] = page[offset]
    }

    for (let i = 0; i < miniblockPerBlock; i++) {
      const bitWidth = bitWidths[i]
      if (bitWidth) {
        if (count > 1) {
          // no more diffs if on last value, delta read bitpacked
          let data = 0
          let stop = -bitWidth
          // only works for bitWidth < 31
          const mask = (1 << bitWidth) - 1
          while (count) {
            if (stop < 0) {
              // fails when data gets too large
              data = (data << 8) | dataView.getUint8(offset++)
              stop += 8
            } else {
              values.push((data >> stop) & mask)
            }
          }
        }
      } else {
        for (let j = 0; j < valuesPerMiniblock && valueIndex < nValues; j++, valueIndex++) {
          values[valueIndex] = value
          value += minDelta
        }
      }
    }
  }
}
