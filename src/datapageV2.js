import { decompressPage } from './column.js'
import { readRleBitPackedHybrid, widthFromMaxInt } from './encoding.js'
import { readPlain } from './plain.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'
import { readVarInt, readZigZagBigInt } from './thrift.js'

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
 * @param {SchemaTree[]} schemaPath
 * @param {ColumnMetaData} columnMetadata
 * @param {Compressors | undefined} compressors
 * @returns {DataPage} definition levels, repetition levels, and array of values
 */
export function readDataPageV2(compressedBytes, ph, schemaPath, columnMetadata, compressors) {
  const view = new DataView(compressedBytes.buffer, compressedBytes.byteOffset, compressedBytes.byteLength)
  const reader = { view, offset: 0 }

  const daph2 = ph.data_page_header_v2
  if (!daph2) throw new Error('parquet data page header v2 is undefined')

  // repetition levels
  const repetitionLevels = readRepetitionLevelsV2(reader, daph2, schemaPath)
  // assert(reader.offset === daph2.repetition_levels_byte_length)

  // definition levels
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  const definitionLevels = readDefinitionLevelsV2(reader, daph2, maxDefinitionLevel)
  // assert(reader.offset === daph2.repetition_levels_byte_length + daph2.definition_levels_byte_length)

  const uncompressedPageSize = ph.uncompressed_page_size - daph2.definition_levels_byte_length - daph2.repetition_levels_byte_length

  let page = compressedBytes.subarray(reader.offset)
  if (daph2.is_compressed && columnMetadata.codec !== 'UNCOMPRESSED') {
    page = decompressPage(page, uncompressedPageSize, columnMetadata.codec, compressors)
  }
  const pageView = new DataView(page.buffer, page.byteOffset, page.byteLength)

  // read values based on encoding
  /** @type {import('./types.d.ts').DecodedArray} */
  let dataPage = []
  const nValues = daph2.num_values - daph2.num_nulls
  if (daph2.encoding === 'PLAIN') {
    const pageReader = { view: pageView, offset: 0 }
    const { type_length } = schemaPath[schemaPath.length - 1].element
    dataPage = readPlain(pageReader, columnMetadata.type, nValues, type_length)
  } else if (daph2.encoding === 'RLE') {
    const bitWidth = 1
    const pageReader = { view: pageView, offset: 4 }
    dataPage = new Array(nValues)
    readRleBitPackedHybrid(pageReader, bitWidth, uncompressedPageSize, dataPage)
  } else if (
    daph2.encoding === 'PLAIN_DICTIONARY' ||
    daph2.encoding === 'RLE_DICTIONARY'
  ) {
    const bitWidth = pageView.getUint8(0)
    const pageReader = { view: pageView, offset: 1 }
    dataPage = new Array(nValues)
    readRleBitPackedHybrid(pageReader, bitWidth, uncompressedPageSize, dataPage)
  } else if (daph2.encoding === 'DELTA_BINARY_PACKED') {
    const int32 = columnMetadata.type === 'INT32'
    dataPage = int32 ? new Int32Array(nValues) : new BigInt64Array(nValues)
    deltaBinaryUnpack(page, nValues, dataPage)
  } else {
    throw new Error(`parquet unsupported encoding: ${daph2.encoding}`)
  }

  return { definitionLevels, repetitionLevels, dataPage }
}

/**
 * Read the repetition levels from this page, if any.
 *
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader data view for the page
 * @param {DataPageHeaderV2} daph2 data page header
 * @param {SchemaTree[]} schemaPath
 * @returns {any[]} repetition levels and number of bytes read
 */
export function readRepetitionLevelsV2(reader, daph2, schemaPath) {
  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  if (!maxRepetitionLevel) return []

  const bitWidth = widthFromMaxInt(maxRepetitionLevel)
  // num_values is index 1 for either type of page header
  const values = new Array(daph2.num_values)
  readRleBitPackedHybrid(
    reader, bitWidth, daph2.repetition_levels_byte_length, values
  )
  return values
}

/**
 * Read the definition levels from this page, if any.
 *
 * @param {DataReader} reader data view for the page
 * @param {DataPageHeaderV2} daph2 data page header v2
 * @param {number} maxDefinitionLevel
 * @returns {number[] | undefined} definition levels and number of bytes read
 */
function readDefinitionLevelsV2(reader, daph2, maxDefinitionLevel) {
  if (maxDefinitionLevel) {
    // not the same as V1, because we know the length
    const bitWidth = widthFromMaxInt(maxDefinitionLevel)
    const values = new Array(daph2.num_values)
    readRleBitPackedHybrid(reader, bitWidth, daph2.definition_levels_byte_length, values)
    return values
  }
}

/**
 * Unpack the delta binary packed encoding.
 *
 * @param {Uint8Array} page page data
 * @param {number} nValues number of values to read
 * @param {Int32Array | BigInt64Array} values array to write to
 */
function deltaBinaryUnpack(page, nValues, values) {
  const int32 = values instanceof Int32Array
  const view = new DataView(page.buffer, page.byteOffset, page.byteLength)
  const reader = { view, offset: 0 }
  const blockSize = readVarInt(reader)
  const miniblockPerBlock = readVarInt(reader)
  let count = readVarInt(reader)
  let value = readZigZagBigInt(reader) // first value
  let valueIndex = 0
  values[valueIndex++] = int32 ? Number(value) : value

  const valuesPerMiniblock = blockSize / miniblockPerBlock

  while (valueIndex < nValues) {
    const minDelta = readZigZagBigInt(reader)
    const bitWidths = new Uint8Array(miniblockPerBlock)
    for (let i = 0; i < miniblockPerBlock; i++) {
      bitWidths[i] = page[reader.offset++]
    }

    for (let i = 0; i < miniblockPerBlock; i++) {
      let miniblockCount = Math.min(count, valuesPerMiniblock)
      const bitWidth = BigInt(bitWidths[i])
      if (bitWidth) {
        if (count > 1) {
          const mask = (1n << bitWidth) - 1n
          let bitpackPos = 0n
          while (count && miniblockCount) {
            let bits = (BigInt(view.getUint8(reader.offset)) >> bitpackPos) & mask // TODO: don't re-read value every time
            bitpackPos += bitWidth
            while (bitpackPos >= 8) {
              bitpackPos -= 8n
              reader.offset++
              bits |= (BigInt(view.getUint8(reader.offset)) << bitWidth - bitpackPos) & mask
            }
            const delta = minDelta + bits
            value += delta
            values[valueIndex++] = int32 ? Number(value) : value
            count--
            miniblockCount--
          }
        }
      } else {
        for (let j = 0; j < valuesPerMiniblock && valueIndex < nValues; j++) {
          value += minDelta
          values[valueIndex++] = int32 ? Number(value) : value
        }
      }
    }
  }
}
