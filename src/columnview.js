/**
 * @import {ColumnLevelPage, DecodedArray, ParquetColumnLeaf, ParquetColumnView, ParquetParsers, SchemaTree} from '../src/types.js'
 */

import { assembleLists, assembleNested } from './assemble.js'
import { DEFAULT_PARSERS } from './convert.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'
import { flatten } from './utils.js'

/**
 * Keep physical leaf values and their row boundaries in columns. A row is
 * assembled only when requested by get() or toArray().
 *
 * @param {SchemaTree} schema top-level selected column
 * @param {{pathInSchema: string[], schemaPath: SchemaTree[], pages: ColumnLevelPage[]}[]} decodedLeaves
 * @param {number} groupStart absolute first row of the row group
 * @param {number} groupRows number of rows in the row group
 * @param {number} rowStart absolute first requested row
 * @param {number} rowEnd absolute end of requested rows
 * @param {Partial<ParquetParsers> | undefined} parsers
 * @returns {ParquetColumnView}
 */
export function createColumnView(schema, decodedLeaves, groupStart, groupRows, rowStart, rowEnd, parsers) {
  const leaves = decodedLeaves.map(leaf => indexLeaf(leaf, groupRows))
  const allParsers = { ...DEFAULT_PARSERS, ...parsers }
  const baseRow = rowStart - groupStart
  const length = rowEnd - rowStart

  /**
   * @param {number} index
   * @returns {any}
   */
  function get(index) {
    if (!Number.isSafeInteger(index) || index < 0 || index >= length) {
      throw new RangeError('parquet column view index out of range')
    }
    const row = baseRow + index
    if (!schema.children.length) return materializeLeafRow(leaves[0], row)[0]

    /** @type {Map<string, DecodedArray>} */
    const subcolumnData = new Map()
    for (const leaf of leaves) {
      subcolumnData.set(leaf.pathInSchema.join('.'), materializeLeafRow(leaf, row))
    }
    assembleNested(subcolumnData, schema, allParsers)
    const assembled = subcolumnData.get(schema.path.join('.'))
    if (!assembled) throw new Error('parquet column view assembly failed')
    return assembled[0]
  }

  /** @returns {DecodedArray} */
  function toArray() {
    if (!leaves.length) return []
    /** @type {Map<string, DecodedArray>} */
    const subcolumnData = new Map()
    for (const leaf of leaves) {
      /** @type {DecodedArray[]} */
      const chunks = []
      /** @type {DecodedArray | undefined} */
      let previous
      for (const page of leaf.pages) {
        const output = Array.isArray(previous) ? previous : []
        const assembled = assembleLists(
          output, page.definitionLevels, page.repetitionLevels, page.values, leaf.schemaPath
        )
        if (assembled !== previous) chunks.push(assembled)
        previous = assembled
      }
      const complete = flatten(chunks)
      const selected = baseRow === 0 && length === complete.length
        ? complete : complete.slice(baseRow, baseRow + length)
      subcolumnData.set(leaf.pathInSchema.join('.'), selected)
    }
    if (!schema.children.length) {
      const column = subcolumnData.get(schema.path.join('.'))
      if (!column) throw new Error('parquet column view assembly failed')
      return column
    }
    assembleNested(subcolumnData, schema, allParsers)
    const assembled = subcolumnData.get(schema.path.join('.'))
    if (!assembled) throw new Error('parquet column view assembly failed')
    return assembled
  }

  return { groupStart, rowStart, rowEnd, length, leaves, get, toArray }
}

/**
 * @param {{pathInSchema: string[], schemaPath: SchemaTree[], pages: ColumnLevelPage[]}} leaf
 * @param {number} expectedRows
 * @returns {ParquetColumnLeaf & { maxDefinitionLevel: number }}
 */
function indexLeaf(leaf, expectedRows) {
  const maxDefinitionLevel = getMaxDefinitionLevel(leaf.schemaPath)
  const maxRepetitionLevel = getMaxRepetitionLevel(leaf.schemaPath)
  const rowOffsets = new Uint32Array(expectedRows + 1)
  const valueOffsets = new Uint32Array(expectedRows + 1)
  let eventCount = 0
  let valueCount = 0
  let rowCount = 0
  const pages = leaf.pages.map(page => {
    const eventStart = eventCount
    const valueStart = valueCount
    const count = page.repetitionLevels.length || page.definitionLevels.length || page.values.length
    if (maxRepetitionLevel && count && !page.repetitionLevels.length) {
      throw new Error('parquet column view missing repetition levels')
    }
    for (let i = 0; i < count; i++) {
      if (!eventCount || !page.repetitionLevels.length || page.repetitionLevels[i] === 0) {
        if (rowCount >= expectedRows) throw new Error('parquet column view row count exceeds metadata')
        rowOffsets[rowCount] = eventCount
        valueOffsets[rowCount] = valueCount
        rowCount++
      }
      if (!page.definitionLevels.length || page.definitionLevels[i] === maxDefinitionLevel) valueCount++
      eventCount++
    }
    return { ...page, eventStart, eventEnd: eventCount, valueStart }
  })
  rowOffsets[rowCount] = eventCount
  valueOffsets[rowCount] = valueCount
  if (rowCount !== expectedRows) {
    throw new Error(`parquet column view row count mismatch: ${rowCount} != ${expectedRows}`)
  }
  return {
    pathInSchema: leaf.pathInSchema,
    schemaPath: leaf.schemaPath,
    maxDefinitionLevel,
    pages,
    rowOffsets,
    valueOffsets,
  }
}

/**
 * @param {ReturnType<typeof indexLeaf>} leaf
 * @param {number} row row within row group
 * @returns {DecodedArray}
 */
function materializeLeafRow(leaf, row) {
  const first = leaf.rowOffsets[row]
  const last = leaf.rowOffsets[row + 1]
  let nextValue = leaf.valueOffsets[row]
  /** @type {DecodedArray} */
  let output = []
  let low = 0
  let high = leaf.pages.length
  while (low < high) {
    const middle = Math.floor((low + high) / 2)
    if (leaf.pages[middle].eventEnd <= first) low = middle + 1
    else high = middle
  }
  for (let pageIndex = low; pageIndex < leaf.pages.length; pageIndex++) {
    const page = leaf.pages[pageIndex]
    if (page.eventStart >= last) break
    const start = Math.max(first, page.eventStart)
    const end = Math.min(last, page.eventEnd)
    if (start >= end) continue
    const localStart = start - page.eventStart
    const localEnd = end - page.eventStart
    const definitions = page.definitionLevels.length ? page.definitionLevels.slice(localStart, localEnd) : []
    const repetitions = page.repetitionLevels.length ? page.repetitionLevels.slice(localStart, localEnd) : []
    let defined = localEnd - localStart
    if (definitions.length) {
      defined = 0
      for (const level of definitions) if (level === leaf.maxDefinitionLevel) defined++
    }
    const valueStart = nextValue - page.valueStart
    const values = page.values.slice(valueStart, valueStart + defined)
    nextValue += defined
    // @ts-expect-error the assembler accepts a typed first result and an array thereafter
    output = assembleLists(output, definitions, repetitions, values, leaf.schemaPath)
  }
  return output
}
