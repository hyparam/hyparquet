import { isListLike, isMapLike } from './schema.js'
import { decodeVariantColumn } from './variant.js'

/**
 * @import {AsyncPages, DecodedArray, ParquetParsers, SchemaTree} from '../src/types.js'
 */
/**
 * Cursor into the levels and values of a leaf column.
 *
 * @typedef {object} Leaf
 * @property {number} skipped rows before the first row
 * @property {Uint8Array} definitionLevels
 * @property {Uint8Array} repetitionLevels
 * @property {DecodedArray[]} data non-null values of each page
 * @property {DecodedArray} values current page of values
 * @property {number} maxDefinitionLevel
 * @property {number} i level index
 * @property {number} v value index in the current page
 * @property {number} page
 */
/**
 * @typedef {object} Context
 * @property {Map<string, AsyncPages>} columns
 * @property {ParquetParsers} parsers
 * @property {Leaf[]} leaves every leaf cursor, in schema order
 * @property {SchemaTree[]} [schemaPath]
 */

/**
 * Assemble rows of a top-level column from its leaf columns, according to
 * Dremel encoding. Walks the schema once per row with a cursor into each
 * leaf's levels and values, so only the output rows are allocated.
 *
 * @param {SchemaTree} schema top-level schema element
 * @param {Map<string, AsyncPages>} columns leaf columns by dotted path
 * @param {ParquetParsers} parsers
 * @param {number} rowStart first row to assemble, relative to the row group
 * @param {number} rowEnd
 * @param {SchemaTree[]} [schemaPath] assemble only the lists of this leaf, not structs
 * @returns {any[]}
 */
export function assembleColumn(schema, columns, parsers, rowStart, rowEnd, schemaPath) {
  /** @type {Leaf[]} */
  const leaves = []
  const read = reader(schema, 0, 0, { columns, parsers, leaves, schemaPath })
  for (const leaf of leaves) skipRows(leaf, rowStart - leaf.skipped)
  const rows = new Array(Math.max(rowEnd - rowStart, 0))
  for (let row = 0; row < rows.length; row++) rows[row] = read()
  return rows
}

/**
 * Build a function that reads the next value of a schema node.
 * Every leaf under the node must be positioned at the same record, and is
 * left at the start of the next one.
 *
 * @param {SchemaTree} node
 * @param {number} def definition level of the parent
 * @param {number} rep repetition level of the parent
 * @param {Context} ctx
 * @param {boolean} [unwrap] return an only child in place of a struct
 * @returns {() => any}
 */
function reader(node, def, rep, ctx, unwrap) {
  const { repetition_type } = node.element
  if (repetition_type !== 'REQUIRED') def++
  if (repetition_type === 'REPEATED') rep++
  const start = ctx.leaves.length
  const read = element(node, def, rep, ctx, unwrap)
  const leaves = ctx.leaves.slice(start)
  const [first] = leaves

  if (repetition_type === 'REPEATED') {
    return () => {
      if (first.definitionLevels[first.i] < def) return skip(leaves, [])
      // count elements first, push() over-allocates small lists
      const { repetitionLevels } = first
      let length = 1
      for (let i = first.i + 1; repetitionLevels[i] >= rep; i++) {
        if (repetitionLevels[i] === rep) length++
      }
      const list = new Array(length)
      for (let k = 0; k < length; k++) list[k] = read()
      return list
    }
  }
  if (repetition_type === 'OPTIONAL') {
    if (!node.children.length) {
      // optional leaf: one closure in the hottest path
      const { definitionLevels } = first
      return () => definitionLevels[first.i++] === def ? nextValue(first) : null
    }
    return () => first.definitionLevels[first.i] < def ? skip(leaves, null) : read()
  }
  return read
}

/**
 * Build a function that reads a defined value of a schema node.
 *
 * @param {SchemaTree} node
 * @param {number} def definition level of the node
 * @param {number} rep repetition level of the node
 * @param {Context} ctx
 * @param {boolean} [unwrap]
 * @returns {() => any}
 */
function element(node, def, rep, ctx, unwrap) {
  const { children } = node
  if (!children.length) {
    const column = ctx.columns.get(node.path.join('.'))
    if (!column?.definitionLevels || !column.repetitionLevels) {
      throw new Error(`parquet column missing levels: ${node.path.join('.')}`)
    }
    const { skipped, data, definitionLevels, repetitionLevels } = column
    const leaf = {
      skipped, definitionLevels, repetitionLevels, data,
      values: data[0] ?? [],
      maxDefinitionLevel: def,
      i: 0,
      v: 0,
      page: 0,
    }
    ctx.leaves.push(leaf)
    return () => {
      leaf.i++
      return nextValue(leaf)
    }
  }
  if (ctx.schemaPath) return reader(ctx.schemaPath[node.path.length + 1], def, rep, ctx)
  if (unwrap && children.length === 1) return reader(children[0], def, rep, ctx)
  if (isListLike(node)) return reader(children[0], def, rep, ctx, true)

  if (isMapLike(node)) {
    const start = ctx.leaves.length
    // levels of the repeated key_value group
    def++
    rep++
    const [key, value] = children[0].children.map(child => reader(child, def, rep, ctx))
    const leaves = ctx.leaves.slice(start)
    const [first] = leaves
    return () => {
      /** @type {Record<string, any>} */
      const map = {}
      if (first.definitionLevels[first.i] < def) return skip(leaves, map)
      do {
        const name = key()
        map[name] = value()
      } while (first.repetitionLevels[first.i] === rep)
      return map
    }
  }

  // struct
  const names = children.map(child => child.element.name)
  const fields = children.map(child => reader(child, def, rep, ctx))
  const variant = node.element.logical_type?.type === 'VARIANT'
  return () => {
    /** @type {Record<string, any>} */
    const struct = {}
    for (let k = 0; k < fields.length; k++) struct[names[k]] = fields[k]()
    return variant ? decodeVariantColumn(struct, ctx.parsers) : struct
  }
}

/**
 * Read the next non-null value of a leaf, moving on to the next page as needed.
 *
 * @param {Leaf} leaf
 * @returns {any}
 */
function nextValue(leaf) {
  while (leaf.v >= leaf.values.length) {
    leaf.v -= leaf.values.length
    leaf.values = leaf.data[++leaf.page]
  }
  return leaf.values[leaf.v++]
}

/**
 * A null or empty value takes one level entry in every leaf below it.
 *
 * @template T
 * @param {Leaf[]} leaves
 * @param {T} value
 * @returns {T}
 */
function skip(leaves, value) {
  for (let k = 0; k < leaves.length; k++) leaves[k].i++
  return value
}

/**
 * Advance a leaf past whole rows.
 *
 * @param {Leaf} leaf
 * @param {number} rows
 */
function skipRows(leaf, rows) {
  const { definitionLevels, repetitionLevels, maxDefinitionLevel } = leaf
  let { i, v } = leaf
  for (; rows > 0; rows--) {
    do {
      // no definition levels means every value is defined
      if (!(definitionLevels[i] < maxDefinitionLevel)) v++
      i++
    } while (repetitionLevels[i])
  }
  leaf.i = i
  leaf.v = v
}
