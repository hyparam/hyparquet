import { getMaxDefinitionLevel, isListLike, isMapLike } from './schema.js'
import { decodeVariantColumn } from './variant.js'

/**
 * Reconstructs a complex nested structure from flat arrays of values and
 * definition and repetition levels, according to Dremel encoding.
 *
 * @param {any[]} output
 * @param {number[] | undefined} definitionLevels
 * @param {number[]} repetitionLevels
 * @param {DecodedArray} values
 * @param {SchemaTree[]} schemaPath
 * @returns {DecodedArray}
 */
export function assembleLists(output, definitionLevels, repetitionLevels, values, schemaPath) {
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  // If no def/rep levels, synthesize def levels at max
  if (!definitionLevels?.length && !repetitionLevels.length) {
    if (!maxDefinitionLevel || !values.length) return values
    definitionLevels = new Array(values.length).fill(maxDefinitionLevel)
  }
  const n = definitionLevels?.length || repetitionLevels.length
  const repetitionPath = schemaPath.map(({ element }) => element.repetition_type)
  let valueIndex = 0

  // No repetition: one value per row. Rows where an ancestor struct is null
  // get a null marker so assembleNested can tell which ancestor is null.
  if (!repetitionLevels.length && definitionLevels && !repetitionPath.includes('REPEATED', 1)) {
    const parentDefinitionLevel = repetitionPath.at(-1) === 'REQUIRED' ? maxDefinitionLevel : maxDefinitionLevel - 1
    for (let i = 0; i < n; i++) {
      const def = definitionLevels[i]
      if (def === maxDefinitionLevel) output.push(values[valueIndex++])
      else output.push(def < parentDefinitionLevel ? nullAt(def) : null)
    }
    return output
  }

  // Track state of nested structures
  const containerStack = [output]
  let currentContainer = output
  let currentDepth = 0 // schema depth
  let currentDefLevel = 0 // list depth
  let currentRepLevel = 0

  if (repetitionLevels[0]) {
    // continue previous row
    while (currentDepth < repetitionPath.length - 2 && currentRepLevel < repetitionLevels[0]) {
      currentDepth++
      if (repetitionPath[currentDepth] !== 'REQUIRED') {
        // go into last list
        currentContainer = currentContainer.at(-1)
        containerStack.push(currentContainer)
        currentDefLevel++
      }
      if (repetitionPath[currentDepth] === 'REPEATED') currentRepLevel++
    }
  }

  for (let i = 0; i < n; i++) {
    // assert(currentDefLevel === containerStack.length - 1)
    const def = definitionLevels?.length ? definitionLevels[i] : maxDefinitionLevel
    const rep = repetitionLevels[i]

    // Pop up to start of rep level
    while (currentDepth && (rep < currentRepLevel || repetitionPath[currentDepth] !== 'REPEATED')) {
      if (repetitionPath[currentDepth] !== 'REQUIRED') {
        containerStack.pop()
        currentDefLevel--
      }
      if (repetitionPath[currentDepth] === 'REPEATED') currentRepLevel--
      currentDepth--
    }
    // @ts-expect-error won't be empty
    currentContainer = containerStack.at(-1)

    // Go deeper to end of definition level
    while (
      (currentDepth < repetitionPath.length - 2 || repetitionPath[currentDepth + 1] === 'REPEATED') &&
      (currentDefLevel < def || repetitionPath[currentDepth + 1] === 'REQUIRED')
    ) {
      currentDepth++
      if (repetitionPath[currentDepth] !== 'REQUIRED') {
        /** @type {any[]} */
        const newList = []
        currentContainer.push(newList)
        currentContainer = newList
        containerStack.push(newList)
        currentDefLevel++
      }
      if (repetitionPath[currentDepth] === 'REPEATED') currentRepLevel++
    }

    // Add value or null based on definition level
    if (def === maxDefinitionLevel) {
      // assert(currentDepth === maxDefinitionLevel || currentDepth === repetitionPath.length - 2)
      currentContainer.push(values[valueIndex++])
    } else if (currentDepth === repetitionPath.length - 2) {
      // an undefined repeated leaf is an empty list, not a null value
      currentContainer.push(repetitionPath[currentDepth + 1] === 'REPEATED' ? [] : null)
    } else {
      currentContainer.push([])
    }
  }

  // Handle edge cases for empty inputs or single-level data
  if (!output.length) {
    // return max definition level of nested lists
    for (let i = 0; i < maxDefinitionLevel; i++) {
      /** @type {any[]} */
      const newList = []
      currentContainer.push(newList)
      currentContainer = newList
    }
  }

  return output
}

/**
 * Assemble a nested column from subcolumn data, one value per row.
 *
 * @param {Map<string, DecodedArray>} subcolumnData
 * @param {SchemaTree} schema top-level schema element
 * @param {ParquetParsers} parsers
 */
export function assembleNested(subcolumnData, schema, parsers) {
  subcolumnData.set(schema.path.join('.'), assembleColumn(subcolumnData, schema, parsers, 0))
}

/**
 * Assemble a column with no repeated ancestors, one value per row.
 * Rows where an ancestor is null hold a null marker for the parent to resolve.
 *
 * @param {Map<string, DecodedArray>} subcolumnData
 * @param {SchemaTree} schema
 * @param {ParquetParsers} parsers
 * @param {number} parentDefinitionLevel
 * @returns {DecodedArray}
 */
function assembleColumn(subcolumnData, schema, parsers, parentDefinitionLevel) {
  const path = schema.path.join('.')
  if (isListLike(schema) || isMapLike(schema) || schema.element.repetition_type === 'REPEATED') {
    // repeated columns have a wrapper array for each optional ancestor
    assembleWrapped(subcolumnData, schema, parsers, parentDefinitionLevel)
    return unwrapAncestors(takeColumn(subcolumnData, path), parentDefinitionLevel)
  }
  if (!schema.children.length) return takeColumn(subcolumnData, path)

  // Struct-like column
  const definitionLevel = schema.element.repetition_type === 'REQUIRED' ? parentDefinitionLevel : parentDefinitionLevel + 1
  const keys = schema.children.map(child => child.element.name)
  const columns = schema.children.map(child => assembleColumn(subcolumnData, child, parsers, definitionLevel))
  const { length } = columns[0]
  for (const column of columns) {
    if (column.length !== length) throw new Error('parquet struct parsing error')
  }
  let out = new Array(length)
  for (let i = 0; i < length; i++) {
    const first = columns[0][i]
    if (first !== undefined && first === nullMarkers[first?.def]) {
      // this struct is null, or one of its ancestors is
      out[i] = first.def < parentDefinitionLevel ? first : null
    } else {
      /** @type {Record<string, any>} */
      const obj = {}
      for (let k = 0; k < keys.length; k++) obj[keys[k]] = columns[k][i]
      out[i] = obj
    }
  }
  if (schema.element.logical_type?.type === 'VARIANT') {
    out = decodeVariantColumn(out, parsers)
  }
  return out
}

/**
 * Assemble a nested structure where each optional level is a wrapper array.
 *
 * @param {Map<string, DecodedArray>} subcolumnData
 * @param {SchemaTree} schema
 * @param {ParquetParsers} parsers
 * @param {number} depth depth of nested structure
 */
function assembleWrapped(subcolumnData, schema, parsers, depth) {
  const path = schema.path.join('.')
  const optional = schema.element.repetition_type === 'OPTIONAL'
  const nextDepth = optional ? depth + 1 : depth

  if (isListLike(schema)) {
    let sublist = schema.children[0]
    let subDepth = nextDepth
    if (sublist.children.length === 1) {
      sublist = sublist.children[0]
      subDepth++
    }
    assembleWrapped(subcolumnData, sublist, parsers, subDepth)

    const subcolumn = sublist.path.join('.')
    const values = subcolumnData.get(subcolumn)
    if (!values) throw new Error('parquet list column missing values')
    if (optional) flattenAtDepth(values, depth)
    subcolumnData.set(path, values)
    subcolumnData.delete(subcolumn)
    return
  }

  if (isMapLike(schema)) {
    const mapName = schema.children[0].element.name

    // Assemble keys and values
    assembleWrapped(subcolumnData, schema.children[0].children[0], parsers, nextDepth + 1)
    assembleWrapped(subcolumnData, schema.children[0].children[1], parsers, nextDepth + 1)

    const keys = subcolumnData.get(`${path}.${mapName}.key`)
    const values = subcolumnData.get(`${path}.${mapName}.value`)

    if (!keys) throw new Error('parquet map column missing keys')
    if (!values) throw new Error('parquet map column missing values')
    if (keys.length !== values.length) {
      throw new Error('parquet map column key/value length mismatch')
    }

    const out = assembleMaps(keys, values, nextDepth)
    if (optional) flattenAtDepth(out, depth)

    subcolumnData.delete(`${path}.${mapName}.key`)
    subcolumnData.delete(`${path}.${mapName}.value`)
    subcolumnData.set(path, out)
    return
  }

  // Struct-like column
  if (schema.children.length) {
    // construct a meta struct and then invert
    const invertDepth = schema.element.repetition_type === 'REQUIRED' ? depth : depth + 1
    /** @type {Record<string, any>} */
    const struct = {}
    for (const child of schema.children) {
      assembleWrapped(subcolumnData, child, parsers, invertDepth)
      const childData = subcolumnData.get(child.path.join('.'))
      if (!childData) throw new Error('parquet struct missing child data')
      struct[child.element.name] = childData
    }
    // remove children
    for (const child of schema.children) {
      subcolumnData.delete(child.path.join('.'))
    }

    // invert struct by depth
    let inverted = invertStruct(struct, invertDepth)
    if (schema.element.logical_type?.type === 'VARIANT') {
      inverted = decodeVariantColumn(inverted, parsers)
    }
    if (optional) flattenAtDepth(inverted, depth)
    subcolumnData.set(path, inverted)
  }
}

/**
 * Replace the wrapper array of each optional ancestor with its contents,
 * or with a null marker if the ancestor is null.
 *
 * @param {DecodedArray} rows
 * @param {number} depth number of wrapper arrays
 * @returns {DecodedArray}
 */
function unwrapAncestors(rows, depth) {
  if (!depth) return rows
  for (let i = 0; i < rows.length; i++) {
    let value = rows[i]
    for (let def = 0; def < depth; def++) {
      if (!value.length) {
        value = nullAt(def)
        break
      }
      value = value[0]
    }
    rows[i] = value
  }
  return rows
}

/**
 * @param {Map<string, DecodedArray>} subcolumnData
 * @param {string} path
 * @returns {DecodedArray}
 */
function takeColumn(subcolumnData, path) {
  const data = subcolumnData.get(path)
  if (!data) throw new Error('parquet struct missing child data')
  subcolumnData.delete(path)
  return data
}

/**
 * Shared markers for rows where an ancestor struct is null, indexed by the
 * row's definition level. Compared by identity.
 *
 * @type {{ def: number }[]}
 */
const nullMarkers = []

/**
 * @param {number} def
 * @returns {{ def: number }}
 */
function nullAt(def) {
  return nullMarkers[def] ??= { def }
}

/**
 * @import {DecodedArray, ParquetParsers, SchemaTree} from '../src/types.js'
 * @param {DecodedArray} arr
 * @param {number} depth
 */
function flattenAtDepth(arr, depth) {
  for (let i = 0; i < arr.length; i++) {
    if (depth) {
      flattenAtDepth(arr[i], depth - 1)
    } else {
      arr[i] = arr[i].length ? arr[i][0] : null
    }
  }
}

/**
 * @param {DecodedArray} keys
 * @param {DecodedArray} values
 * @param {number} depth
 * @returns {any[]}
 */
function assembleMaps(keys, values, depth) {
  const out = []
  for (let i = 0; i < keys.length; i++) {
    if (depth) {
      out.push(assembleMaps(keys[i], values[i], depth - 1)) // go deeper
    } else {
      if (keys[i]) {
        /** @type {Record<string, any>} */
        const obj = {}
        for (let j = 0; j < keys[i].length; j++) {
          const value = values[i][j]
          obj[keys[i][j]] = value === undefined ? null : value
        }
        out.push(obj)
      } else {
        out.push(undefined)
      }
    }
  }
  return out
}

/**
 * Invert a struct-like object by depth.
 *
 * @param {Record<string, any[]>} struct
 * @param {number} depth
 * @returns {any[]}
 */
function invertStruct(struct, depth) {
  const keys = Object.keys(struct)
  return invertColumns(keys, keys.map(key => struct[key]), depth)
}

/**
 * @param {string[]} keys
 * @param {any[][]} columns child arrays, parallel to keys
 * @param {number} depth
 * @returns {any[]}
 */
function invertColumns(keys, columns, depth) {
  const length = columns[0]?.length
  if (!length) return []
  for (const column of columns) {
    if (column.length !== length) throw new Error('parquet struct parsing error')
  }
  const out = new Array(length)
  for (let i = 0; i < length; i++) {
    if (depth) {
      const row = new Array(columns.length)
      for (let k = 0; k < columns.length; k++) row[k] = columns[k][i]
      out[i] = invertColumns(keys, row, depth - 1) // deeper
    } else {
      /** @type {Record<string, any>} */
      const obj = {}
      for (let k = 0; k < keys.length; k++) obj[keys[k]] = columns[k][i]
      out[i] = obj
    }
  }
  return out
}
