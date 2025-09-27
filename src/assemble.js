import { DEFAULT_PARSERS } from './convert.js'
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
  const n = definitionLevels?.length || repetitionLevels.length
  if (!n) return values
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  const repetitionPath = schemaPath.map(({ element }) => element.repetition_type)
  let valueIndex = 0

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
      currentContainer.push(null)
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
 * Assemble a nested structure from subcolumn data.
 * https://github.com/apache/parquet-format/blob/apache-parquet-format-2.10.0/LogicalTypes.md#nested-types
 *
 * @param {Map<string, DecodedArray>} subcolumnData
 * @param {SchemaTree} schema top-level schema element
 * @param {number} [depth] depth of nested structure
 * @param {import('./types.d.ts').ParquetParsers} [parsers]
 */
export function assembleNested(subcolumnData, schema, depth = 0, parsers = DEFAULT_PARSERS) {
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
    assembleNested(subcolumnData, sublist, subDepth, parsers)

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
    assembleNested(subcolumnData, schema.children[0].children[0], nextDepth + 1, parsers)
    assembleNested(subcolumnData, schema.children[0].children[1], nextDepth + 1, parsers)

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
      assembleNested(subcolumnData, child, invertDepth, parsers)
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
 * @import {DecodedArray, SchemaTree} from '../src/types.d.ts'
 * @param {DecodedArray} arr
 * @param {number} depth
 */
function flattenAtDepth(arr, depth) {
  for (let i = 0; i < arr.length; i++) {
    if (depth) {
      flattenAtDepth(arr[i], depth - 1)
    } else {
      arr[i] = arr[i][0]
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
  const length = struct[keys[0]]?.length
  const out = []
  for (let i = 0; i < length; i++) {
    /** @type {Record<string, any>} */
    const obj = {}
    for (const key of keys) {
      if (struct[key].length !== length) throw new Error('parquet struct parsing error')
      obj[key] = struct[key][i]
    }
    if (depth) {
      out.push(invertStruct(obj, depth - 1)) // deeper
    } else {
      out.push(obj)
    }
  }
  return out
}
