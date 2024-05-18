import { isListLike, isMapLike } from './schema.js'

/**
 * Dremel-assembly of arrays of values into lists
 *
 * Reconstructs a complex nested structure from flat arrays of definition and repetition levels,
 * according to Dremel encoding.
 *
 * @typedef {import('./types.d.ts').DecodedArray} DecodedArray
 * @typedef {import('./types.d.ts').FieldRepetitionType} FieldRepetitionType
 * @param {number[] | undefined} definitionLevels
 * @param {number[]} repetitionLevels
 * @param {DecodedArray} values
 * @param {(FieldRepetitionType | undefined)[]} repetitionPath
 * @param {number} maxDefinitionLevel definition level that corresponds to non-null
 * @param {number} maxRepetitionLevel repetition level that corresponds to a new row
 * @returns {DecodedArray} array of values
 */
export function assembleLists(
  definitionLevels, repetitionLevels, values, repetitionPath, maxDefinitionLevel, maxRepetitionLevel
) {
  const n = definitionLevels?.length || repetitionLevels.length
  let valueIndex = 0
  /** @type {any[]} */
  const output = []

  // Track state of nested structures
  const containerStack = [output]
  let currentContainer = output
  let currentDepth = 0 // schema depth
  let currentDefLevel = 0 // list depth
  let currentRepLevel = 0

  for (let i = 0; i < n; i++) {
    // assert(currentDefLevel === containerStack.length - 1)
    const def = definitionLevels?.length ? definitionLevels[i] : maxDefinitionLevel
    const rep = repetitionLevels[i]

    // Pop up to start of rep level
    while (currentDepth && (rep < currentRepLevel || repetitionPath[currentDepth] === 'OPTIONAL')) {
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
    while (currentDepth < repetitionPath.length - 2 && currentDefLevel < def) {
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
  if (output.length === 0) {
    if (values.length > 0 && maxRepetitionLevel === 0) {
      return values // flat list
    }
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
 * @typedef {import('./types.d.ts').SchemaTree} SchemaTree
 * @param {Map<string, any[]>} subcolumnData
 * @param {SchemaTree} schema top-level schema element
 * @param {number} [depth] depth of nested structure
 */
export function assembleNested(subcolumnData, schema, depth = 0) {
  const path = schema.path.join('.')
  const optional = schema.element.repetition_type === 'OPTIONAL'
  const nextDepth = optional ? depth + 1 : depth

  if (isListLike(schema)) {
    const sublist = schema.children[0].children[0]
    assembleNested(subcolumnData, sublist, nextDepth + 1)

    const subcolumn = sublist.path.join('.')
    const values = subcolumnData.get(subcolumn)
    if (!values) throw new Error('parquet list-like column missing values')
    if (optional) flattenAtDepth(values, depth)
    subcolumnData.set(path, values)
    subcolumnData.delete(subcolumn)
    return
  }

  if (isMapLike(schema)) {
    const mapName = schema.children[0].element.name

    // Assemble keys and values
    assembleNested(subcolumnData, schema.children[0].children[0], nextDepth + 1)
    assembleNested(subcolumnData, schema.children[0].children[1], nextDepth + 1)

    const keys = subcolumnData.get(`${path}.${mapName}.key`)
    const values = subcolumnData.get(`${path}.${mapName}.value`)

    if (!keys) throw new Error('parquet map-like column missing keys')
    if (!values) throw new Error('parquet map-like column missing values')
    if (keys.length !== values.length) {
      throw new Error('parquet map-like column key/value length mismatch')
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
    /** @type {Record<string, any>} */
    const struct = {}
    for (const child of schema.children) {
      assembleNested(subcolumnData, child, nextDepth)
      const childData = subcolumnData.get(child.path.join('.'))
      if (!childData) throw new Error('parquet struct-like column missing child data')
      if (child.element.repetition_type === 'OPTIONAL') {
        flattenAtDepth(childData, depth)
      }
      struct[child.element.name] = childData
    }
    // remove children
    for (const child of schema.children) {
      subcolumnData.delete(child.path.join('.'))
    }
    // invert struct by depth
    subcolumnData.set(path, invertStruct(struct, depth))
    return
  }
  // assert(schema.element.repetition_type !== 'REPEATED')
}

/**
 * @param {any[]} arr
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
 * @param {any[]} keys
 * @param {any[]} values
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
