import { isListLike, isMapLike } from './schema.js'

/**
 * Dremel-assembly of arrays of values into lists
 *
 * Reconstructs a complex nested structure from flat arrays of definition and repetition levels,
 * according to Dremel encoding.
 *
 * @typedef {import('./types.d.ts').DecodedArray} DecodedArray
 * @param {number[] | undefined} definitionLevels
 * @param {number[]} repetitionLevels
 * @param {DecodedArray} values
 * @param {boolean} isNullable can entries be null?
 * @param {number} maxDefinitionLevel definition level that corresponds to non-null
 * @param {number} maxRepetitionLevel repetition level that corresponds to a new row
 * @returns {DecodedArray} array of values
 */
export function assembleLists(
  definitionLevels, repetitionLevels, values, isNullable, maxDefinitionLevel, maxRepetitionLevel
) {
  const n = definitionLevels?.length || repetitionLevels.length
  let valueIndex = 0
  /** @type {any[]} */
  const output = []

  // Track state of nested structures
  const containerStack = [output]
  let currentContainer = output

  for (let i = 0; i < n; i++) {
    const def = definitionLevels?.length ? definitionLevels[i] : maxDefinitionLevel
    const rep = repetitionLevels[i]

    if (rep !== maxRepetitionLevel) {
      // Move back to the parent container
      while (rep < containerStack.length - 1) {
        containerStack.pop()
      }
      // Construct new lists up to max repetition level
      // @ts-expect-error won't be empty
      currentContainer = containerStack.at(-1)
    }

    // Add lists up to definition level
    const targetDepth = isNullable ? (def + 1) / 2 : maxRepetitionLevel + 1
    for (let j = containerStack.length; j < targetDepth; j++) {
      /** @type {any[]} */
      const newList = []
      currentContainer.push(newList)
      currentContainer = newList
      containerStack.push(newList)
    }

    // Add value or null based on definition level
    if (def === maxDefinitionLevel) {
      currentContainer.push(values[valueIndex++])
    } else if (isNullable) {
      // TODO: actually depends on level required or not
      if (def % 2 === 0) {
        currentContainer.push(undefined)
      } else {
        currentContainer.push([])
      }
    }
  }

  // Handle edge cases for empty inputs or single-level data
  if (output.length === 0) {
    if (values.length > 0 && maxRepetitionLevel === 0) {
      // All values belong to the same (root) list
      return [values]
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

// TODO: depends on prior def level

/**
 * Assemble a nested structure from subcolumn data.
 * https://github.com/apache/parquet-format/blob/apache-parquet-format-2.10.0/LogicalTypes.md
 *
 * @typedef {import('./types.d.ts').SchemaTree} SchemaTree
 * @param {Map<string, any[]>} subcolumnData
 * @param {SchemaTree} schema top-level schema element
 * @param {number} depth
 */
export function assembleNested(subcolumnData, schema, depth = 0) {
  if (schema.path.length - 1 !== depth) throw new Error(`WTF parquet struct-like column depth mismatch ${schema.path.length - 1} !== ${depth}`)
  const path = schema.path.join('.')

  if (isListLike(schema)) {
    const sublist = schema.children[0].children[0]
    assembleNested(subcolumnData, sublist, depth + 2)

    const subcolumn = sublist.path.join('.')
    const values = subcolumnData.get(subcolumn)
    if (!values) throw new Error('parquet list-like column missing values')
    subcolumnData.set(path, values)
    subcolumnData.delete(subcolumn)
    return
  }

  if (isMapLike(schema)) {
    const mapName = schema.children[0].element.name

    // Assemble keys and values
    assembleNested(subcolumnData, schema.children[0].children[0], depth + 2)
    assembleNested(subcolumnData, schema.children[0].children[1], depth + 2)

    const keys = subcolumnData.get(`${path}.${mapName}.key`)
    const values = subcolumnData.get(`${path}.${mapName}.value`)

    if (!keys) throw new Error('parquet map-like column missing keys')
    if (!values) throw new Error('parquet map-like column missing values')

    if (keys.length !== values.length) {
      throw new Error('parquet map-like column key/value length mismatch')
    }

    const out = assembleMaps(keys, values)

    subcolumnData.delete(`${path}.${mapName}.key`)
    subcolumnData.delete(`${path}.${mapName}.value`)
    subcolumnData.set(path, out)
    return
  }
}

/**
 * @param {any[]} keys
 * @param {any[]} values
 * @returns {any[]}
 */
function assembleMaps(keys, values) {
  const out = []
  for (let i = 0; i < keys.length; i++) {
    // keys will be empty for {} and undefined for null
    if (keys[i]) {
      /** @type {Record<string, any>} */
      const obj = {}
      for (let j = 0; j < keys[i].length; j++) {
        if (Array.isArray(keys[i][j])) {
          // TODO: key should not be an array, this is an assemble bug?
          keys[i][j] = keys[i][j][0]
          values[i][j] = values[i][j][0]
        }
        if (!keys[i][j]) continue
        obj[keys[i][j]] = values[i][j] === undefined ? null : values[i][j]
      }
      out.push(obj)
    } else {
      out.push(undefined)
    }
  }
  return out
}
