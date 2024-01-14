import { FieldRepetitionType } from './constants.js'

/**
 * @typedef {import('./types.js').SchemaElement} SchemaElement
 * @typedef {{ element: SchemaElement, children: SchemaTree[], endIndex: number }} SchemaTree
 */

/**
 * Build a tree from the schema elements.
 *
 * @param {SchemaElement[]} schema
 * @param {number} i index of the root element
 * @returns {SchemaTree} tree of schema elements
 */
function schemaTree(schema, i) {
  const root = schema[i]
  const children = []
  i++

  // Read the specified number of children
  if (root.num_children) {
    while (children.length < root.num_children) {
      const child = schemaTree(schema, i)
      i = child.endIndex
      children.push(child)
    }
  }

  return { endIndex: i, element: root, children }
}

/**
 * Get the schema element with the given name.
 *
 * @param {SchemaElement[]} schema
 * @param {string[]} name path to the element
 * @returns {SchemaElement} schema element
 */
export function schemaElement(schema, name) {
  let tree = schemaTree(schema, 0)
  // traverse the tree to find the element
  for (const part of name) {
    const child = tree.children.find(child => child.element.name === part)
    if (!child) {
      throw new Error(`parquet schema element not found: ${name}`)
    }
    tree = child
  }
  return tree.element
}

/**
 * Check if the schema element with the given name is required.
 *
 * @param {SchemaElement[]} schema
 * @param {string[]} name path to the element
 * @returns {boolean} true if the element is required
 */
export function isRequired(schema, name) {
  return schemaElement(schema, name).repetition_type === FieldRepetitionType.REQUIRED
}

/**
 * Get the max repetition level for a given schema path.
 *
 * @param {SchemaElement[]} schema
 * @param {string[]} parts path to the element
 * @returns {number} max repetition level
 */
export function getMaxRepetitionLevel(schema, parts) {
  let maxLevel = 0
  parts.forEach((part, i) => {
    const element = schemaElement(schema, parts.slice(0, i + 1))
    if (element.repetition_type === FieldRepetitionType.REPEATED) {
      maxLevel += 1
    }
  })
  return maxLevel
}

/**
 * Get the max definition level for a given schema path.
 *
 * @param {SchemaElement[]} schema
 * @param {string[]} parts path to the element
 * @returns {number} max definition level
 */
export function getMaxDefinitionLevel(schema, parts) {
  let maxLevel = 0
  parts.forEach((part, i) => {
    const element = schemaElement(schema, parts.slice(0, i + 1))
    if (element.repetition_type !== FieldRepetitionType.REQUIRED) {
      maxLevel += 1
    }
  })
  return maxLevel
}

/**
 * Get the number of bytes to skip for definition levels.
 *
 * @param {number} num number of values
 * @returns {number} number of bytes to skip
 */
export function skipDefinitionBytes(num) {
  let byteLength = 6
  let n = num >>> 8
  while (n !== 0) {
    byteLength += 1
    n >>>= 7
  }
  return byteLength
}
