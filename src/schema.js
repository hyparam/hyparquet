/**
 * @typedef {import('./types.js').SchemaElement} SchemaElement
 * @typedef {import('./types.js').SchemaTree} SchemaTree
 */

/**
 * Build a tree from the schema elements.
 *
 * @param {SchemaElement[]} schema
 * @param {number} rootIndex index of the root element
 * @returns {SchemaTree} tree of schema elements
 */
export function schemaTree(schema, rootIndex) {
  const root = schema[rootIndex]
  const children = []
  let count = 1

  // Read the specified number of children
  if (root.num_children) {
    while (children.length < root.num_children) {
      const child = schemaTree(schema, rootIndex + count)
      count += child.count
      children.push(child)
    }
  }

  return { count, element: root, children }
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
    if (!child) throw new Error(`parquet schema element not found: ${name}`)
    tree = child
  }
  return tree.element
}

/**
 * Check if the schema element with the given name is required.
 * An element is required if all of its ancestors are required.
 *
 * @param {SchemaElement[]} schema
 * @param {string[]} name path to the element
 * @returns {boolean} true if the element is required
 */
export function isRequired(schema, name) {
  /** @type {SchemaTree | undefined} */
  let tree = schemaTree(schema, 0)
  for (let i = 0; i < name.length; i++) {
    // Find schema child with the given name
    tree = tree.children.find(child => child.element.name === name[i])
    if (!tree) throw new Error(`parquet schema element not found: ${name}`)
    if (tree.element.repetition_type !== 'REQUIRED') {
      return false
    }
  }
  return true
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
    if (element.repetition_type === 'REPEATED') {
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
    if (element.repetition_type !== 'REQUIRED') {
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
