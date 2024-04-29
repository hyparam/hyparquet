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
function schemaTree(schema, rootIndex) {
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
 * @returns {SchemaTree} schema element
 */
export function schemaElement(schema, name) {
  let tree = schemaTree(schema, 0)
  // traverse the tree to find the element
  for (const part of name) {
    const child = tree.children.find(child => child.element.name === part)
    if (!child) throw new Error(`parquet schema element not found: ${name}`)
    tree = child
  }
  return tree
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
    const { element } = schemaElement(schema, parts.slice(0, i + 1))
    if (element.repetition_type === 'REPEATED') {
      maxLevel++
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
    const { element } = schemaElement(schema, parts.slice(0, i + 1))
    if (element.repetition_type !== 'REQUIRED') {
      maxLevel++
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
    byteLength++
    n >>>= 7
  }
  return byteLength
}

/**
 * Get the column name as foo.bar and handle list-like columns.
 * @param {SchemaElement[]} schema
 * @param {string[]} path
 * @returns {string} column name
 */
export function getColumnName(schema, path) {
  if (isListLike(schema, path) || isMapLike(schema, path)) {
    return path.slice(0, -2).join('.')
  } else {
    return path.join('.')
  }
}

/**
 * Check if a column is list-like.
 *
 * @param {SchemaElement[]} schemaElements parquet schema elements
 * @param {string[]} path column path
 * @returns {boolean} true if map-like
 */
export function isListLike(schemaElements, path) {
  const schema = schemaElement(schemaElements, path.slice(0, -2))
  if (path.length < 3) return false
  if (schema.element.converted_type !== 'LIST') return false
  if (schema.children.length > 1) return false

  const firstChild = schema.children[0]
  if (firstChild.children.length > 1) return false
  if (firstChild.element.repetition_type !== 'REPEATED') return false

  const secondChild = firstChild.children[0]
  if (secondChild.element.repetition_type !== 'REQUIRED') return false

  return true
}

/**
 * Check if a column is map-like.
 *
 * @param {SchemaElement[]} schemaElements parquet schema elements
 * @param {string[]} path column path
 * @returns {boolean} true if map-like
 */
export function isMapLike(schemaElements, path) {
  const schema = schemaElement(schemaElements, path.slice(0, -2))
  if (path.length < 3) return false
  if (schema.element.converted_type !== 'MAP') return false
  if (schema.children.length > 1) return false

  const firstChild = schema.children[0]
  if (firstChild.children.length !== 2) return false
  if (firstChild.element.repetition_type !== 'REPEATED') return false

  const keyChild = firstChild.children.find(child => child.element.name === 'key')
  if (keyChild?.element.repetition_type !== 'REQUIRED') return false

  const valueChild = firstChild.children.find(child => child.element.name === 'value')
  if (valueChild?.element.repetition_type === 'REPEATED') return false

  return true
}
