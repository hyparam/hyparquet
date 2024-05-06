/**
 * Build a tree from the schema elements.
 *
 * @typedef {import('./types.js').SchemaElement} SchemaElement
 * @typedef {import('./types.js').SchemaTree} SchemaTree
 * @param {SchemaElement[]} schema
 * @param {number} rootIndex index of the root element
 * @param {string[]} path path to the element
 * @returns {SchemaTree} tree of schema elements
 */
function schemaTree(schema, rootIndex, path) {
  const element = schema[rootIndex]
  const children = []
  let count = 1

  // Read the specified number of children
  if (element.num_children) {
    while (children.length < element.num_children) {
      const childElement = schema[rootIndex + count]
      const child = schemaTree(schema, rootIndex + count, [...path, childElement.name])
      count += child.count
      children.push(child)
    }
  }

  return { count, element, children, path }
}

/**
 * Get schema elements from the root to the given element name.
 *
 * @param {SchemaElement[]} schema
 * @param {string[]} name path to the element
 * @returns {SchemaTree[]} list of schema elements
 */
export function getSchemaPath(schema, name) {
  let tree = schemaTree(schema, 0, [])
  const path = [tree]
  for (const part of name) {
    const child = tree.children.find(child => child.element.name === part)
    if (!child) throw new Error(`parquet schema element not found: ${name}`)
    path.push(child)
    tree = child
  }
  return path
}

/**
 * Check if the schema path and all its ancestors are required.
 *
 * @param {SchemaTree[]} schemaPath
 * @returns {boolean} true if the element is required
 */
export function isRequired(schemaPath) {
  for (const { element } of schemaPath.slice(1)) {
    if (element.repetition_type !== 'REQUIRED') {
      return false
    }
  }
  return true
}

/**
 * Get the max repetition level for a given schema path.
 *
 * @param {SchemaTree[]} schemaPath
 * @returns {number} max repetition level
 */
export function getMaxRepetitionLevel(schemaPath) {
  let maxLevel = 0
  for (const { element } of schemaPath) {
    if (element.repetition_type === 'REPEATED') {
      maxLevel++
    }
  }
  return maxLevel
}

/**
 * Get the max definition level for a given schema path.
 *
 * @param {SchemaTree[]} schemaPath
 * @returns {number} max definition level
 */
export function getMaxDefinitionLevel(schemaPath) {
  let maxLevel = 0
  for (const { element } of schemaPath.slice(1)) {
    if (element.repetition_type !== 'REQUIRED') {
      maxLevel++
    }
  }
  return maxLevel
}

/**
 * Check if a column is list-like.
 *
 * @param {SchemaTree} schema
 * @returns {boolean} true if list-like
 */
export function isListLike(schema) {
  if (!schema) return false
  if (schema.element.converted_type !== 'LIST') return false
  if (schema.children.length > 1) return false

  const firstChild = schema.children[0]
  if (firstChild.children.length > 1) return false
  if (firstChild.element.repetition_type !== 'REPEATED') return false

  return true
}

/**
 * Check if a column is map-like.
 *
 * @param {SchemaTree} schema
 * @returns {boolean} true if map-like
 */
export function isMapLike(schema) {
  if (!schema) return false
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
