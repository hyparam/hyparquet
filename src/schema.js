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
 * Get schema elements from the root to the given element name.
 *
 * @param {SchemaElement[]} schema
 * @param {string[]} name path to the element
 * @returns {SchemaTree[]} list of schema elements
 */
export function getSchemaPath(schema, name) {
  let tree = schemaTree(schema, 0)
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
 * Get the column name as foo.bar and handle list and map like columns.
 *
 * @param {string[]} path
 * @returns {string} column name
 */
export function getColumnName(path) {
  return path.join('.')
    .replace(/(\.list\.element)+/g, '')
    .replace(/\.key_value\.key/g, '')
    .replace(/\.key_value\.value/g, '')
}

/**
 * Check if a column is list-like.
 *
 * @param {SchemaTree[]} schemaPath
 * @returns {boolean} true if map-like
 */
export function isListLike(schemaPath) {
  const schema = schemaPath.at(-3)
  if (!schema) return false
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
 * @param {SchemaTree[]} schemaPath
 * @returns {boolean} true if map-like
 */
export function isMapLike(schemaPath) {
  const schema = schemaPath.at(-3)
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
