/**
 * @import {KeyValue, LogicalType, SchemaElement} from '../src/types.d.ts'
 * @param {SchemaElement[]} schema
 * @param {KeyValue[] | undefined} key_value_metadata
 * @returns {void}
 */
export function markGeoColumns(schema, key_value_metadata) {
  // Prepare the list of GeoParquet columns
  /** @type {Map<string, LogicalType>} */
  const columns = new Map()
  const geo = key_value_metadata?.find(({ key }) => key === 'geo')?.value
  const decodedColumns = (geo && JSON.parse(geo)?.columns) ?? {}
  for (const [name, column] of Object.entries(decodedColumns)) {
    if (column.encoding !== 'WKB') {
      continue
    }
    const type = column.edges === 'spherical' ? 'GEOGRAPHY' : 'GEOMETRY'
    const id = column.crs?.id ?? column.crs?.ids?.[0]
    const crs = id ? `${id.authority}:${id.code.toString()}` : undefined
    // Note: we can't infer GEOGRAPHY's algorithm from GeoParquet
    columns.set(name, { type, crs })
  }

  // Mark schema elements with logical type
  // Only look at root-level columns of type BYTE_ARRAY without existing logical_type
  let i = 1 // skip root
  while (i < schema.length) {
    const element = schema[i]
    const { logical_type, name, num_children, repetition_type, type } = element
    i++
    if (num_children) {
      i += num_children
      continue // skip the element and its children
    }
    if (type === 'BYTE_ARRAY' && logical_type === undefined && repetition_type !== 'REPEATED') {
      element.logical_type = columns.get(name)
    }
  }
}
