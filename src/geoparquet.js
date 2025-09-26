/** @import {GeometryType, GeoParquet, GeoParquetColumn, KeyValue, SchemaElement} from '../src/types.d.ts' */

export const unsupportedEncodings = [
  'point',
  'linestring',
  'polygon',
  'multipoint',
  'multilinestring',
  'multipolygon',
]

/** @type GeometryType[] */
export const supportedGeometryTypes = [
  'Point',
  'LineString',
  'Polygon',
  'MultiPoint',
  'MultiLineString',
  'MultiPolygon',
]
export const unsupportedGeometryTypes = [
  'GeometryCollection',
  'GeometryCollection Z',
  'Point Z',
  'LineString Z',
  'Polygon Z',
  'MultiPoint Z',
  'MultiLineString Z',
  'MultiPolygon Z',
]

/**
 * Converts 'geo' field to GeoParquet metadata, or undefined in case of error
 *
 * @param {KeyValue[]} key_value_metadata
 * @param {SchemaElement[]} schema
 * @returns {GeoParquet | undefined}
 */
export function convertGeoParquet(key_value_metadata, schema) {
  try {
    return parseGeoParquet(key_value_metadata, schema)
  } catch (_) {
    return undefined
  }
}

/**
 * Parse the 'geo' field to GeoParquet metadata
 *
 * @param {KeyValue[]} key_value_metadata
 * @param {SchemaElement[]} schema
 * @returns {GeoParquet}
 */
export function parseGeoParquet(key_value_metadata, schema) {
  // Only columns with BYTE_ARRAY type, encoded with WKB, are supported
  const columnNames = schema.filter(e => e.type === 'BYTE_ARRAY').map(e => e.name)

  const geoString = key_value_metadata?.find(kv => kv.key === 'geo')?.value
  if (geoString === undefined) {
    throw new Error('No geo field')
  }

  /** @type {unknown} */
  let geo
  try {
    geo = JSON.parse(geoString)
  } catch (_) {
    throw new Error('Invalid GeoParquet metadata: geo field is not valid JSON')
  }

  if (
    !geo ||
    typeof geo !== 'object' ||
    Array.isArray(geo)
  ) {
    throw new Error('Invalid GeoParquet metadata: not an object')
  }

  /* Version */
  if (
    !('version' in geo) ||
    typeof geo.version !== 'string'
  ) {
    throw new Error('Invalid GeoParquet metadata: missing or invalid version')
  }
  if (geo.version !== '1.0.0' && geo.version !== '1.1.0' && geo.version !== '1.2.0-dev') {
    throw new Error(`Unsupported GeoParquet version: ${geo.version}`)
  }
  const { version } = geo

  /* Columns */
  if (
    !('columns' in geo) ||
    !geo.columns ||
    typeof geo.columns !== 'object' ||
    Array.isArray(geo.columns)
  ) {
    throw new Error('Invalid GeoParquet metadata: missing or invalid columns')
  }

  /** @type GeoParquet['columns'] */
  const columns = {}
  for (const [columnName, metadata] of Object.entries(geo.columns)) {
    columns[columnName] = parseColumn({ columnName, metadata, columnNames, version })
  }

  /* Primary column */
  if (
    !('primary_column' in geo) ||
    typeof geo.primary_column !== 'string'
  ) {
    throw new Error('Invalid GeoParquet metadata: missing or invalid primary_column')
  }
  if (!(geo.primary_column in columns)) {
    throw new Error(`Invalid GeoParquet metadata: primary_column "${geo.primary_column}" does not exist in columns`)
  }
  const { primary_column } = geo

  return {
    version,
    primary_column,
    columns,
  }
}

/**
 * Parse GeoParquet column metadata
 * @param {object} options
 * @param {string} options.columnName
 * @param {unknown} options.metadata
 * @param {string[]} options.columnNames
 * @param {GeoParquet['version']} options.version
 * @returns {GeoParquetColumn}
 */
export function parseColumn({ columnName, metadata, columnNames, version }) {
  if (!columnNames.includes(columnName)) {
    throw new Error(`Invalid GeoParquet metadata: no BYTE_ARRAY column called "${columnName}" in file`)
  }

  if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) {
    throw new Error(`Invalid GeoParquet metadata: column "${columnName}" is not an object`)
  }

  /* Encoding */
  if (!('encoding' in metadata)) {
    throw new Error(`Invalid GeoParquet metadata: column "${columnName}" missing encoding`)
  }
  if (
    typeof metadata.encoding !== 'string' ||
    !['WKB', ...unsupportedEncodings].includes(metadata.encoding)
  ) {
    throw new Error(`Invalid GeoParquet metadata: column "${columnName}" invalid encoding "${metadata.encoding}"`)
  }
  if (metadata.encoding !== 'WKB') {
    if (version === '1.0.0') {
      throw new Error(`Invalid GeoParquet metadata: column "${columnName}" has invalid encoding "${metadata.encoding}" for version 1.0.0 (must be "WKB")`)
    } else {
      throw new Error(`Unsupported GeoParquet metadata: column "${columnName}" has encoding "${metadata.encoding}", but only "WKB" is supported`)
    }
  }
  const { encoding } = metadata

  /* Geometry types */

  /** @type GeometryType[] */
  const geometry_types = []
  if (
    !('geometry_types' in metadata) ||
    !Array.isArray(metadata.geometry_types)
  ) {
    throw new Error(`Invalid GeoParquet metadata: column "${columnName}" missing or invalid geometry_types`)
  }
  for (const geometry_type of metadata.geometry_types) {
    if (unsupportedGeometryTypes.includes(geometry_type)) {
      throw new Error(`Invalid GeoParquet metadata: column "${columnName}" geometry_type "${geometry_type}" is not supported`)
    }
    if (!supportedGeometryTypes.includes(geometry_type)) {
      throw new Error(`Invalid GeoParquet metadata: column "${columnName}" has invalid geometry_type "${geometry_type}"`)
    }
    geometry_types.push(geometry_type)
  }
  const duplicates = geometry_types.filter((e, i, a) => a.indexOf(e) !== i)
  if (duplicates.length > 0) {
    throw new Error(`Invalid GeoParquet metadata: column "${columnName}" has duplicate geometry_types: ${duplicates.join(', ')}`)
  }

  return {
    encoding,
    geometry_types,
  }
}

