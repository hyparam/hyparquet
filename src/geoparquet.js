/**
 * @import {KeyValue, LogicalType} from '../src/types.d.ts'
 * @param {KeyValue[] | undefined} key_value_metadata
 * @returns {{ name: string; logical_type: LogicalType; }[]}
 */
export function getGeoParquetColumns(key_value_metadata) {
  /** @type {{ name: string; logical_type: LogicalType }[]} */
  const columns = []

  const geo = key_value_metadata?.find(({ key }) => key === 'geo')?.value
  const decodedColumns = (geo && JSON.parse(geo)?.columns) ?? {}
  for (const [name, column] of Object.entries(decodedColumns)) {
    if (column.encoding !== 'WKB') {
      continue
    }
    const type = column.edges === 'spherical' ? 'GEOGRAPHY' : 'GEOMETRY'
    const crs = convertCrs(column.crs)
    // Note: we can't infer GEOGRAPHY's algorithm from GeoParquet
    columns.push({ name, logical_type: { type, crs } })
  }
  return columns
}

/**
 * Convert crs property from GeoParquet metadata to crs field in Parquet LogicalType.
 *
 * @param {any} crs
 * @returns {string | undefined}
 */
function convertCrs(crs) {
  const id = crs?.id ?? crs?.ids?.[0]
  if (id) {
    return `${id.authority}:${id.code.toString()}`
  }
}
