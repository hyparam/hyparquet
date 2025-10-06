/**
 * @typedef {import('../src/types.d.ts').SchemaElement} SchemaElement
 * @typedef {import('../src/types.d.ts').LogicalType} LogicalType
 */

/**
 *
 * @param {{ key: string; value: string }[] | undefined} key_value_metadata
 * @returns {{ name: string; logical_type: LogicalType }[] | undefined}
 */
export function getGeoParquetColumns(key_value_metadata) {
  const geo = key_value_metadata?.find(({ key }) => key === 'geo')?.value
  if (!geo) {
    return
  }
  /** @type {{ name: string; logical_type: LogicalType }[]} */
  const columns = []
  for (const [name, column] of Object.entries(JSON.parse(geo)?.columns)) {
    const { encoding, edges } = column
    if (encoding !== 'WKB') {
      continue
    }
    const type = edges === 'spherical' ? 'GEOGRAPHY' : 'GEOMETRY'
    /** @type {LogicalType} */
    const logical_type = { type }
    /* TODO(SL): extract 'crs' from column.crs */
    /* Note: I don't think it's possible to get 'algorithm' from GeoParquet */
    columns.push({ name, logical_type })
  }
  return columns
}
