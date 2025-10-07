/**
 * @import {KeyValue, LogicalType} from '../src/types.d.ts'
 * @param {KeyValue[] | undefined} key_value_metadata
 * @returns {{ name: string; logical_type: LogicalType }[]}
 */
export function getGeoParquetColumns(key_value_metadata) {
  /** @type {{ name: string; logical_type: LogicalType }[]} */
  const columns = []

  const geo = key_value_metadata?.find(({ key }) => key === 'geo')?.value
  const decodedColumns = (geo && JSON.parse(geo)?.columns) ?? {}
  for (const [name, { encoding, edges }] of Object.entries(decodedColumns)) {
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
