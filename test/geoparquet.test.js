import { describe, expect, test } from 'vitest'
import { convertGeoParquet, parseColumn, parseGeoParquet, supportedGeometryTypes, unsupportedEncodings, unsupportedGeometryTypes } from '../src/geoparquet.js'

describe('geoparquet', () => {
  describe('convertGeoParquet', () => {
    /** @type Parameters<parseGeoParquet>[1] */
    const schema = [
      { name: 'a', type: 'BYTE_ARRAY' },
      { name: 'b', type: 'BYTE_ARRAY' },
    ]

    test.for([
      [],
      [{ key: 'geo', value: 'invalid' }],
      [{ key: 'geo', value: JSON.stringify({ version: 'unknown' }) }],
    ])('ignores missing or erroneous geo field', (key_value_metadata) => {
      expect(convertGeoParquet(key_value_metadata, schema)).toBeUndefined()
    })

    test('parses valid metadata', () => {
      const value = {
        version: '1.0.0',
        columns: {
          a: {
            encoding: 'WKB',
            geometry_types: ['Point', 'LineString'],
          },
        },
        primary_column: 'a',
      }
      expect(convertGeoParquet([{ key: 'geo', value: JSON.stringify(value) }], schema)).toEqual(value)
    })
  })
  describe('parseGeoParquet', () => {
    /** @type Parameters<parseGeoParquet>[1] */
    const schema = [
      { name: 'a', type: 'BYTE_ARRAY' },
      { name: 'b', type: 'BYTE_ARRAY' },
    ]

    test.for([
      [],
      [{ key: 'other', value: '{}' }],
    ])('throws for missing geo metadata', (key_value_metadata) => {
      expect(() => parseGeoParquet(key_value_metadata, schema)).toThrow('No geo field')
    })

    test.for([
      '',
      '{',
      'undef',
    ])('throws for invalid JSON geo field', (value) => {
      expect(() => parseGeoParquet([{ key: 'geo', value }], schema)).toThrow('not valid JSON')
    })

    test.for([
      'string',
      [],
      0,
      null,
    ])('throws if geo is not an object', (geo) => {
      expect(() => parseGeoParquet([{ key: 'geo', value: JSON.stringify(geo) }], schema)).toThrow('not an object')
    })

    test.for([
      {},
      { version: 0 },
    ])('throws if version is not a string', (geo) => {
      expect(() => parseGeoParquet([{ key: 'geo', value: JSON.stringify(geo) }], schema)).toThrow('missing or invalid version')
    })

    test.for([
      'unknown',
      '',
      '1.2.0',
    ])('throws if version is not supported', (version) => {
      expect(() => parseGeoParquet([{ key: 'geo', value: JSON.stringify({ version }) }], schema)).toThrow('Unsupported GeoParquet version')
    })

    test.for([
      {},
      { columns: undefined },
      { columns: null },
      { columns: 'string' },
      { columns: [] },
    ])('throws for missing or invalid columns', (value) => {
      expect(() => parseGeoParquet([{ key: 'geo', value: JSON.stringify({ version: '1.0.0', ...value }) }], schema)).toThrow('missing or invalid columns')
    })

    // See parseColumn for tests about the columns field

    test.for([
      {},
      { primary_column: undefined },
      { primary_column: null },
      { primary_column: [] },
      { primary_column: {} },
    ])('throws for missing or invalid primary_column', (value) => {
      expect(() => parseGeoParquet([{ key: 'geo', value: JSON.stringify({ version: '1.0.0', columns: {}, ...value }) }], schema)).toThrow('missing or invalid primary_column')
    })

    test.for([
      'c',
    ])('throws for missing or invalid primary_column', (primary_column) => {
      expect(() => parseGeoParquet([{ key: 'geo', value: JSON.stringify({
        version: '1.0.0',
        columns: {
          a: {
            encoding: 'WKB',
            geometry_types: ['Point', 'LineString'],
          },
        },
        primary_column,
      }) }], schema)).toThrow('does not exist in columns')
    })

    test('parses valid metadata', () => {
      const value = {
        version: '1.0.0',
        columns: {
          a: {
            encoding: 'WKB',
            geometry_types: ['Point', 'LineString'],
          },
        },
        primary_column: 'a',
      }
      expect(parseGeoParquet([{ key: 'geo', value: JSON.stringify(value) }], schema)).toEqual(value)
    })

    test('ignores extra fields', () => {
      const value = {
        version: '1.0.0',
        columns: {
          a: {
            encoding: 'WKB',
            geometry_types: ['Point', 'LineString'],
          },
        },
        primary_column: 'a',
      }
      expect(parseGeoParquet([{ key: 'geo', value: JSON.stringify({ ...value, 'extra': 'field' }) }], schema)).toEqual(value)
    })

    test.for(['1.0.0', '1.1.0'])('supports version %s', (version) => {
      const value = {
        version,
        columns: {
          a: {
            encoding: 'WKB',
            geometry_types: ['Point', 'LineString'],
          },
        },
        primary_column: 'a',
      }
      expect(parseGeoParquet([{ key: 'geo', value: JSON.stringify(value) }], schema)).toEqual(value)
    })

    test('supports multiple geospatial columns', () => {
      const value = {
        version: '1.0.0',
        columns: {
          a: {
            encoding: 'WKB',
            geometry_types: ['Point', 'LineString'],
          },
          b: {
            encoding: 'WKB',
            geometry_types: ['Polygon'],
          },
        },
        primary_column: 'b',
      }
      expect(parseGeoParquet([{ key: 'geo', value: JSON.stringify(value) }], schema)).toEqual(value)
    })

    test('parses the official v1.0.0 GeoParquet example', () => {
      // The file is here: https://github.com/opengeospatial/geoparquet/blob/v1.0.0/examples/example.parquet
      const value = '{"version": "1.0.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"], "crs": {"$schema": "https://proj.org/schemas/v0.6/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84 (CRS84)", "datum_ensemble": {"name": "World Geodetic System 1984 ensemble", "members": [{"name": "World Geodetic System 1984 (Transit)", "id": {"authority": "EPSG", "code": 1166}}, {"name": "World Geodetic System 1984 (G730)", "id": {"authority": "EPSG", "code": 1152}}, {"name": "World Geodetic System 1984 (G873)", "id": {"authority": "EPSG", "code": 1153}}, {"name": "World Geodetic System 1984 (G1150)", "id": {"authority": "EPSG", "code": 1154}}, {"name": "World Geodetic System 1984 (G1674)", "id": {"authority": "EPSG", "code": 1155}}, {"name": "World Geodetic System 1984 (G1762)", "id": {"authority": "EPSG", "code": 1156}}, {"name": "World Geodetic System 1984 (G2139)", "id": {"authority": "EPSG", "code": 1309}}], "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563}, "accuracy": "2.0", "id": {"authority": "EPSG", "code": 6326}}, "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}, {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}]}, "scope": "Not known.", "area": "World.", "bbox": {"south_latitude": -90, "west_longitude": -180, "north_latitude": 90, "east_longitude": 180}, "id": {"authority": "OGC", "code": "CRS84"}}, "edges": "planar", "bbox": [-180.0, -90.0, 180.0, 83.6451]}}}'
      const expected = {
        columns: {
          geometry: {
            encoding: 'WKB',
            geometry_types: ['Polygon', 'MultiPolygon'],
          },
        },
        primary_column: 'geometry',
        version: '1.0.0',
      }
      expect(parseGeoParquet([{ key: 'geo', value }], [{ name: 'geometry', type: 'BYTE_ARRAY' }])).toEqual(expected)
    })

    test('parses the official v1.1.0 GeoParquet example', () => {
      // The file is here: https://github.com/opengeospatial/geoparquet/blob/v1.1.0%2Bp1/examples/example.parquet
      const value = '{"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"], "crs": {"$schema": "https://proj.org/schemas/v0.6/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84 (CRS84)", "datum_ensemble": {"name": "World Geodetic System 1984 ensemble", "members": [{"name": "World Geodetic System 1984 (Transit)", "id": {"authority": "EPSG", "code": 1166}}, {"name": "World Geodetic System 1984 (G730)", "id": {"authority": "EPSG", "code": 1152}}, {"name": "World Geodetic System 1984 (G873)", "id": {"authority": "EPSG", "code": 1153}}, {"name": "World Geodetic System 1984 (G1150)", "id": {"authority": "EPSG", "code": 1154}}, {"name": "World Geodetic System 1984 (G1674)", "id": {"authority": "EPSG", "code": 1155}}, {"name": "World Geodetic System 1984 (G1762)", "id": {"authority": "EPSG", "code": 1156}}, {"name": "World Geodetic System 1984 (G2139)", "id": {"authority": "EPSG", "code": 1309}}], "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563}, "accuracy": "2.0", "id": {"authority": "EPSG", "code": 6326}}, "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}, {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}]}, "scope": "Not known.", "area": "World.", "bbox": {"south_latitude": -90, "west_longitude": -180, "north_latitude": 90, "east_longitude": 180}, "id": {"authority": "OGC", "code": "CRS84"}}, "edges": "planar", "bbox": [-180.0, -90.0, 180.0, 83.6451], "covering": {"bbox": {"xmin": ["bbox", "xmin"], "ymin": ["bbox", "ymin"], "xmax": ["bbox", "xmax"], "ymax": ["bbox", "ymax"]}}}}}'
      const expected = {
        columns: {
          geometry: {
            encoding: 'WKB',
            geometry_types: ['Polygon', 'MultiPolygon'],
          },
        },
        primary_column: 'geometry',
        version: '1.1.0',
      }
      expect(parseGeoParquet([{ key: 'geo', value }], [{ name: 'geometry', type: 'BYTE_ARRAY' }])).toEqual(expected)
    })
  })
  describe('parseColumn', () => {
    /** @type Parameters<parseColumn>[0] */
    const options = {
      columnName: 'a',
      metadata: {
        encoding: 'WKB',
        geometry_types: ['Point', 'LineString'],
      },
      columnNames: ['a', 'b'],
      version: '1.0.0',
    }

    test.for([
      [],
      ['b', 'c'],
    ])('throws for unknown column', (columnNames) => {
      expect(() => parseColumn({ ...options, columnNames })).toThrow('no BYTE_ARRAY column called')
    })

    test.for([
      undefined,
      null,
      'invalid',
      [],
    ])('throws for invalid metadata', (metadata) => {
      expect(() => parseColumn({ ...options, metadata })).toThrow('not an object')
    })

    test.for([
      {},
      { 'a': 0 },
    ])('throws for missing encoding', (metadata) => {
      expect(() => parseColumn({ ...options, metadata })).toThrow('missing encoding')
    })

    test.for([
      undefined,
      0,
      '',
      'unknown',
      'wkb', // case-sensitive match
    ])('throws for invalid encoding', (encoding) => {
      expect(() => parseColumn({ ...options, metadata: { encoding } })).toThrow('invalid encoding')
    })

    test.for([
      'point',
      'linestring',
    ])('throws for invalid encoding in v1.0.0', (encoding) => {
      expect(() => parseColumn({ ...options, metadata: { encoding } })).toThrow('must be "WKB"')
    })

    test.for(unsupportedEncodings)('throws for unsupported encoding %s in v1.1.0', (encoding) => {
      expect(() => parseColumn({ ...options, version: '1.1.0', metadata: { encoding } })).toThrow('only "WKB" is supported')
    })

    test.for([
      undefined,
      null,
      'invalid',
      {},
    ])('throws for missing or invalid geometry_types', (geometry_columns) => {
      expect(() => parseColumn({ ...options, metadata: { encoding: 'WKB', geometry_columns } })).toThrow('missing or invalid geometry_types')
    })

    test.for(
      unsupportedGeometryTypes
    )('throws for unsupported geometry_types', (geometry_type) => {
      expect(() => parseColumn({ ...options, metadata: { encoding: 'WKB', geometry_types: [geometry_type] } })).toThrow('is not supported')
    })

    test.for([
      [0],
      ['unknown'],
      ['Point  Z'],
      ['point'],
      ['Point', 'unknown'],
      ...supportedGeometryTypes.map(d => [d.toLowerCase()]), // case-sensitive match
    ])('throws for invalid geometry_types', (geometry_types) => {
      expect(() => parseColumn({ ...options, metadata: { encoding: 'WKB', geometry_types } })).toThrow('has invalid geometry_type')
    })

    test.for([
      ['Point', 'Point'],
      ['Point', 'LineString', 'Point'],
    ])('throws for duplicate geometry_types', (geometry_types) => {
      expect(() => parseColumn({ ...options, metadata: { encoding: 'WKB', geometry_types } })).toThrow('has duplicate geometry_type')
    })

    test('parses valid metadata', () => {
      expect(parseColumn(options)).toEqual(options.metadata)
    })

    test('ignores extra fields', () => {
      expect(parseColumn({ ...options, metadata: { encoding: 'WKB', geometry_types: ['Point'], 'another': 'field' } })).toEqual({ encoding: 'WKB', geometry_types: ['Point'] })
    })

    test('supports empty geometry_types', () => {
      expect(parseColumn({ ...options, metadata: { encoding: 'WKB', geometry_types: [] } })).toEqual({ encoding: 'WKB', geometry_types: [] })
    })

    test.for(supportedGeometryTypes)('supports %s geometry_type', (geometry_type) => {
      expect(parseColumn({ ...options, metadata: { encoding: 'WKB', geometry_types: [geometry_type] } })).toEqual({ encoding: 'WKB', geometry_types: [geometry_type] })
    })

  })
})
