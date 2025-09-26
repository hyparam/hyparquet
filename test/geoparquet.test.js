import { describe, expect, test } from 'vitest'
import { convertGeoParquet, encodings, geometryTypes, parseColumn, parseGeoParquet } from '../src/geoparquet.js'

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
            geometry_types: ['Point', 'LineString Z'],
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
            geometry_types: ['Point', 'LineString Z'],
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
            geometry_types: ['Point', 'LineString Z'],
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
            geometry_types: ['Point', 'LineString Z'],
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
            geometry_types: ['Point', 'LineString Z'],
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
            geometry_types: ['Point', 'LineString Z'],
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
  })
  describe('parseColumn', () => {
    /** @type Parameters<parseColumn>[0] */
    const options = {
      columnName: 'a',
      metadata: {
        encoding: 'WKB',
        geometry_types: ['Point', 'LineString Z'],
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

    test.for(encodings.filter(d => d !== 'WKB'))('throws for unsupported encoding %s in v1.1.0', (encoding) => {
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

    test.for([
      [0],
      ['unknown'],
      ['Point  Z'],
      ['point'],
      ['Point', 'unknown'],
      ...geometryTypes.map(d => [d.toLowerCase()]), // case-sensitive match
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

    test.for(geometryTypes)('supports %s geometry_type', (geometry_type) => {
      expect(parseColumn({ ...options, metadata: { encoding: 'WKB', geometry_types: [geometry_type] } })).toEqual({ encoding: 'WKB', geometry_types: [geometry_type] })
    })

  })
})
