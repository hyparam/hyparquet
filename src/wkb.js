/**
 * WKB (Well-Known Binary) decoder for geometry objects.
 *
 * @import {DataReader, Geometry} from '../src/types.js'
 * @param {DataReader} reader
 * @returns {Geometry} geometry object
 */
export function wkbToGeojson(reader) {
  const flags = getFlags(reader)

  if (flags.type === 1) { // Point
    return { type: 'Point', coordinates: readPosition(reader, flags) }
  } else if (flags.type === 2) { // LineString
    return { type: 'LineString', coordinates: readLine(reader, flags) }
  } else if (flags.type === 3) { // Polygon
    return { type: 'Polygon', coordinates: readPolygon(reader, flags) }
  } else if (flags.type === 4) { // MultiPoint
    const points = []
    for (let i = 0; i < flags.count; i++) {
      points.push(readPosition(reader, getFlags(reader)))
    }
    return { type: 'MultiPoint', coordinates: points }
  } else if (flags.type === 5) { // MultiLineString
    const lines = []
    for (let i = 0; i < flags.count; i++) {
      lines.push(readLine(reader, getFlags(reader)))
    }
    return { type: 'MultiLineString', coordinates: lines }
  } else if (flags.type === 6) { // MultiPolygon
    const polygons = []
    for (let i = 0; i < flags.count; i++) {
      polygons.push(readPolygon(reader, getFlags(reader)))
    }
    return { type: 'MultiPolygon', coordinates: polygons }
  } else if (flags.type === 7) { // GeometryCollection
    const geometries = []
    for (let i = 0; i < flags.count; i++) {
      geometries.push(wkbToGeojson(reader))
    }
    return { type: 'GeometryCollection', geometries }
  } else {
    throw new Error(`Unsupported geometry type: ${flags.type}`)
  }
}

/**
 * @typedef {object} WkbFlags
 * @property {boolean} littleEndian
 * @property {number} type
 * @property {number} dim
 * @property {number} count
 */

/**
 * Extract ISO WKB flags and base geometry type.
 *
 * @param {DataReader} reader
 * @returns {WkbFlags}
 */
function getFlags(reader) {
  const { view } = reader
  const littleEndian = view.getUint8(reader.offset++) === 1
  const rawType = view.getUint32(reader.offset, littleEndian)
  reader.offset += 4

  const type = rawType % 1000
  const flags = Math.floor(rawType / 1000)

  let count = 0
  if (type > 1 && type <= 7) {
    count = view.getUint32(reader.offset, littleEndian)
    reader.offset += 4
  }

  // XY, XYZ, XYM, XYZM
  let dim = 2
  if (flags) dim++
  if (flags === 3) dim++

  return { littleEndian, type, dim, count }
}

/**
 * @param {DataReader} reader
 * @param {WkbFlags} flags
 * @returns {number[]}
 */
function readPosition(reader, flags) {
  const points = []
  for (let i = 0; i < flags.dim; i++) {
    const coord = reader.view.getFloat64(reader.offset, flags.littleEndian)
    reader.offset += 8
    points.push(coord)
  }
  return points
}

/**
 * @param {DataReader} reader
 * @param {WkbFlags} flags
 * @returns {number[][]}
 */
function readLine(reader, flags) {
  const points = []
  for (let i = 0; i < flags.count; i++) {
    points.push(readPosition(reader, flags))
  }
  return points
}

/**
 * @param {DataReader} reader
 * @param {WkbFlags} flags
 * @returns {number[][][]}
 */
function readPolygon(reader, flags) {
  const { view } = reader
  const rings = []
  for (let r = 0; r < flags.count; r++) {
    const count = view.getUint32(reader.offset, flags.littleEndian)
    reader.offset += 4
    rings.push(readLine(reader, { ...flags, count }))
  }
  return rings
}
