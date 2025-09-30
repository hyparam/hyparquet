/**
 * WKB (Well-Known Binary) decoder for geometry objects.
 *
 * @import {DataReader, Geometry} from '../src/types.js'
 * @param {DataReader} reader
 * @returns {Geometry} geometry object
 */
export function wkbToGeojson(reader) {
  const { view } = reader
  const isLittleEndian = view.getUint8(reader.offset++) === 1

  // Read geometry type
  const rawGeometryType = view.getUint32(reader.offset, isLittleEndian)
  reader.offset += 4
  const { type: geometryType, hasZ, hasM, hasSRID } = getFlags(rawGeometryType)

  if (hasSRID) {
    reader.offset += 4 // SRID is int32
  }

  if (geometryType === 1) { // Point
    return { type: 'Point', coordinates: readPosition(reader, isLittleEndian, hasZ, hasM) }
  } else if (geometryType === 2) { // LineString
    const numPoints = view.getUint32(reader.offset, isLittleEndian)
    reader.offset += 4
    const coords = new Array(numPoints)
    for (let i = 0; i < numPoints; i++) {
      coords[i] = readPosition(reader, isLittleEndian, hasZ, hasM)
    }
    return { type: 'LineString', coordinates: coords }
  } else if (geometryType === 3) { // Polygon
    const numRings = view.getUint32(reader.offset, isLittleEndian)
    reader.offset += 4
    const coords = new Array(numRings)
    for (let r = 0; r < numRings; r++) {
      const numPoints = view.getUint32(reader.offset, isLittleEndian)
      reader.offset += 4
      const ring = new Array(numPoints)
      for (let p = 0; p < numPoints; p++) {
        ring[p] = readPosition(reader, isLittleEndian, hasZ, hasM)
      }
      coords[r] = ring
    }
    return { type: 'Polygon', coordinates: coords }
  } else if (geometryType === 4) { // MultiPoint
    const numPoints = view.getUint32(reader.offset, isLittleEndian)
    reader.offset += 4
    const points = new Array(numPoints)
    for (let i = 0; i < numPoints; i++) {
      const pointIsLittleEndian = view.getUint8(reader.offset++) === 1
      const rawPointType = view.getUint32(reader.offset, pointIsLittleEndian)
      reader.offset += 4
      const { type: pointType, hasZ: pointHasZ, hasM: pointHasM, hasSRID: pointHasSRID } = getFlags(rawPointType)

      if (pointHasSRID) {
        reader.offset += 4
      }

      if (pointType !== 1) {
        throw new Error(`Expected Point in MultiPoint, got ${rawPointType}`)
      }

      points[i] = readPosition(reader, pointIsLittleEndian, pointHasZ, pointHasM)
    }
    return { type: 'MultiPoint', coordinates: points }
  } else if (geometryType === 5) { // MultiLineString
    const numLineStrings = view.getUint32(reader.offset, isLittleEndian)
    reader.offset += 4
    const lineStrings = new Array(numLineStrings)
    for (let i = 0; i < numLineStrings; i++) {
      const lineIsLittleEndian = view.getUint8(reader.offset++) === 1
      const rawLineType = view.getUint32(reader.offset, lineIsLittleEndian)
      reader.offset += 4
      const { type: lineType, hasZ: lineHasZ, hasM: lineHasM, hasSRID: lineHasSRID } = getFlags(rawLineType)

      if (lineHasSRID) {
        reader.offset += 4
      }
      if (lineType !== 2) {
        throw new Error(`Expected LineString in MultiLineString, got ${rawLineType}`)
      }

      const numPoints = view.getUint32(reader.offset, lineIsLittleEndian)
      reader.offset += 4
      const coords = new Array(numPoints)
      for (let p = 0; p < numPoints; p++) {
        coords[p] = readPosition(reader, lineIsLittleEndian, lineHasZ, lineHasM)
      }
      lineStrings[i] = coords
    }
    return { type: 'MultiLineString', coordinates: lineStrings }
  } else if (geometryType === 6) { // MultiPolygon
    const numPolygons = view.getUint32(reader.offset, isLittleEndian)
    reader.offset += 4
    const polygons = new Array(numPolygons)
    for (let i = 0; i < numPolygons; i++) {
      const polyIsLittleEndian = view.getUint8(reader.offset++) === 1
      const rawPolyType = view.getUint32(reader.offset, polyIsLittleEndian)
      reader.offset += 4
      const { type: polyType, hasZ: polyHasZ, hasM: polyHasM, hasSRID: polyHasSRID } = getFlags(rawPolyType)

      if (polyHasSRID) {
        reader.offset += 4
      }

      if (polyType !== 3) {
        throw new Error(`Expected Polygon in MultiPolygon, got ${rawPolyType}`)
      }

      const numRings = view.getUint32(reader.offset, polyIsLittleEndian)
      reader.offset += 4

      const pgCoords = new Array(numRings)
      for (let r = 0; r < numRings; r++) {
        const numPoints = view.getUint32(reader.offset, polyIsLittleEndian)
        reader.offset += 4
        const ring = new Array(numPoints)
        for (let p = 0; p < numPoints; p++) {
          ring[p] = readPosition(reader, polyIsLittleEndian, polyHasZ, polyHasM)
        }
        pgCoords[r] = ring
      }
      polygons[i] = pgCoords
    }
    return { type: 'MultiPolygon', coordinates: polygons }
  } else if (geometryType === 7) { // GeometryCollection
    const numGeometries = view.getUint32(reader.offset, isLittleEndian)
    reader.offset += 4
    const geometries = new Array(numGeometries)
    for (let i = 0; i < numGeometries; i++) {
      geometries[i] = wkbToGeojson(reader)
    }
    return { type: 'GeometryCollection', geometries }
  } else {
    throw new Error(`Unsupported geometry type: ${rawGeometryType}`)
  }
}

/**
 * Extract EWKB flag metadata and normalize the base geometry type.
 *
 * @param {number} type
 * @returns {{type: number, hasZ: boolean, hasM: boolean, hasSRID: boolean}}
 */
function getFlags(type) {
  let hasZ = (type & 0x80000000) !== 0
  let hasM = (type & 0x40000000) !== 0
  const hasSRID = (type & 0x20000000) !== 0

  if (hasZ) {
    type &= ~0x80000000
  }
  if (hasM) {
    type &= ~0x40000000
  }
  if (hasSRID) {
    type &= ~0x20000000
  }

  if (type >= 3000) {
    hasZ = true
    hasM = true
    type -= 3000
  } else if (type >= 2000) {
    hasM = true
    type -= 2000
  } else if (type >= 1000) {
    hasZ = true
    type -= 1000
  }

  return { type, hasZ, hasM, hasSRID }
}

/**
 * @param {DataReader} reader
 * @param {boolean} isLittleEndian
 * @param {boolean} hasZ
 * @param {boolean} hasM
 * @returns {number[]}
 */
function readPosition(reader, isLittleEndian, hasZ, hasM) {
  const { view } = reader
  const x = view.getFloat64(reader.offset, isLittleEndian)
  reader.offset += 8
  const y = view.getFloat64(reader.offset, isLittleEndian)
  reader.offset += 8
  const coordinates = [x, y]

  if (hasZ) {
    const z = view.getFloat64(reader.offset, isLittleEndian)
    reader.offset += 8
    coordinates.push(z)
  }

  if (hasM) {
    const m = view.getFloat64(reader.offset, isLittleEndian)
    reader.offset += 8
    coordinates.push(m)
  }

  return coordinates
}
