
const geometryTypePoint = 1
const geometryTypeLineString = 2
const geometryTypePolygon = 3
const geometryTypeMultiPoint = 4
const geometryTypeMultiLineString = 5
const geometryTypeMultiPolygon = 6
// const geometryTypeGeometryCollection = 7
// const geometryTypeCircularString = 8
// const geometryTypeCompoundCurve = 9
// const geometryTypeCurvePolygon = 10
// const geometryTypeMultiCurve = 11
// const geometryTypeMultiSurface = 12
// const geometryTypeCurve = 13
// const geometryTypeSurface = 14
// const geometryTypePolyhedralSurface = 15
// const geometryTypeTIN = 16
// const geometryTypeTriangle = 17
// const geometryTypeCircle = 18
// const geometryTypeGeodesicString = 19
// const geometryTypeEllipticalCurve = 20
// const geometryTypeNurbsCurve = 21
// const geometryTypeClothoid = 22
// const geometryTypeSpiralCurve = 23
// const geometryTypeCompoundSurface = 24

/**
 * WKB (Well Known Binary) decoder for geometry objects.
 *
 * @import { Geometry } from '../src/types.js'
 * @param {Uint8Array} wkb
 * @returns {Geometry} GeoJSON geometry object
 */
export function decodeWKB(wkb) {
  const dv = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength)
  let offset = 0

  // Byte order: 0 = big-endian, 1 = little-endian
  const byteOrder = wkb[offset]; offset += 1
  const isLittleEndian = byteOrder === 1

  // Read geometry type
  const geometryType = dv.getUint32(offset, isLittleEndian)
  offset += 4

  // WKB geometry types (OGC):
  if (geometryType === geometryTypePoint) {
    // Point
    const x = dv.getFloat64(offset, isLittleEndian); offset += 8
    const y = dv.getFloat64(offset, isLittleEndian); offset += 8
    return { type: 'Point', coordinates: [x, y] }
  } else if (geometryType === geometryTypeLineString) {
    // LineString
    const numPoints = dv.getUint32(offset, isLittleEndian); offset += 4
    const coords = []
    for (let i = 0; i < numPoints; i++) {
      const x = dv.getFloat64(offset, isLittleEndian); offset += 8
      const y = dv.getFloat64(offset, isLittleEndian); offset += 8
      coords.push([x, y])
    }
    return { type: 'LineString', coordinates: coords }
  } else if (geometryType === geometryTypePolygon) {
    // Polygon
    const numRings = dv.getUint32(offset, isLittleEndian); offset += 4
    const coords = []
    for (let r = 0; r < numRings; r++) {
      const numPoints = dv.getUint32(offset, isLittleEndian); offset += 4
      const ring = []
      for (let p = 0; p < numPoints; p++) {
        const x = dv.getFloat64(offset, isLittleEndian); offset += 8
        const y = dv.getFloat64(offset, isLittleEndian); offset += 8
        ring.push([x, y])
      }
      coords.push(ring)
    }
    return { type: 'Polygon', coordinates: coords }
  } else if (geometryType === geometryTypeMultiPolygon) {
    // MultiPolygon
    const numPolygons = dv.getUint32(offset, isLittleEndian); offset += 4
    const polygons = []
    for (let i = 0; i < numPolygons; i++) {
      // Each polygon has its own byte order & geometry type
      const polyIsLittleEndian = wkb[offset] === 1; offset += 1
      const polyType = dv.getUint32(offset, polyIsLittleEndian); offset += 4
      if (polyType !== geometryTypePolygon) {
        throw new Error(`Expected Polygon in MultiPolygon, got ${polyType}`)
      }
      const numRings = dv.getUint32(offset, polyIsLittleEndian); offset += 4

      const pgCoords = []
      for (let r = 0; r < numRings; r++) {
        const numPoints = dv.getUint32(offset, polyIsLittleEndian); offset += 4
        const ring = []
        for (let p = 0; p < numPoints; p++) {
          const x = dv.getFloat64(offset, polyIsLittleEndian); offset += 8
          const y = dv.getFloat64(offset, polyIsLittleEndian); offset += 8
          ring.push([x, y])
        }
        pgCoords.push(ring)
      }
      polygons.push(pgCoords)
    }
    return { type: 'MultiPolygon', coordinates: polygons }
  } else if (geometryType === geometryTypeMultiPoint) {
    // MultiPoint
    const numPoints = dv.getUint32(offset, isLittleEndian); offset += 4
    const points = []
    for (let i = 0; i < numPoints; i++) {
      // Each point has its own byte order & geometry type
      const pointIsLittleEndian = wkb[offset] === 1; offset += 1
      const pointType = dv.getUint32(offset, pointIsLittleEndian); offset += 4
      if (pointType !== geometryTypePoint) {
        throw new Error(`Expected Point in MultiPoint, got ${pointType}`)
      }
      const x = dv.getFloat64(offset, pointIsLittleEndian); offset += 8
      const y = dv.getFloat64(offset, pointIsLittleEndian); offset += 8
      points.push([x, y])
    }
    return { type: 'MultiPoint', coordinates: points }
  } else if (geometryType === geometryTypeMultiLineString) {
    // MultiLineString
    const numLineStrings = dv.getUint32(offset, isLittleEndian); offset += 4
    const lineStrings = []
    for (let i = 0; i < numLineStrings; i++) {
      // Each line has its own byte order & geometry type
      const lineIsLittleEndian = wkb[offset] === 1; offset += 1
      const lineType = dv.getUint32(offset, lineIsLittleEndian); offset += 4
      if (lineType !== geometryTypeLineString) {
        throw new Error(`Expected LineString in MultiLineString, got ${lineType}`)
      }
      const numPoints = dv.getUint32(offset, isLittleEndian); offset += 4
      const coords = []
      for (let p = 0; p < numPoints; p++) {
        const x = dv.getFloat64(offset, lineIsLittleEndian); offset += 8
        const y = dv.getFloat64(offset, lineIsLittleEndian); offset += 8
        coords.push([x, y])
      }
      lineStrings.push(coords)
    }
    return { type: 'MultiLineString', coordinates: lineStrings }
  } else {
    throw new Error(`Unsupported geometry type: ${geometryType}`)
  }
}
