import fs from 'fs'
import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { asyncBufferFromFile, toGeoJson, toJson } from '../src/node.js'

const baseDirectory = 'test/files/geojson'

describe('toGeoJson parse test files', () => {
  const files = fs.readdirSync(baseDirectory).filter(f => f.endsWith('.parquet'))

  files.forEach(filename => {
    it(`parse data from ${filename}`, async () => {
      const base = filename.replace('.parquet', '')
      const file = await asyncBufferFromFile(`${baseDirectory}/${filename}`)
      const geojson = await toGeoJson({ file })
      const expected = fileToJson(`${baseDirectory}/${base}.json`)
      expect(toJson(geojson)).toEqual(expected)
    })
  })

  // Parse compressed parquet files
  const compressedFiles = fs.readdirSync(`${baseDirectory}/compressed`)
  compressedFiles.forEach(filename => {
    it(`parse data from compressed ${filename}`, async () => {
      const file = await asyncBufferFromFile(`${baseDirectory}/compressed/${filename}`)
      const geojson = await toGeoJson({ file, compressors })
      const expected = fileToJson(`${baseDirectory}/compressed-example.json`)
      expect(toJson(geojson)).toEqual(expected)
    })
  })
})

/**
 * @param {string} filePath
 * @returns {any}
 */
function fileToJson(filePath) {
  const buffer = fs.readFileSync(filePath)
  return JSON.parse(buffer.toString())
}
