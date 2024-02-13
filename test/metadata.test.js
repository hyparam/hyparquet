import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetMetadata, parquetMetadataAsync } from '../src/hyparquet.js'
import { toJson } from '../src/toJson.js'
import { fileToAsyncBuffer, fileToJson, readFileToArrayBuffer } from './helpers.js'

describe('parquetMetadata', () => {
  it('should parse metadata from all test files', async () => {
    const files = fs.readdirSync('test/files')
    for (const file of files) {
      if (!file.endsWith('.parquet')) continue
      const arrayBuffer = await readFileToArrayBuffer(`test/files/${file}`)
      const result = parquetMetadata(arrayBuffer)
      const base = file.replace('.parquet', '')
      const expected = fileToJson(`test/files/${base}.metadata.json`)
      expect(toJson(result)).containSubset(expected)
    }
  })

  it('should throw an error for a too short file', () => {
    const arrayBuffer = new ArrayBuffer(0)
    expect(() => parquetMetadata(arrayBuffer)).toThrow('parquet file is too short')
  })

  it('should throw an error for invalid metadata length', () => {
    const arrayBuffer = new ArrayBuffer(12)
    const view = new DataView(arrayBuffer)
    view.setUint32(0, 0x31524150, true) // magic number PAR1
    view.setUint32(4, 1000, true) // 1000 bytes exceeds buffer
    view.setUint32(8, 0x31524150, true) // magic number PAR1
    expect(() => parquetMetadata(arrayBuffer))
      .toThrow('parquet metadata length 1000 exceeds available buffer 4')
  })

  it('should throw an error for invalid magic number', () => {
    const arrayBuffer = new ArrayBuffer(8)
    expect(() => parquetMetadata(arrayBuffer))
      .toThrow('parquet file invalid (footer != PAR1)')
  })

  it('should throw an error for invalid metadata length', () => {
    const { buffer } = new Uint8Array([255, 255, 255, 255, 80, 65, 82, 49])
    expect(() => parquetMetadata(buffer))
      .toThrow('parquet metadata length 4294967295 exceeds available buffer 0')
  })
})

describe('parquetMetadataAsync', () => {
  it('should parse metadata asynchronously from all test files', async () => {
    const files = fs.readdirSync('test/files')
    for (const file of files) {
      if (!file.endsWith('.parquet')) continue
      const asyncBuffer = fileToAsyncBuffer(`test/files/${file}`)
      const result = await parquetMetadataAsync(asyncBuffer)
      const base = file.replace('.parquet', '')
      const expected = fileToJson(`test/files/${base}.metadata.json`)
      expect(toJson(result)).containSubset(expected)
    }
  })

  it('should throw an error for invalid magic number', () => {
    const { buffer } = new Uint8Array([255, 255, 255, 255, 255, 255, 255, 255])
    expect(parquetMetadataAsync(buffer)).rejects
      .toThrow('parquet file invalid (footer != PAR1)')
  })

  it('should throw an error for invalid metadata length', () => {
    const { buffer } = new Uint8Array([255, 255, 255, 255, 80, 65, 82, 49])
    expect(parquetMetadataAsync(buffer)).rejects
      .toThrow('parquet metadata length 4294967295 exceeds available buffer 0')
  })
})
