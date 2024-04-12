import fs from 'fs'
import { describe, expect, it } from 'vitest'
import { parquetMetadata, parquetMetadataAsync } from '../src/hyparquet.js'
import { toJson } from '../src/utils.js'
import { fileToAsyncBuffer, fileToJson, readFileToArrayBuffer } from './helpers.js'

const files = fs.readdirSync('test/files').filter(f => f.endsWith('.parquet'))

describe('parquetMetadata', () => {
  files.forEach(file => {
    it(`parse metadata from ${file}`, async () => {
      const arrayBuffer = await readFileToArrayBuffer(`test/files/${file}`)
      const result = toJson(parquetMetadata(arrayBuffer))
      const base = file.replace('.parquet', '')
      const expected = fileToJson(`test/files/${base}.metadata.json`)
      expect(result, JSON.stringify(result, null, 2)).toEqual(expected)
    })
  })

  it('throws for arrayBuffer undefined', () => {
    // @ts-expect-error testing invalid input
    expect(() => parquetMetadata(undefined)).toThrow('parquet arrayBuffer is required')
  })

  it('throws for a too short file', () => {
    const arrayBuffer = new ArrayBuffer(0)
    expect(() => parquetMetadata(arrayBuffer)).toThrow('parquet file is too short')
  })

  it('throws for invalid metadata length', () => {
    const arrayBuffer = new ArrayBuffer(12)
    const view = new DataView(arrayBuffer)
    view.setUint32(0, 0x31524150, true) // magic number PAR1
    view.setUint32(4, 1000, true) // 1000 bytes exceeds buffer
    view.setUint32(8, 0x31524150, true) // magic number PAR1
    expect(() => parquetMetadata(arrayBuffer))
      .toThrow('parquet metadata length 1000 exceeds available buffer 4')
  })

  it('throws for invalid magic number', () => {
    const arrayBuffer = new ArrayBuffer(8)
    expect(() => parquetMetadata(arrayBuffer))
      .toThrow('parquet file invalid (footer != PAR1)')
  })

  it('throws for invalid metadata length', () => {
    const { buffer } = new Uint8Array([255, 255, 255, 255, 80, 65, 82, 49])
    expect(() => parquetMetadata(buffer))
      .toThrow('parquet metadata length 4294967295 exceeds available buffer 0')
  })
})

describe('parquetMetadataAsync', () => {
  files.forEach(file => {
    it(`parse metadata async from ${file}`, async () => {
      const asyncBuffer = fileToAsyncBuffer(`test/files/${file}`)
      const result = await parquetMetadataAsync(asyncBuffer)
      const base = file.replace('.parquet', '')
      const expected = fileToJson(`test/files/${base}.metadata.json`)
      expect(toJson(result)).toEqual(expected)
    })
  })

  it('throws for asyncBuffer undefined', async () => {
    const arrayBuffer = undefined
    await expect(parquetMetadataAsync(arrayBuffer)).rejects
      .toThrow('parquet asyncBuffer is required')
  })

  it('throws for invalid magic number', async () => {
    const { buffer } = new Uint8Array([255, 255, 255, 255, 255, 255, 255, 255])
    await expect(parquetMetadataAsync(buffer)).rejects
      .toThrow('parquet file invalid (footer != PAR1)')
  })

  it('throws for invalid metadata length', async () => {
    const { buffer } = new Uint8Array([255, 255, 255, 255, 80, 65, 82, 49])
    await expect(parquetMetadataAsync(buffer)).rejects
      .toThrow('parquet metadata length 4294967295 exceeds available buffer 0')
  })
})
