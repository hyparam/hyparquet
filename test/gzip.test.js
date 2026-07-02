import { gzipSync } from 'zlib'
import { describe, expect, it } from 'vitest'
import { gzipUncompress } from '../src/gzip.js'
import { parquetRead, toJson } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { fileToJson } from './helpers.js'

describe('gzipUncompress', () => {
  it('decompresses a gzip member', async () => {
    const input = new TextEncoder().encode('hyparquet '.repeat(100))
    const output = await gzipUncompress(gzipSync(input), input.length)
    expect(output).toEqual(input)
  })

  it('decompresses concatenated gzip members', async () => {
    const a = new TextEncoder().encode('hello ')
    const b = new TextEncoder().encode('world')
    const ga = gzipSync(a)
    const gb = gzipSync(b)
    const concat = new Uint8Array(ga.length + gb.length)
    concat.set(ga, 0)
    concat.set(gb, ga.length)
    const output = await gzipUncompress(concat, a.length + b.length)
    expect(new TextDecoder().decode(output)).toBe('hello world')
  })

  it('throws when output exceeds expected length', async () => {
    const input = new TextEncoder().encode('hyparquet '.repeat(100))
    await expect(gzipUncompress(gzipSync(input), 10))
      .rejects.toThrow('parquet gzip decompressed data exceeds expected length 10')
  })

  it('rejects on malformed input', async () => {
    await expect(gzipUncompress(new Uint8Array([1, 2, 3, 4]), 10))
      .rejects.toThrow()
  })
})

describe('parquetRead gzip files without compressors option', () => {
  it.for([
    'rle_boolean_encoding.parquet',
    'concatenated_gzip_members.parquet',
    'byte_stream_split_extended.gzip.parquet',
  ])('parse data from %s', async filename => {
    const file = await asyncBufferFromFile(`test/files/${filename}`)
    await parquetRead({
      file,
      onComplete(rows) {
        const base = filename.replace('.parquet', '')
        const expected = fileToJson(`test/files/${base}.json`)
        expect(JSON.parse(JSON.stringify(toJson(rows)))).toEqual(expected)
      },
    })
  })
})
