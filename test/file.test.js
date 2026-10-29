import { describe, expect, it } from 'vitest'
import { DEFAULT_PARSERS, convert, convertWithDictionary } from '../src/convert.js'
import { parquetMetadataAsync, parquetReadObjects } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'
import { getSchemaPath } from '../src/schema.js'

/** @import {ColumnDecoder, SchemaElement} from '../src/types.js' */

// Hand-encoded, uncompressed PLAIN pages with RLE definition levels.
// The optional FILE group uses Thrift logical type field 19 and reordered fields.
// Rows cover binary inline, empty inline with provenance, whole external file,
// external range beyond Number.MAX_SAFE_INTEGER, self-reference, and null group.
const fixture = 'test/files/file.parquet'
const empty = { inline: null, size: null, uri: null, checksum: null, offset: null, content_type: null }
const expected = [
  { file: { ...empty, inline: new Uint8Array([0, 255, 128]), content_type: 'image/png' } },
  { file: { ...empty, inline: new Uint8Array(), uri: 's3://bucket/original', offset: 3n, size: 0n } },
  { file: { ...empty, uri: 's3://bucket/whole', checksum: 'ETAG:abc' } },
  { file: { ...empty, uri: 'relative.bin', offset: 9007199254740993n, size: 7n } },
  { file: { ...empty, offset: 0n, size: 4n } },
  { file: undefined },
]

describe('FILE logical type', () => {
  it('recognizes the FILE annotation in serialized metadata', async () => {
    const metadata = await parquetMetadataAsync(await asyncBufferFromFile(fixture))
    expect(metadata.schema[1].logical_type).toEqual({ type: 'FILE' })
  })

  it.each([undefined, true, false])('reads FILE fields with utf8=%s', async utf8 => {
    const file = await asyncBufferFromFile(fixture)
    expect(await parquetReadObjects({ file, utf8 })).toEqual(expected)
  })

  it('reads a range of FILE rows', async () => {
    const file = await asyncBufferFromFile(fixture)
    expect(await parquetReadObjects({ file, rowStart: 1, rowEnd: 5 })).toEqual(expected.slice(1, 5))
  })

  it('preserves inline-only FILE data in dictionaries', () => {
    /** @type {SchemaElement[]} */
    const schema = [
      { name: 'schema', num_children: 1 },
      { name: 'file', num_children: 1, logical_type: { type: 'FILE' } },
      { name: 'inline', type: 'BYTE_ARRAY', repetition_type: 'OPTIONAL' },
    ]
    /** @type {ColumnDecoder} */
    const column = {
      pathInSchema: ['file', 'inline'],
      type: 'BYTE_ARRAY',
      element: schema[2],
      schemaPath: getSchemaPath(schema, ['file', 'inline']),
      codec: 'UNCOMPRESSED',
      parsers: DEFAULT_PARSERS,
    }
    const bytes = new Uint8Array([0, 255, 128])
    const dictionary = convert([bytes, new Uint8Array()], column)
    expect(convertWithDictionary(new Uint8Array([1, 0, 0]), dictionary, 'RLE_DICTIONARY', column))
      .toEqual([new Uint8Array(), bytes, bytes])

    // An ordinary struct's inline column still follows the utf8 option.
    delete schema[1].logical_type
    expect(convert([new TextEncoder().encode('text')], column)).toEqual(['text'])
  })
})
