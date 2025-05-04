import { describe, expect, it } from 'vitest'
import { parquetRead, parquetReadObjects } from '../src/hyparquet.js'
import { asyncBufferFromFile } from '../src/utils.js'

describe('parquetRead with external file_path', () => {
  it('throws if fileProvider is not provided', async () => {
    const file = await asyncBufferFromFile('test/files/file_path/annotated.parquet')
    await expect(parquetRead({ file }))
      .rejects.toThrow('parquet column uses external file_path, fileProvider required')
  })

  it('does not require fileProvider if external column not queried', async () => {
    const file = await asyncBufferFromFile('test/files/file_path/annotated.parquet')
    const rows = await parquetReadObjects({ file, columns: ['quality'] })
    expect(rows).toEqual([
      { quality: 'good' },
      { quality: 'bad' },
      { quality: 'bad' },
      { quality: null },
      { quality: null },
      { quality: 'good' },
      { quality: 'bad' },
      { quality: 'bad' },
      { quality: null },
      { quality: null },
      { quality: 'good' },
      { quality: 'bad' },
      { quality: 'bad' },
      { quality: null },
      { quality: null },
    ])
  })

  it('reads from external column', async () => {
    const rows = await parquetReadObjects({
      file: await asyncBufferFromFile('test/files/file_path/annotated.parquet'),
      fileProvider: filePath => asyncBufferFromFile(`test/files/file_path/${filePath}`),
    })
    expect(rows).toEqual([
      { numbers: 1n, quality: 'good' },
      { numbers: 2n, quality: 'bad' },
      { numbers: 3n, quality: 'bad' },
      { numbers: 4n, quality: null },
      { numbers: 5n, quality: null },
      { numbers: 6n, quality: 'good' },
      { numbers: 7n, quality: 'bad' },
      { numbers: 8n, quality: 'bad' },
      { numbers: 9n, quality: null },
      { numbers: 10n, quality: null },
      { numbers: 11n, quality: 'good' },
      { numbers: 12n, quality: 'bad' },
      { numbers: 13n, quality: 'bad' },
      { numbers: 14n, quality: null },
      { numbers: 15n, quality: null },
    ])
  })
})
