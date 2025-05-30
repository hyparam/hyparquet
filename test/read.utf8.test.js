import { describe, expect, it } from 'vitest'
import { parquetReadObjects } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'

describe('parquetRead utf8', () => {
  it('default utf8 behavior', async () => {
    const file = await asyncBufferFromFile('test/files/strings.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows).toEqual([
      { bytes: 'alpha', c_utf8: 'alpha', l_utf8: 'alpha' },
      { bytes: 'bravo', c_utf8: 'bravo', l_utf8: 'bravo' },
      { bytes: 'charlie', c_utf8: 'charlie', l_utf8: 'charlie' },
      { bytes: 'delta', c_utf8: 'delta', l_utf8: 'delta' },
    ])
  })

  it('utf8 = true', async () => {
    const file = await asyncBufferFromFile('test/files/strings.parquet')
    const rows = await parquetReadObjects({ file, utf8: true })
    expect(rows).toEqual([
      { bytes: 'alpha', c_utf8: 'alpha', l_utf8: 'alpha' },
      { bytes: 'bravo', c_utf8: 'bravo', l_utf8: 'bravo' },
      { bytes: 'charlie', c_utf8: 'charlie', l_utf8: 'charlie' },
      { bytes: 'delta', c_utf8: 'delta', l_utf8: 'delta' },
    ])
  })

  it('utf8 = false', async () => {
    const file = await asyncBufferFromFile('test/files/strings.parquet')
    const rows = await parquetReadObjects({ file, utf8: false })
    expect(rows).toEqual([
      {
        bytes: new Uint8Array([97, 108, 112, 104, 97]),
        c_utf8: 'alpha',
        l_utf8: 'alpha',
      },
      {
        bytes: new Uint8Array([98, 114, 97, 118, 111]),
        c_utf8: 'bravo',
        l_utf8: 'bravo',
      },
      {
        bytes: new Uint8Array([99, 104, 97, 114, 108, 105, 101]),
        c_utf8: 'charlie',
        l_utf8: 'charlie',
      },
      {
        bytes: new Uint8Array([100, 101, 108, 116, 97]),
        c_utf8: 'delta',
        l_utf8: 'delta',
      },
    ])
  })
})
