import { describe, expect, it } from 'vitest'
import { parquetReadObjects } from '../src/index.js'
import { asyncBufferFromFile } from '../src/node.js'

/**
 * Test for issue #147: struct children with different page counts
 * cause "parquet struct parsing error" when using rowEnd.
 *
 * The bug occurs when:
 * 1. A struct has multiple child columns
 * 2. One child has multiple pages (large data)
 * 3. Another child has fewer pages (small/compressible data)
 * 4. rowEnd is used to limit the number of rows read
 *
 * The root cause is in column.js - for non-flat columns, all pages
 * are read, but truncation only affects the last chunk. If a column
 * has multiple chunks (pages), earlier chunks aren't truncated,
 * resulting in mismatched array lengths during struct assembly.
 *
 * Test file: rowend_struct.parquet (created with pyarrow)
 * - 1050 rows
 * - struct column 's' with children:
 *   - 'a': unique strings (2 data pages due to snappy compression)
 *   - 'b': same string "x" (1 data page)
 */
describe('rowEnd with struct columns', () => {
  it('reads all rows without error', async () => {
    const file = await asyncBufferFromFile('test/files/rowend_struct.parquet')
    const rows = await parquetReadObjects({ file })
    expect(rows.length).toBe(1050)
    expect(rows[0]).toEqual({ s: { a: 'v0000', b: 'x' } })
    expect(rows[1049]).toEqual({ s: { a: 'v1049', b: 'x' } })
  })

  it('reads partial rows with rowEnd', async () => {
    const file = await asyncBufferFromFile('test/files/rowend_struct.parquet')
    // This should return 10 rows but currently throws
    // "parquet struct parsing error" due to mismatched child array lengths
    const rows = await parquetReadObjects({ file, rowEnd: 10 })
    expect(rows.length).toBe(10)
    expect(rows[0]).toEqual({ s: { a: 'v0000', b: 'x' } })
    expect(rows[9]).toEqual({ s: { a: 'v0009', b: 'x' } })
  })

  it('reads middle rows with rowStart and rowEnd', async () => {
    const file = await asyncBufferFromFile('test/files/rowend_struct.parquet')
    const rows = await parquetReadObjects({ file, rowStart: 100, rowEnd: 110 })
    expect(rows.length).toBe(10)
    expect(rows[0]).toEqual({ s: { a: 'v0100', b: 'x' } })
    expect(rows[9]).toEqual({ s: { a: 'v0109', b: 'x' } })
  })
})
