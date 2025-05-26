import { parquetMetadataAsync } from './metadata.js'
import { parquetPlan, prefetchAsyncBuffer } from './plan.js'
import { readRowGroup } from './rowgroup.js'
import { concat } from './utils.js'

/**
 * @import {ParquetReadOptions} from '../src/types.d.ts'
 */
/**
 * Read parquet data rows from a file-like object.
 * Reads the minimal number of row groups and columns to satisfy the request.
 *
 * Returns a void promise when complete.
 * Errors are thrown on the returned promise.
 * Data is returned in callbacks onComplete, onChunk, onPage, NOT the return promise.
 * See parquetReadObjects for a more convenient API.
 *
 * @param {ParquetReadOptions} options read options
 * @returns {Promise<void>} resolves when all requested rows and columns are parsed, all errors are thrown here
 */
export async function parquetRead(options) {
  // load metadata if not provided
  options.metadata ??= await parquetMetadataAsync(options.file)
  const { metadata, onComplete, rowStart = 0, rowEnd } = options
  if (rowStart < 0) throw new Error('parquetRead rowStart must be positive')

  // prefetch byte ranges
  const plan = parquetPlan(options)
  options.file = prefetchAsyncBuffer(options.file, plan)

  /** @type {any[][]} */
  const rowData = []

  // read row groups
  let groupStart = 0 // first row index of the current group
  for (const rowGroup of metadata.row_groups) {
    // number of rows in this row group
    const groupRows = Number(rowGroup.num_rows)
    // if row group overlaps with row range, read it
    if (groupStart + groupRows >= rowStart && (rowEnd === undefined || groupStart < rowEnd)) {
      // read row group
      const groupData = await readRowGroup(options, rowGroup, groupStart)
      if (onComplete) {
        // filter to rows in range
        const start = Math.max(rowStart - groupStart, 0)
        const end = rowEnd === undefined ? undefined : rowEnd - groupStart
        concat(rowData, groupData.slice(start, end))
      }
    }
    groupStart += groupRows
  }

  if (onComplete) onComplete(rowData)
}
