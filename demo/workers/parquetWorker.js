import { compressors } from 'hyparquet-compressors'
import { parquetQuery } from '../../src/query.js'
import { asyncBufferFrom } from './parquetWorkerClient.js'

self.onmessage = async ({ data }) => {
  const { metadata, from, rowStart, rowEnd, orderBy, columns, queryId, chunks } = data
  const file = await asyncBufferFrom(from)
  /**
   * @typedef {import('../../src/hyparquet.js').ColumnData} ColumnData
   * @type {((chunk: ColumnData) => void) | undefined}
   */
  const onChunk = chunks ? chunk => self.postMessage({ chunk, queryId }) : undefined
  try {
    const result = await parquetQuery({
      metadata, file, rowStart, rowEnd, orderBy, columns, compressors, onChunk,
    })
    self.postMessage({ result, queryId })
  } catch (error) {
    self.postMessage({ error, queryId })
  }
}
