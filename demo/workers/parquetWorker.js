import { compressors } from 'hyparquet-compressors'
import { parquetQuery } from '../../src/query.js'
import { asyncBufferFrom } from './parquetWorkerClient.js'

self.onmessage = async ({ data }) => {
  const { metadata, asyncBuffer, rowStart, rowEnd, orderBy } = data
  const file = await asyncBufferFrom(asyncBuffer)
  try {
    const result = await parquetQuery({
      metadata, file, rowStart, rowEnd, orderBy, compressors,
    })
    self.postMessage({ result })
  } catch (error) {
    self.postMessage({ error })
  }
}
