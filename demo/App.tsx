import HighTable, { DataFrame, rowCache } from 'hightable'
import React, { useEffect, useState } from 'react'
import { FileMetaData, parquetMetadataAsync, parquetSchema } from '../src/metadata.js'
import { byteLengthFromUrl } from '../src/utils.js'
import Dropdown from './Dropdown.js'
import Dropzone from './Dropzone.js'
import Layout from './Layout.js'
import ParquetLayout from './ParquetLayout.js'
import ParquetMetadata from './ParquetMetadata.js'
import { AsyncBufferFrom, asyncBufferFrom, parquetQueryWorker } from './workers/parquetWorkerClient.js'

type Lens = 'table' | 'metadata' | 'layout'

/**
 * Hyparquet demo viewer page
 * @param {Object} props
 * @param {string} [props.url]
 * @returns {ReactNode}
 */
export default function App({ url }: { url?: string }) {
  const [progress, setProgress] = useState<number>()
  const [error, setError] = useState<Error>()
  const [df, setDf] = useState<DataFrame>()
  const [name, setName] = useState<string>()
  const [lens, setLens] = useState<Lens>('table')
  const [metadata, setMetadata] = useState<FileMetaData>()
  const [byteLength, setByteLength] = useState<number>()

  useEffect(() => {
    if (!df && url) {
      onUrlDrop(url)
    }
  }, [ url ])

  async function onFileDrop(file: File) {
    // Clear query string
    history.pushState({}, '', location.pathname)
    setAsyncBuffer(file.name, { file, byteLength: file.size })
  }
  async function onUrlDrop(url: string) {
    // Add key=url to query string
    const params = new URLSearchParams(location.search)
    params.set('key', url)
    history.pushState({}, '', `${location.pathname}?${params}`)
    try {
      const byteLength = await byteLengthFromUrl(url)
      setAsyncBuffer(url, { url, byteLength })
    } catch (e) {
      setError(e as Error)
    }
  }
  async function setAsyncBuffer(name: string, from: AsyncBufferFrom) {
    // TODO: Replace welcome with spinner
    const asyncBuffer = await asyncBufferFrom(from)
    const metadata = await parquetMetadataAsync(asyncBuffer)
    setMetadata(metadata)
    setName(name)
    setByteLength(from.byteLength)
    let df = parquetDataFrame(from, metadata)
    df = rowCache(df)
    setDf(df)
    document.getElementById('welcome')?.remove()
  }

  return <Layout progress={progress} error={error}>
    <Dropzone
      onError={(e) => setError(e)}
      onFileDrop={onFileDrop}
      onUrlDrop={onUrlDrop}>
      {metadata && df && <>
        <div className='top-header'>{name}</div>
        <div className='view-header'>
          {byteLength !== undefined && <span title={byteLength.toLocaleString() + ' bytes'}>{formatFileSize(byteLength)}</span>}
          <span>{df.numRows.toLocaleString()} rows</span>
          <Dropdown label={lens}>
            <button onClick={() => setLens('table')}>Table</button>
            <button onClick={() => setLens('metadata')}>Metadata</button>
            <button onClick={() => setLens('layout')}>Layout</button>
          </Dropdown>
        </div>
        {lens === 'table' && <HighTable data={df} onError={setError} />}
        {lens === 'metadata' && <ParquetMetadata metadata={metadata} />}
        {lens === 'layout' && <ParquetLayout byteLength={byteLength!} metadata={metadata} />}
      </>}
    </Dropzone>
  </Layout>
}

/**
 * Convert a parquet file into a dataframe.
 */
function parquetDataFrame(from: AsyncBufferFrom, metadata: FileMetaData): DataFrame {
  const { children } = parquetSchema(metadata)
  return {
    header: children.map(child => child.element.name),
    numRows: Number(metadata.num_rows),
    /**
     * @param {number} rowStart
     * @param {number} rowEnd
     * @param {string} orderBy
     * @returns {Promise<any[][]>}
     */
    rows(rowStart, rowEnd, orderBy) {
      console.log(`reading rows ${rowStart}-${rowEnd}`, orderBy)
      return parquetQueryWorker({ asyncBuffer: from, metadata, rowStart, rowEnd, orderBy })
    },
    sortable: true,
  }
}

/**
 * Returns the file size in human readable format.
 *
 * @param {number} bytes file size in bytes
 * @returns {string} formatted file size string
 */
function formatFileSize(bytes: number): string {
  const sizes = ['b', 'kb', 'mb', 'gb', 'tb']
  if (bytes === 0) return '0 b'
  const i = Math.floor(Math.log2(bytes) / 10)
  if (i === 0) return bytes + ' b'
  const base = bytes / Math.pow(1024, i)
  return (base < 10 ? base.toFixed(1) : Math.round(base)) + ' ' + sizes[i]
}
