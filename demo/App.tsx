import HighTable, { DataFrame, sortableDataFrame } from 'hightable'
import { compressors } from 'hyparquet-compressors'
import React, { useState } from 'react'
import { parquetReadObjects } from '../src/hyparquet.js'
import { FileMetaData, parquetMetadataAsync, parquetSchema } from '../src/metadata.js'
import type { AsyncBuffer } from '../src/types.js'
import { asyncBufferFromUrl } from '../src/utils.js'
import Dropdown from './Dropdown.js'
import Dropzone from './Dropzone.js'
import Layout from './Layout.js'
import ParquetLayout from './ParquetLayout.js'
import ParquetMetadata from './ParquetMetadata.js'

type Lens = 'table' | 'metadata' | 'layout'

/**
 * Hyparquet demo viewer page
 * @returns {ReactNode}
 */
export default function App() {
  const [progress, setProgress] = useState<number>()
  const [error, setError] = useState<Error>()
  const [df, setDf] = useState<DataFrame>()
  const [name, setName] = useState<string>()
  const [lens, setLens] = useState<Lens>('table')
  const [metadata, setMetadata] = useState<FileMetaData>()
  const [byteLength, setByteLength] = useState<number>()

  async function onFileDrop(file: File) {
    setAsyncBuffer(file.name, await file.arrayBuffer())
  }
  async function onUrlDrop(url: string) {
    setAsyncBuffer(url, await asyncBufferFromUrl(url))
  }
  async function setAsyncBuffer(name: string, asyncBuffer: AsyncBuffer) {
    const metadata = await parquetMetadataAsync(asyncBuffer)
    setMetadata(metadata)
    setName(name)
    setByteLength(asyncBuffer.byteLength)
    let df = parquetDataFrame(asyncBuffer, metadata)
    if (df.numRows <= 10000) {
      df = sortableDataFrame(df)
    }
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
        {lens === 'table' && <HighTable data={df} />}
        {lens === 'metadata' && <ParquetMetadata metadata={metadata} />}
        {lens === 'layout' && <ParquetLayout byteLength={byteLength!} metadata={metadata} />}
      </>}
    </Dropzone>
  </Layout>
}

/**
 * Convert a parquet file into a dataframe.
 *
 * @param {AsyncBuffer} file - parquet file asyncbuffer
 * @param {FileMetaData} metadata - parquet file metadata
 * @returns {DataFrame} dataframe
 */
function parquetDataFrame(file: AsyncBuffer, metadata: FileMetaData): DataFrame {
  const { children } = parquetSchema(metadata)
  return {
    header: children.map(child => child.element.name),
    numRows: Number(metadata.num_rows),
    /**
     * @param {number} rowStart
     * @param {number} rowEnd
     * @returns {Promise<any[][]>}
     */
    rows(rowStart, rowEnd) {
      console.log(`reading rows ${rowStart}-${rowEnd}`)
      return parquetReadObjects({ file, compressors, rowStart, rowEnd })
    },
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
