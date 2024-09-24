import React from 'react'
import type { FileMetaData } from '../src/metadata.js'
import { toJson } from '../src/utils.js'

interface MetadataProps {
  metadata: FileMetaData
}

/**
 * Renders the metadata of a parquet file as JSON.
 * @param {Object} props
 * @param {FileMetaData} props.metadata
 * @returns {ReactNode}
 */
export default function ParquetMetadata({ metadata }: MetadataProps) {
  return <code className='viewer'>
    {JSON.stringify(toJson(metadata), null, ' ')}
  </code>
}
