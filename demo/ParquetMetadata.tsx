import React from 'react'
import type { FileMetaData } from '../src/metadata.js'
import { toJson } from '../src/utils.js'

interface MetadataProps {
  metadata: FileMetaData
}

export default function ParquetMetadata({ metadata }: MetadataProps) {
  return <code className='viewer'>
    {JSON.stringify(toJson(metadata), null, ' ')}
  </code>
}
