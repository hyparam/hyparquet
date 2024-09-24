import React, { ReactNode } from 'react'
import { getColumnRange } from '../src/column.js'
import type { FileMetaData } from '../src/metadata.js'
import { ColumnChunk } from '../src/types.js'

interface LayoutProps {
  byteLength: number
  metadata: FileMetaData
}

/**
 * Renders the file layout of a parquet file as nested rowgroups and columns.
 * @param {Object} props
 * @param {number} props.byteLength
 * @param {FileMetaData} props.metadata
 * @returns {ReactNode}
 */
export default function ParquetLayout({ byteLength, metadata }: LayoutProps) {
  const metadataStart = byteLength - metadata.metadata_length - 4
  const metadataEnd = byteLength - 4

  return <div className='viewer'>
    <div className='layout'>
      <Cell name='PAR1' start={0n} end={4n} />
      <RowGroups metadata={metadata} />
      <ColumnIndexes metadata={metadata} />
      <Cell name='Metadata' start={metadataStart} end={metadataEnd} />
      <Cell name='PAR1' start={metadataEnd} end={byteLength} />
    </div>
  </div>
}


function Cell<N extends bigint | number>({ name, start, end }: { name: string, start: N, end: N }) {
  const bytes = end - start
  return <div className="cell">
    <label>{name}</label>
    <ul>
      <li>start {start.toLocaleString()}</li>
      <li>bytes {bytes.toLocaleString()}</li>
      <li>end {end.toLocaleString()}</li>
    </ul>
  </div>
}

function Group({ children, name, bytes }: { children: ReactNode, name?: string, bytes?: bigint }) {
  return <div className="group">
    <div className="group-header">
      <label>{name}</label>
      <span>{bytes === undefined ? '' : `bytes ${bytes.toLocaleString()}`}</span>
    </div>
    {children}
  </div>
}

function RowGroups({ metadata }: { metadata: FileMetaData }) {
  return <>
    {metadata.row_groups.map((rowGroup, i) => (
      <Group key={i} name={`RowGroup ${i}`} bytes={rowGroup.total_byte_size}>
        {rowGroup.columns.map((column, j) => (
          <Column key={j} column={column} />
        ))}
      </Group>
    ))}
  </>
}

function Column({ key, column }: { key: number, column: ColumnChunk }) {
  if (!column.meta_data) return null
  const end = getColumnRange(column.meta_data)[1]
  const pages = [
    { name: 'Dictionary', offset: column.meta_data.dictionary_page_offset },
    { name: 'Data', offset: column.meta_data.data_page_offset },
    { name: 'Index', offset: column.meta_data.index_page_offset },
    { name: 'End', offset: end },
  ]
    .filter(({ offset }) => offset !== undefined)
    .sort((a, b) => Number(a.offset) - Number(b.offset))

  const children = pages.slice(0, -1).map(({ name, offset }, index) => (
    <Cell key={name} name={name} start={offset!} end={pages[index + 1].offset!} />
  ))


  return <Group
    key={key}
    name={`Column ${column.meta_data?.path_in_schema.join('.')}`}
    bytes={column.meta_data?.total_compressed_size}>
    {children}
  </Group>
}

function ColumnIndexes({ metadata }: { metadata: FileMetaData }) {
  const indexPages = []
  for (const rowGroup of metadata.row_groups) {
    for (const column of rowGroup.columns) {
      const columnName = column.meta_data?.path_in_schema.join('.')
      if (column.column_index_offset) {
        indexPages.push({
          name: `ColumnIndex ${columnName}`,
          start: column.column_index_offset,
          end: column.column_index_offset + BigInt(column.column_index_length || 0),
        })
      }
      if (column.offset_index_offset) {
        indexPages.push({
          name: `OffsetIndex ${columnName}`,
          start: column.offset_index_offset,
          end: column.offset_index_offset + BigInt(column.offset_index_length || 0),
        })
      }
    }
  }

  return <Group name='ColumnIndexes'>
    {indexPages.map(({ name, start, end }, index) => (
      <Cell key={index} name={name} start={start} end={end} />
    ))}
  </Group>
}
