/**
 * @typedef {import('../src/types.js').FileMetaData} FileMetaData
 */

import { getColumnRange } from '../src/column.js'

/**
 * @param {FileMetaData} metadata
 * @returns {HTMLDivElement}
 */
export function fileMetadata(metadata) {
  let html = '<h2>Metadata</h2>'
  html += `<pre>${JSON.stringify(metadata, null, 2)}</pre>`
  const div = document.createElement('div')
  div.innerHTML = html
  div.classList.add('layout', 'collapsed') // start collapsed
  div.children[0].addEventListener('click', () => {
    div.classList.toggle('collapsed')
  })
  return div
}

/**
 * Render parquet file layout.
 *
 * @param {FileMetaData} metadata
 * @param {number} byteLength
 * @returns {HTMLDivElement}
 */
export function fileLayout(metadata, byteLength) {
  let html = '<h2>File layout</h2>'
  html += cell('PAR1', 0n, 4n) // magic number
  /** @type {[string, bigint, bigint][]} */
  const indexPages = []
  for (const rowGroupIndex in metadata.row_groups) {
    const rowGroup = metadata.row_groups[rowGroupIndex]
    html += group(`RowGroup ${rowGroupIndex} (${rowGroup.total_byte_size.toLocaleString()} bytes)`)
    for (const column of rowGroup.columns) {
      const columnName = column.meta_data?.path_in_schema.join('.')
      html += group(`Column ${columnName}`)
      if (column.meta_data) {
        const end = getColumnRange(column.meta_data)[1]
        /* eslint-disable no-extra-parens */
        const pages = (/** @type {[string, bigint][]} */
          ([
            ['Dictionary', column.meta_data.dictionary_page_offset],
            ['Data', column.meta_data.data_page_offset],
            ['Index', column.meta_data.index_page_offset],
            ['End', end],
          ]))
          .filter(([, offset]) => offset !== undefined)
          .sort((a, b) => Number(a[1]) - Number(b[1]))

        for (let i = 0; i < pages.length - 1; i++) {
          const [name, start] = pages[i]
          const end = pages[i + 1][1]
          html += cell(name, start, end)
        }
      }
      if (column.column_index_offset) {
        indexPages.push([`ColumnIndex RowGroup${rowGroupIndex} ${columnName}`, column.column_index_offset, BigInt(column.column_index_length || 0)])
      }
      if (column.offset_index_offset) {
        indexPages.push([`OffsetIndex RowGroup${rowGroupIndex} ${columnName}`, column.offset_index_offset, BigInt(column.offset_index_length || 0)])
      }
      html += '</div>'
    }
    html += '</div>'
  }
  for (const [name, start, length] of indexPages) {
    html += cell(name, start, start + length)
  }
  const metadataStart = BigInt(byteLength - metadata.metadata_length - 4)
  const metadataEnd = BigInt(byteLength - 4)
  html += cell('Metadata', metadataStart, metadataEnd)
  html += cell('PAR1', metadataEnd, BigInt(byteLength)) // magic number
  const div = document.createElement('div')
  div.innerHTML = html
  div.classList.add('layout', 'collapsed') // start collapsed
  div.children[0].addEventListener('click', () => {
    div.classList.toggle('collapsed')
  })
  return div
}

/**
 * @param {string} name
 * @returns {string}
 */
function group(name) {
  return `<div>${name}`
}

/**
 * @param {string} name
 * @param {bigint} start
 * @param {bigint} end
 * @returns {string}
 */
function cell(name, start, end) {
  const bytes = end - start
  return `
    <div class="cell">
      <label>${name}</label>
      <ul>
        <li>start ${start.toLocaleString()}</li>
        <li>bytes ${bytes.toLocaleString()}</li>
        <li>end ${end.toLocaleString()}</li>
      </ul>
    </div>`
}
