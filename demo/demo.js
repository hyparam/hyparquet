import HighTable from 'hightable'
import { compressors } from 'hyparquet-compressors'
import React from 'react'
import ReactDOM from 'react-dom'
import {
  parquetMetadata, parquetMetadataAsync, parquetRead, parquetSchema, toJson,
} from '../src/hyparquet.js'
import { asyncBufferFromUrl } from '../src/utils.js'
import { initDropzone } from './dropzone.js'
import { fileLayout, fileMetadata } from './layout.js'

/**
 * @typedef {import('../src/types.js').AsyncBuffer} AsyncBuffer
 * @typedef {import('../src/types.js').FileMetaData} FileMetaData
 */

const content = document.querySelectorAll('#content')[0]

// Initialize drag-and-drop
initDropzone(handleFileDrop, handleUrlDrop)

/**
 * @param {string} url
 */
async function handleUrlDrop(url) {
  content.innerHTML = ''
  try {
    const asyncBuffer = await asyncBufferFromUrl(url)
    const metadata = await parquetMetadataAsync(asyncBuffer)
    await render(asyncBuffer, metadata, `<a href="${url}">${url}</a>`)
  } catch (e) {
    console.error('Error fetching url', e)
    content.innerHTML += `<div class="error">Error fetching url ${url}\n${e}</div>`
  }
}

/**
 * @param {File} file
 */
function handleFileDrop(file) {
  content.innerHTML = ''
  const reader = new FileReader()
  reader.onload = async e => {
    try {
      const arrayBuffer = e.target?.result
      if (!(arrayBuffer instanceof ArrayBuffer)) throw new Error('Missing arrayBuffer')
      const metadata = parquetMetadata(arrayBuffer)
      await render(arrayBuffer, metadata, file.name)
    } catch (e) {
      console.error('Error parsing file', e)
      content.innerHTML = `<strong>${file.name}</strong>`
      content.innerHTML += `<div class="error">Error parsing file\n${e}</div>`
    }
  }
  reader.onerror = e => {
    console.error('Error reading file', e)
    content.innerHTML = `<strong>${file.name}</strong>`
    content.innerHTML += `<div class="error">Error reading file\n${e.target?.error}</div>`
  }
  reader.readAsArrayBuffer(file)
}

/**
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {string} name
 */
function render(file, metadata, name) {
  renderSidebar(file, metadata, name)

  const { children } = parquetSchema(metadata)

  const dataframe = {
    header: children.map(child => child.element.name),
    numRows: Number(metadata.num_rows),
    /**
     * @param {number} rowStart
     * @param {number} rowEnd
     * @returns {Promise<any[][]>}
     */
    rows(rowStart, rowEnd) {
      console.log(`reading rows ${rowStart}-${rowEnd}`)
      return new Promise((resolve, reject) => {
        parquetRead({ file, compressors, rowStart, rowEnd, onComplete: resolve })
          .catch(reject)
      })
    },
  }
  renderTable(dataframe)
}

/**
 * @param {AsyncBuffer} asyncBuffer
 * @param {FileMetaData} metadata
 * @param {string} name
 */
function renderSidebar(asyncBuffer, metadata, name) {
  const sidebar = /** @type {HTMLElement} */ (document.getElementById('sidebar'))
  sidebar.innerHTML = `<div id="filename">${name}</div>`
  sidebar.appendChild(fileMetadata(toJson(metadata)))
  sidebar.appendChild(fileLayout(metadata, asyncBuffer))
}

/**
 * @param {import('hightable').DataFrame} data
 */
function renderTable(data) {
  // Load HighTable.tsx and render
  const container = document.getElementById('content')
  // @ts-expect-error ReactDOM type issue
  const root = ReactDOM.createRoot(container)
  root.render(React.createElement(HighTable, { data }))
}
