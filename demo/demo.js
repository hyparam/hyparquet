import {
  parquetMetadata, parquetMetadataAsync, parquetRead, parquetSchema, toJson,
} from '../src/hyparquet.js'
import { asyncBufferFromUrl } from '../src/utils.js'
import { compressors } from './hyparquet-compressors.min.js'
import { fileLayout, fileMetadata } from './layout.js'

/**
 * @typedef {import('../src/types.js').AsyncBuffer} AsyncBuffer
 * @typedef {import('../src/types.js').FileMetaData} FileMetaData
 */

/* eslint-disable no-extra-parens */
const dropzone = /** @type {HTMLElement} */ (document.getElementById('dropzone'))
const fileInput = /** @type {HTMLInputElement} */ (document.getElementById('#file-input'))
const content = document.querySelectorAll('#content')[0]
const welcome = document.querySelectorAll('#welcome')[0]

let enterCount = 0

dropzone.addEventListener('dragenter', e => {
  if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy'
  dropzone.classList.add('over')
  enterCount++
})

dropzone.addEventListener('dragover', e => {
  e.preventDefault()
})

dropzone.addEventListener('dragleave', () => {
  enterCount--
  if (!enterCount) dropzone.classList.remove('over')
})

dropzone.addEventListener('drop', e => {
  e.preventDefault() // prevent dropped file from being "downloaded"
  dropzone.classList.remove('over')

  if (!e.dataTransfer) throw new Error('Missing dataTransfer')
  const { files, items } = e.dataTransfer
  if (files.length > 0) {
    const file = files[0]
    processFile(file)
  }
  if (items.length > 0) {
    const item = items[0]
    if (item.kind === 'string') {
      item.getAsString(str => {
        if (str.startsWith('http')) {
          processUrl(str)
        }
      })
    }
  }
})

/**
 * @param {string} url
 */
async function processUrl(url) {
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
function processFile(file) {
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
 * @param {AsyncBuffer} asyncBuffer
 * @param {FileMetaData} metadata
 * @param {string} name
 */
async function render(asyncBuffer, metadata, name) {
  renderSidebar(asyncBuffer, metadata, name)

  const { children } = parquetSchema(metadata)
  const header = children.map(child => child.element.name)

  const startTime = performance.now()
  await parquetRead({
    compressors,
    file: asyncBuffer,
    rowEnd: 1000,
    onComplete(/** @type {any[][]} */ data) {
      const ms = performance.now() - startTime
      console.log(`parsed ${name} in ${ms.toFixed(0)} ms`)
      content.appendChild(renderTable(header, data))
    },
  })
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

welcome.addEventListener('click', () => {
  fileInput?.click()
})

fileInput?.addEventListener('change', () => {
  if (fileInput.files?.length) {
    processFile(fileInput.files[0])
  }
})

/**
 * @param {string[]} header
 * @param {any[][] | Record<string, any>[]} data
 * @returns {HTMLTableElement}
 */
function renderTable(header, data) {
  const table = document.createElement('table')
  const thead = document.createElement('thead')
  const tbody = document.createElement('tbody')
  const headerRow = document.createElement('tr')
  headerRow.appendChild(document.createElement('th'))
  for (const columnName of header) {
    const th = document.createElement('th')
    th.innerText = columnName
    headerRow.appendChild(th)
  }
  thead.appendChild(headerRow)
  table.appendChild(thead)
  for (const row of data) {
    const tr = document.createElement('tr')
    const rowNumber = document.createElement('td')
    rowNumber.innerText = String(tbody.children.length + 1)
    tr.appendChild(rowNumber)
    for (const value of Object.values(row)) {
      const td = document.createElement('td')
      td.innerText = stringify(value)
      tr.appendChild(td)
    }
    tbody.appendChild(tr)
  }
  table.appendChild(tbody)
  return table
}

/**
 * @param {any} value
 * @param {number} depth
 * @returns {string}
 */
function stringify(value, depth = 0) {
  if (value === null) return depth ? 'null' : ''
  if (value === undefined) return depth ? 'undefined' : ''
  if (typeof value === 'bigint') return value.toString()
  if (typeof value === 'string') return value
  if (Array.isArray(value)) return `[${value.map(v => stringify(v, depth + 1)).join(', ')}]`
  if (value instanceof Date) return value.toISOString()
  if (typeof value === 'object') return `{${Object.entries(value).map(([k, v]) => `${k}: ${stringify(v, depth + 1)}`).join(', ')}}`
  return value
}
