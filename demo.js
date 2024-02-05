import { parquetMetadata, parquetMetadataAsync, toJson } from './src/hyparquet.js'

const dropzone = document.getElementById('dropzone')
const layout = document.getElementById('layout')
const metadataDiv = document.getElementById('metadata')
const fileInput = document.getElementById('file-input')

dropzone.addEventListener('dragover', e => {
  e.preventDefault()
  e.dataTransfer.dropEffect = 'copy'
  dropzone.classList.add('over')
})

dropzone.addEventListener('dragleave', () => {
  dropzone.classList.remove('over')
})

dropzone.addEventListener('drop', e => {
  e.preventDefault() // prevent dropped file from being "downloaded"
  dropzone.classList.remove('over')

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

async function processUrl(url) {
  try {
    // Check if file is accessible and get its size
    const head = await fetch(url, { method: 'HEAD' })
    if (!head.ok) {
      dropzone.innerHTML = `<strong>${url}</strong>`
      dropzone.innerHTML += `<div class="error">Error fetching file\n${head.status} ${head.statusText}</div>`
      return
    }
    const size = head.headers.get('content-length')
    if (!size) {
      dropzone.innerHTML = `<strong>${url}</strong>`
      dropzone.innerHTML += '<div class="error">Error fetching file\nNo content-length header</div>'
      return
    }
    // Construct an AsyncBuffer that fetches file chunks
    const asyncBuffer = {
      byteLength: Number(size),
      slice: async (start, end) => {
        const rangeEnd = end === undefined ? '' : end - 1
        const res = await fetch(url, {
          headers: { Range: `bytes=${start}-${rangeEnd}` },
        })
        return res.arrayBuffer()
      },
    }
    const metadata = await parquetMetadataAsync(asyncBuffer)
    url = `<a href="${url}">${url}</a>`
    renderSidebar(asyncBuffer, metadata, url)
  } catch (e) {
    console.error('Error fetching file', e)
    dropzone.innerHTML = `<strong>${url}</strong>`
    dropzone.innerHTML += `<div class="error">Error fetching file\n${e}</div>`
  }
}

function processFile(file) {
  const reader = new FileReader()
  reader.onload = e => {
    try {
      const arrayBuffer = e.target.result
      const metadata = parquetMetadata(arrayBuffer)
      renderSidebar(arrayBuffer, metadata, file.name)
    } catch (e) {
      console.error('Error parsing file', e)
      dropzone.innerHTML = `<strong>${file.name}</strong>`
      dropzone.innerHTML += `<div class="error">Error parsing file\n${e}</div>`
    }
  }
  reader.onerror = e => {
    console.error('Error reading file', e)
    dropzone.innerText = `Error reading file\n${e.target.error}`
  }
  reader.readAsArrayBuffer(file)
}

function renderSidebar(asyncBuffer, metadata, name) {
  layout.innerHTML = `<strong>${name}</strong>`
  // render file layout
  layout.appendChild(fileLayout(metadata, asyncBuffer.byteLength))
  // display metadata
  metadataDiv.innerHTML = ''
  metadataDiv.appendChild(fileMetadata(toJson(metadata)))
}

dropzone.addEventListener('click', () => {
  fileInput.click()
})

fileInput.addEventListener('change', () => {
  if (fileInput.files.length > 0) {
    processFile(fileInput.files[0])
  }
})

// Render file layout
function fileLayout(metadata, byteLength) {
  let html = '<h2>File layout</h2>'
  html += cell('PAR1', 0, 4, 4) // magic number
  for (const rowGroupIndex in metadata.row_groups) {
    const rowGroup = metadata.row_groups[rowGroupIndex]
    html += group(`Row group ${rowGroupIndex} (${rowGroup.total_byte_size.toLocaleString()} bytes)`)
    for (const column of rowGroup.columns) {
      const columnName = column.meta_data.path_in_schema.join('.')

      let columnOffset = column.meta_data.dictionary_page_offset
      if (!columnOffset || column.meta_data.data_page_offset < columnOffset) {
        columnOffset = column.meta_data.data_page_offset
      }
      columnOffset = Number(columnOffset)
      const bytes = Number(column.meta_data.total_compressed_size)
      const end = columnOffset + bytes
      html += cell(`Column ${columnName}`, columnOffset, bytes, end)
    }
    html += '</div>'
  }
  const metadataStart = byteLength - metadata.metadata_length - 4
  html += cell('Metadata', metadataStart, metadata.metadata_length, byteLength - 4)
  html += cell('PAR1', byteLength - 4, 4, byteLength) // magic number
  const div = document.createElement('div')
  div.innerHTML = html
  div.classList.add('collapsed') // start collapsed
  div.children[0].addEventListener('click', () => {
    div.classList.toggle('collapsed')
  })
  return div
}
function group(name) {
  return `<div>${name}`
}
function cell(name, start, bytes, end) {
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

// Render metadata
function fileMetadata(metadata) {
  let html = '<h2>Metadata</h2>'
  html += `<pre>${JSON.stringify(metadata, null, 2)}</pre>`
  const div = document.createElement('div')
  div.innerHTML = html
  div.classList.add('collapsed') // start collapsed
  div.children[0].addEventListener('click', () => {
    div.classList.toggle('collapsed')
  })
  return div
}
