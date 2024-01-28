import { parquetMetadata, toJson } from './src/hyparquet.js'

const dropzone = document.getElementById('dropzone')
const layout = document.getElementById('layout')

dropzone.addEventListener('dragover', e => {
  e.preventDefault()
  e.dataTransfer.dropEffect = 'copy'
  dropzone.classList.add('over')
})

dropzone.addEventListener('dragleave', () => {
  dropzone.classList.remove('over')
})

function group(name) {
  return `<div>${name}`
}
function cell(name, start, bytes, end) {
  return `
    <div class="cell">
      <label>${name}</label>
      <ul>
        <li>start ${start}</li>
        <li>bytes ${bytes}</li>
        <li>end ${end}</li>
      </ul>
    </div>`
}

dropzone.addEventListener('drop', e => {
  e.preventDefault() // prevent dropped file from being "downloaded"
  dropzone.classList.remove('over')

  const { files } = e.dataTransfer
  if (files.length > 0) {
    const file = files[0]
    const reader = new FileReader()
    reader.onload = e => {
      try {
        const arrayBuffer = e.target.result
        const metadata = toJson(parquetMetadata(arrayBuffer))

        console.log('metadata', metadata)

        // render file layout
        let html = '<h2>File layout</h2>'
        html += cell('PAR1', 0, 4, 4) // magic number
        for (const rowGroupIndex in metadata.row_groups) {
          const rowGroup = metadata.row_groups[rowGroupIndex]
          html += group(`Row group ${rowGroupIndex} (${rowGroup.total_byte_size} bytes)`)
          for (const column of rowGroup.columns) {
            const columnName = column.meta_data.path_in_schema.join('.')

            let columnOffset = column.meta_data.dictionary_page_offset
            if (!columnOffset || column.meta_data.data_page_offset < columnOffset) {
              columnOffset = column.meta_data.data_page_offset
            }
            columnOffset = Number(columnOffset)
            const bytes = column.meta_data.total_compressed_size
            const end = columnOffset + bytes
            html += cell(`Column ${columnName}`, columnOffset, bytes, end)
          }
          html += '</div>'
        }
        const metadataStart = arrayBuffer.byteLength - metadata.metadata_length - 4
        html += cell('Metadata', metadataStart, metadata.metadata_length, arrayBuffer.byteLength - 4)
        html += cell('PAR1', arrayBuffer.byteLength - 4, 4, arrayBuffer.byteLength) // magic number
        layout.innerHTML = html

        // display metadata
        dropzone.innerHTML = `<strong>${file.name}</strong>`
        dropzone.innerHTML += `<pre>${JSON.stringify(metadata, null, 2)}</pre>`
      } catch (e) {
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
})
