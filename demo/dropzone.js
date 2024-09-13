/**
 * Initialize the dropzone for file and url drag-and-drop.
 *
 * @param {Function} handleFileDrop
 * @param {Function} handleUrlDrop
 */
export function initDropzone(handleFileDrop, handleUrlDrop) {
  let enterCount = 0

  const dropzone = /** @type {HTMLElement} */ (document.getElementById('dropzone'))
  const fileInput = /** @type {HTMLInputElement} */ (document.getElementById('file-input'))
  const welcome = document.querySelectorAll('#welcome')[0]

  // Click to select file
  welcome.addEventListener('click', () => {
    fileInput?.click()
  })
  fileInput?.addEventListener('change', () => {
    if (fileInput.files?.length) {
      handleFileDrop(fileInput.files[0])
    }
  })

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
      handleFileDrop(file)
    }
    if (items.length > 0) {
      const item = items[0]
      if (item.kind === 'string') {
        item.getAsString(str => {
          if (str.startsWith('http')) {
            handleUrlDrop(str)
          }
        })
      }
    }
  })
}
