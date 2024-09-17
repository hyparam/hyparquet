import React from 'react'
import { ReactNode, useEffect, useRef, useState } from 'react'

interface DropzoneProps {
  children: ReactNode
  onFileDrop: (file: File) => void
  onUrlDrop: (url: string) => void
  onError: (error: Error) => void
}

/**
 * A dropzone component for uploading files.
 *
 * Shows a fullscreen overlay when files are dragged over the dropzone.
 *
 * You can have an element inside the dropzone that triggers the file input
 * dialog when clicked by adding the class 'dropzone-select' to it.
 *
 * @param {Object} props
 * @param {ReactNode} props.children - message to display in dropzone.
 * @param {Function} props.onFileDrop - called when a file is dropped.
 * @param {Function} props.onUrlDrop - called when a url is dropped.
 * @param {Function} props.onError  - called when an error occurs.
 * @returns {ReactNode}
 */
export default function Dropzone({ children, onFileDrop, onUrlDrop, onError }: DropzoneProps) {
  const dropzoneRef = useRef<HTMLDivElement>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)
  // number of dragenter events minus dragleave events
  const [enters, setEnters] = useState(0)

  /**
   * Trigger file input dialog.
   * @param {MouseEvent} e - click
   */
  function triggerFileSelect(e: React.MouseEvent<HTMLDivElement>) {
    // If click inside '.dropzone', activate file input dialog
    if ((e.target as Element).classList.contains('dropzone')) {
      fileInputRef.current?.click()
    }
  }

  /**
   * Handle file selection event.
   * Recursively upload files and directories, in parallel.
   * @param {ChangeEvent} e
   * @returns {void}
   */
  function handleFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const { files } = e.target
    if (!files || files.length !== 1) return
    onFileDrop(files[0])
  }

  useEffect(() => {
    const dropzone = dropzoneRef.current
    if (!dropzone) return

    // Attach drag-and-drop event listeners
    function onDragEnter(e: DragEvent) {
      // check if any of the items are files (not strings)
      const items = e.dataTransfer?.items
      if (!items) return
      if (!Array.from(items).some(item => item.kind === 'file')) return
      setEnters(enters => enters + 1)
    }
    function onDragOver(e: DragEvent) {
      e.preventDefault()
    }
    function onDragLeave() {
      setEnters(enters => enters - 1)
    }
    function handleFileDrop(e: DragEvent) {
      e.preventDefault()
      setEnters(0)

      if (!e.dataTransfer) throw new Error('Missing dataTransfer')
      const { files, items } = e.dataTransfer
      if (files.length > 0) {
        const file = files[0]
        onFileDrop(file)
      }
      if (items.length > 0) {
        const item = items[0]
        if (item.kind === 'string') {
          item.getAsString(url => {
            if (url.startsWith('http')) {
              onUrlDrop(url)
            }
          })
        }
      }
    }

    window.addEventListener('dragenter', onDragEnter)
    window.addEventListener('dragover', onDragOver)
    window.addEventListener('dragleave', onDragLeave)
    dropzone.addEventListener('drop', handleFileDrop)

    // Cleanup event listeners when component is unmounted
    return () => {
      window.removeEventListener('dragenter', onDragEnter)
      window.removeEventListener('dragover', onDragOver)
      window.removeEventListener('dragleave', onDragLeave)
      dropzone.removeEventListener('drop', handleFileDrop)
    }
  })

  return (
    <div
      className={enters > 0 ? 'dropzone hover' : 'dropzone'}
      onClick={triggerFileSelect}
      ref={dropzoneRef}>
      {children}
      <div className='overlay'>
        <div className='target'>
          <div>Drop files to view. 👀</div>
        </div>
      </div>
      <input
        onChange={handleFileSelect}
        ref={fileInputRef}
        style={{ display: 'none' }}
        type="file" />
    </div>
  )
}
