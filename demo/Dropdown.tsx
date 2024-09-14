import React from 'react'
import { ReactNode, useEffect, useRef, useState } from 'react'
import { cn } from './Layout.js'

interface DropdownProps {
  label?: string
  className?: string
  children: ReactNode
}

/**
 * Dropdown menu component.
 *
 * @param {Object} props
 * @param {string} props.label - button label
 * @param {string} props.className - custom class name for the dropdown container
 * @param {ReactNode} props.children - dropdown menu items
 * @returns {ReactNode}
 * @example
 * <Dropdown label='Menu'>
 *  <button>Item 1</button>
 *  <button>Item 2</button>
 * </Dropdown>
 */
export default function Dropdown({ label, className, children }: DropdownProps) {
  const [isOpen, setIsOpen] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)

  function toggleDropdown() {
    setIsOpen(!isOpen)
  }

  useEffect(() => {
    function handleClickInside(event: MouseEvent) {
      const target = event.target as Element
      if (menuRef.current && menuRef.current.contains(target) && target?.tagName !== 'INPUT') {
        setIsOpen(false)
      }
    }
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }
    function handleEscape(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setIsOpen(false)
      }
    }
    document.addEventListener('click', handleClickInside)
    document.addEventListener('keydown', handleEscape)
    document.addEventListener('mousedown', handleClickOutside)
    return () => {
      document.removeEventListener('click', handleClickInside)
      document.removeEventListener('keydown', handleEscape)
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [])

  return (
    <div
      className={cn('dropdown', className, isOpen && 'open')}
      ref={dropdownRef}>
      <button className='dropdown-button' onClick={toggleDropdown}>
        {label}
      </button>
      <div className='dropdown-content' ref={menuRef}>
        {children}
      </div>
    </div>
  )
}
