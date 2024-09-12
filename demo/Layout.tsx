import React, { ReactNode, useEffect } from 'react'

interface LayoutProps {
  children: ReactNode
  className?: string
  progress?: number
  error?: Error
}

/**
 * Layout for shared UI.
 * Content div style can be overridden by className prop.
 *
 * @param {Object} props
 * @param {ReactNode} props.children - content to display inside the layout
 * @param {string | undefined} props.className - additional class names to apply to the content container
 * @param {number | undefined} props.progress - progress bar value
 * @param {Error} props.error - error message to display
 * @returns {ReactNode}
 */
export default function Layout({ children, className, progress, error }: LayoutProps) {
  const errorMessage = error?.toString()
  if (error) console.error(error)

  useEffect(() => {
    document.title = 'hyparquet demo - apache parquet file viewer online'
  }, [])

  return <>
    <div className='content-container'>
      <div className={cn('content', className)}>
        {children}
      </div>
      <div className={cn('error-bar', error && 'show-error')}>{errorMessage}</div>
    </div>
    {progress !== undefined && progress < 1 &&
      <div className={'progress-bar'} role='progressbar'>
        <div style={{ width: `${100 * progress}%` }} />
      </div>
    }
  </>
}

/**
 * Helper function to join class names.
 * Filters out falsy values and joins the rest.
 *
 * @param {...string | undefined | false} names - class name(s) to join
 * @returns {string}
 */
export function cn(...names: (string | undefined | false)[]): string {
  return names.filter(n => n).join(' ')
}

export function Spinner({ className }: { className: string }) {
  return <div className={cn('spinner', className)}></div>
}
