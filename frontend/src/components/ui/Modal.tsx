import React, { useEffect, useId, useRef } from 'react'
import { createPortal } from 'react-dom'
import clsx from 'clsx'

interface ModalProps {
  open: boolean
  onClose: () => void
  title?: string
  children: React.ReactNode
  size?: 'sm' | 'md' | 'lg' | 'xl'
}

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  'textarea:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

const getFocusableElements = (root: HTMLElement): HTMLElement[] =>
  Array.from(root.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR)).filter(
    el => !el.hasAttribute('aria-hidden')
  )

export const Modal: React.FC<Readonly<ModalProps>> = ({
  open,
  onClose,
  title,
  children,
  size = 'md',
}) => {
  const titleId = useId()
  const dialogRef = useRef<HTMLDialogElement>(null)
  const contentRef = useRef<HTMLDivElement>(null)
  const previousActiveElement = useRef<HTMLElement | null>(null)

  useEffect(() => {
    const dialog = dialogRef.current

    if (dialog === null || !open) {
      return
    }

    previousActiveElement.current = document.activeElement as HTMLElement | null
    dialog.showModal()
    document.body.style.overflow = 'hidden'
    const focusable = getFocusableElements(contentRef.current as HTMLElement)

    if (focusable.length > 0) {
      focusable[0].focus()
    }

    return () => {
      document.body.style.overflow = 'unset'
      previousActiveElement.current?.focus()
      previousActiveElement.current = null
    }
  }, [open])

  useEffect(() => {
    if (!open) {
      return
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose()

        return
      }

      if (event.key !== 'Tab') {
        return
      }

      const focusable = getFocusableElements(contentRef.current as HTMLElement)

      if (focusable.length === 0) {
        event.preventDefault()

        return
      }

      const first = focusable[0]
      const last = focusable.at(-1) as HTMLElement
      const active = document.activeElement

      if (event.shiftKey && active === first) {
        event.preventDefault()
        last.focus()
      } else if (!event.shiftKey && active === last) {
        event.preventDefault()
        first.focus()
      }
    }

    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [open, onClose])

  if (!open) return null
  const sizeClasses = {
    sm: 'max-w-md',
    md: 'max-w-lg',
    lg: 'max-w-2xl',
    xl: 'max-w-4xl',
  }
  const modalContent = (
    <dialog
      ref={dialogRef}
      className='fixed inset-0 z-50 m-0 h-full max-h-none w-full max-w-none overflow-y-auto bg-transparent p-0 backdrop:bg-muted-900/40'
      aria-labelledby={title ? titleId : undefined}
    >
      <button
        type='button'
        aria-label='Close modal'
        onClick={onClose}
        className='fixed inset-0 w-full h-full cursor-default border-none bg-transparent'
      />
      <div className='relative flex min-h-full items-center justify-center p-4'>
        <div
          ref={contentRef}
          className={clsx(
            'relative w-full rounded-2xl border border-dark-600 bg-alpine-50 shadow-xl',
            sizeClasses[size]
          )}
        >
          {title && (
            <div className='flex items-center justify-between border-b border-dark-600 p-6'>
              <h3 id={titleId} className='text-lg font-semibold text-alpine-900'>
                {title}
              </h3>
              <button
                onClick={onClose}
                className='text-muted-500 transition-colors hover:text-alpine-900'
                aria-label='Close'
              >
                <svg className='w-6 h-6' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                  <path
                    strokeLinecap='round'
                    strokeLinejoin='round'
                    strokeWidth={2}
                    d='M6 18L18 6M6 6l12 12'
                  />
                </svg>
              </button>
            </div>
          )}
          <div className='p-6'>{children}</div>
        </div>
      </div>
    </dialog>
  )

  return createPortal(modalContent, document.body)
}
