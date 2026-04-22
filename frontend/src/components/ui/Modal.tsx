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

export const Modal: React.FC<Readonly<ModalProps>> = ({
  open,
  onClose,
  title,
  children,
  size = 'md',
}) => {
  const titleId = useId()
  const dialogRef = useRef<HTMLDialogElement>(null)

  useEffect(() => {
    const dialog = dialogRef.current

    if (dialog === null || !open) {
      return
    }

    dialog.showModal()
    document.body.style.overflow = 'hidden'

    return () => {
      document.body.style.overflow = 'unset'
    }
  }, [open])

  useEffect(() => {
    if (!open) {
      return
    }

    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose()
      }
    }

    document.addEventListener('keydown', handleEsc)

    return () => {
      document.removeEventListener('keydown', handleEsc)
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
