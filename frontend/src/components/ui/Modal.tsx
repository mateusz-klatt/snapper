import React, { useEffect } from 'react'
import { createPortal } from 'react-dom'
import clsx from 'clsx'

interface ModalProps {
  open: boolean
  onClose: () => void
  title?: string
  children: React.ReactNode
  size?: 'sm' | 'md' | 'lg' | 'xl'
}

export const Modal: React.FC<ModalProps> = ({ open, onClose, title, children, size = 'md' }) => {
  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose()
      }
    }

    if (open) {
      document.addEventListener('keydown', handleEsc)
      document.body.style.overflow = 'hidden'
    }

    return () => {
      document.removeEventListener('keydown', handleEsc)
      document.body.style.overflow = 'unset'
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
    <div className='fixed inset-0 z-50 overflow-y-auto'>
      {}
      <div className='fixed inset-0 bg-black bg-opacity-50 transition-opacity' onClick={onClose} />
      {}
      <div className='flex min-h-full items-center justify-center p-4'>
        <div
          className={clsx(
            'relative w-full bg-dark-800 rounded-lg shadow-xl border border-dark-700',
            sizeClasses[size]
          )}
          onClick={e => e.stopPropagation()}
        >
          {}
          {title && (
            <div className='flex items-center justify-between p-6 border-b border-dark-700'>
              <h3 className='text-lg font-semibold text-white'>{title}</h3>
              <button
                onClick={onClose}
                className='text-dark-400 hover:text-dark-200 transition-colors'
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
          {}
          <div className='p-6'>{children}</div>
        </div>
      </div>
    </div>
  )

  return createPortal(modalContent, document.body)
}
