import React from 'react'
import { Modal } from './Modal'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'

interface ConfirmDialogProps {
  open: boolean
  title: string
  message: string
  confirmText?: string
  cancelText?: string
  onConfirm: () => void
  onCancel: () => void
  variant?: 'default' | 'danger'
}

export const ConfirmDialog: React.FC<Readonly<ConfirmDialogProps>> = ({
  open,
  title,
  message,
  confirmText = 'Confirm',
  cancelText = 'Cancel',
  onConfirm,
  onCancel,
  variant = 'default',
}) => {
  const readOnly = useIsReadOnly()
  const confirmButtonClasses =
    variant === 'danger'
      ? 'bg-loss-600 hover:bg-loss-700 focus:ring-loss-500'
      : 'bg-primary-600 hover:bg-primary-700 focus:ring-primary-500'

  return (
    <Modal open={open} onClose={onCancel} title={title} size='sm'>
      <div className='space-y-6'>
        {}
        <p className='text-muted-600 leading-relaxed'>{message}</p>
        {}
        <div className='flex space-x-3 pt-4 border-t border-dark-600'>
          <button
            onClick={onCancel}
            className='flex-1 px-4 py-2 text-sm font-medium text-alpine-900 bg-alpine-50 border border-dark-600 rounded-md hover:bg-dark-700 focus:outline-hidden focus:ring-2 focus:ring-offset-2 focus:ring-offset-alpine-50 focus:ring-primary-500 transition-colors'
          >
            {cancelText}
          </button>
          <button
            onClick={onConfirm}
            disabled={readOnly}
            className={`flex-1 px-4 py-2 text-sm font-medium text-white border border-transparent rounded-md focus:outline-hidden focus:ring-2 focus:ring-offset-2 focus:ring-offset-dark-800 transition-colors disabled:opacity-50 disabled:cursor-not-allowed ${confirmButtonClasses}`}
          >
            {confirmText}
          </button>
        </div>
      </div>
    </Modal>
  )
}
