import React, { useEffect, useState } from 'react'
import { Modal } from '../../components/ui/Modal'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'

interface ExecutionModeModalProps {
  open: boolean
  onClose: () => void
  onStart: (options: { executionMode: 'thread' | 'process' }) => void
  componentName: string
  description: string
}

export const ExecutionModeModal: React.FC<Readonly<ExecutionModeModalProps>> = ({
  open,
  onClose,
  onStart,
  componentName,
  description,
}) => {
  const readOnly = useIsReadOnly()
  const [executionMode, setExecutionMode] = useState<'thread' | 'process'>('thread')

  useEffect(() => {
    if (open) {
      setExecutionMode('thread')
    }
  }, [open])

  const handleStart = () => {
    onStart({ executionMode })
    onClose()
  }

  return (
    <Modal open={open} onClose={onClose} title={`Start ${componentName}`} size='md'>
      <div className='space-y-6'>
        <p className='text-muted-600'>{description}</p>
        <div className='space-y-4'>
          <h4 className='text-sm font-medium text-muted-700'>Execution Mode:</h4>
          <div className='space-y-3'>
            <label
              htmlFor='exec-mode-thread'
              aria-label='Thread Mode'
              className='flex items-start cursor-pointer p-3 rounded border border-dark-600 hover:border-muted-400'
            >
              <input
                id='exec-mode-thread'
                type='radio'
                value='thread'
                checked={executionMode === 'thread'}
                onChange={(event: React.ChangeEvent<HTMLInputElement>) =>
                  setExecutionMode(event.target.value as 'thread' | 'process')
                }
                className='mr-3 mt-1'
              />
              <div>
                <div className='text-alpine-900 font-medium'>Thread Mode</div>
                <div className='text-sm text-muted-500'>
                  Runs as embedded task within web server. Faster startup, shared memory.
                </div>
              </div>
            </label>
            <label
              htmlFor='exec-mode-process'
              aria-label='Process Mode'
              className='flex items-start cursor-pointer p-3 rounded border border-dark-600 hover:border-muted-400'
            >
              <input
                id='exec-mode-process'
                type='radio'
                value='process'
                checked={executionMode === 'process'}
                onChange={(event: React.ChangeEvent<HTMLInputElement>) =>
                  setExecutionMode(event.target.value as 'thread' | 'process')
                }
                className='mr-3 mt-1'
              />
              <div>
                <div className='text-alpine-900 font-medium'>Process Mode</div>
                <div className='text-sm text-muted-500'>
                  Runs as separate Python process. Isolated, fault-tolerant, detailed monitoring.
                </div>
              </div>
            </label>
          </div>
        </div>
        <div className='flex justify-end space-x-3 pt-4'>
          <button
            onClick={onClose}
            className='px-4 py-2 text-muted-600 hover:text-alpine-900 transition-colors'
          >
            Cancel
          </button>
          <button
            onClick={handleStart}
            disabled={readOnly}
            className='px-4 py-2 bg-primary-600 text-white rounded hover:bg-primary-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed'
          >
            Start {componentName}
          </button>
        </div>
      </div>
    </Modal>
  )
}
