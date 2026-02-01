import React, { useEffect, useState } from 'react'
import { Modal } from '../../components/ui/Modal'

interface ExecutionModeModalProps {
  open: boolean
  onClose: () => void
  onStart: (options: { executionMode: 'thread' | 'process'; autostart: boolean }) => void
  componentName: string
  description: string
  defaultAutostart: boolean
}

export const ExecutionModeModal: React.FC<Readonly<ExecutionModeModalProps>> = ({
  open,
  onClose,
  onStart,
  componentName,
  description,
  defaultAutostart,
}) => {
  const [executionMode, setExecutionMode] = useState<'thread' | 'process'>('thread')
  const [autostart, setAutostart] = useState<boolean>(defaultAutostart)

  useEffect(() => {
    if (open) {
      setExecutionMode('thread')
      setAutostart(defaultAutostart)
    }
  }, [open, defaultAutostart])

  const handleStart = () => {
    onStart({ executionMode, autostart })
    onClose()
  }

  return (
    <Modal open={open} onClose={onClose} title={`Start ${componentName}`} size='md'>
      <div className='space-y-6'>
        <p className='text-gray-300'>{description}</p>
        <div className='space-y-4'>
          <h4 className='text-sm font-medium text-gray-200'>Execution Mode:</h4>
          <div className='space-y-3'>
            <label
              htmlFor='exec-mode-thread'
              aria-label='Thread Mode'
              className='flex items-start cursor-pointer p-3 rounded border border-gray-600 hover:border-gray-500'
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
                <div className='text-white font-medium'>Thread Mode</div>
                <div className='text-sm text-gray-400'>
                  Runs as embedded task within web server. Faster startup, shared memory.
                </div>
              </div>
            </label>
            <label
              htmlFor='exec-mode-process'
              aria-label='Process Mode'
              className='flex items-start cursor-pointer p-3 rounded border border-gray-600 hover:border-gray-500'
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
                <div className='text-white font-medium'>Process Mode</div>
                <div className='text-sm text-gray-400'>
                  Runs as separate Python process. Isolated, fault-tolerant, detailed monitoring.
                </div>
              </div>
            </label>
          </div>
        </div>
        <div className='space-y-2'>
          <h4 className='text-sm font-medium text-gray-200'>Autostart:</h4>
          <label
            htmlFor='exec-mode-autostart'
            aria-label='Enable automatic restart'
            className='flex items-start cursor-pointer p-3 rounded border border-gray-600 hover:border-gray-500'
          >
            <input
              id='exec-mode-autostart'
              type='checkbox'
              checked={autostart}
              onChange={(event: React.ChangeEvent<HTMLInputElement>) =>
                setAutostart(event.target.checked)
              }
              className='mr-3 mt-1'
            />
            <div>
              <div className='text-white font-medium'>Enable automatic restart</div>
              <div className='text-sm text-gray-400'>
                Keep this process enabled so it starts automatically with the server.
              </div>
            </div>
          </label>
        </div>
        <div className='flex justify-end space-x-3 pt-4'>
          <button
            onClick={onClose}
            className='px-4 py-2 text-gray-300 hover:text-white transition-colors'
          >
            Cancel
          </button>
          <button
            onClick={handleStart}
            className='px-4 py-2 bg-primary-600 text-white rounded hover:bg-primary-700 transition-colors'
          >
            Start {componentName}
          </button>
        </div>
      </div>
    </Modal>
  )
}
