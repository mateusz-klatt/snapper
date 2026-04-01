import React, { useEffect, useMemo, useState } from 'react'
import { Modal } from '../../components/ui/Modal'
import type { AvailableProcess } from '../../types/api'
import { useProcessSchema } from '../../hooks/queries'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'

export interface StrategyLaunchData {
  template: string
  processName: string
  strategyName: string
  executionMode: 'thread' | 'process'
  autostart: boolean
  startImmediately: boolean
  note?: string
  parameters: Record<string, unknown>
}
interface StrategyLaunchModalProps {
  open: boolean
  onClose: () => void
  templates: AvailableProcess[]
  onSubmit: (data: StrategyLaunchData) => Promise<void>
  isSubmitting?: boolean
}
const sanitizeName = (value: string) =>
  value
    .toLowerCase()
    .replaceAll(/[^a-z0-9_]+/g, '_')
    .replaceAll(/(?:^_+)|(?:_+$)/g, '')

export const StrategyLaunchModal: React.FC<Readonly<StrategyLaunchModalProps>> = ({
  open,
  onClose,
  templates,
  onSubmit,
  isSubmitting = false,
}) => {
  const readOnly = useIsReadOnly()
  const sortedTemplates = useMemo(
    () => [...templates].sort((a, b) => a.name.localeCompare(b.name)),
    [templates]
  )
  const [selectedTemplate, setSelectedTemplate] = useState<string>('')
  const [processName, setProcessName] = useState<string>('')
  const [strategyName, setStrategyName] = useState<string>('')
  const [executionMode, setExecutionMode] = useState<'thread' | 'process'>('thread')
  const [autostart, setAutostart] = useState<boolean>(false)
  const [startImmediately, setStartImmediately] = useState<boolean>(true)
  const [note, setNote] = useState<string>('')
  const processSchema = useProcessSchema(selectedTemplate, {
    enabled: open && selectedTemplate.length > 0,
  })
  const defaultKwargs = useMemo(() => {
    const raw = processSchema.data?.payload.default_parameters ?? {}

    return typeof raw === 'object' && raw !== null ? { ...raw } : {}
  }, [processSchema.data?.payload.default_parameters])
  const defaultStrategyName =
    typeof defaultKwargs.name === 'string' ? defaultKwargs.name : undefined

  useEffect(() => {
    if (!open) {
      return
    }

    const schemaMode = processSchema.data?.payload.default_mode

    if (schemaMode === 'thread' || schemaMode === 'process') {
      setExecutionMode(schemaMode)
    }
  }, [open, processSchema.data?.payload.default_mode])
  useEffect(() => {
    if (!open) {
      setSelectedTemplate('')
      setProcessName('')
      setStrategyName('')
      setExecutionMode('thread')
      setAutostart(false)
      setStartImmediately(true)
      setNote('')

      return
    }

    if (!selectedTemplate && templates.length > 0) {
      setSelectedTemplate(templates[0].name)
    }
  }, [open, selectedTemplate, templates])
  useEffect(() => {
    if (!selectedTemplate) {
      return
    }

    const baseTemplate = sanitizeName(selectedTemplate)

    if (!processName) {
      setProcessName(`${baseTemplate}_instance`)
    }

    const defaultName = defaultStrategyName ?? baseTemplate.replace(/^strategy_/, '')

    if (!strategyName) {
      setStrategyName(sanitizeName(defaultName))
    }
  }, [selectedTemplate, defaultStrategyName, processName, strategyName])

  const handleSubmit = async (event: React.SubmitEvent<HTMLFormElement>) => {
    event.preventDefault()
    const template = selectedTemplate
    const params: Record<string, unknown> = {
      ...defaultKwargs,
      name: sanitizeName(strategyName),
    }
    const defaultOutput = defaultKwargs.output

    if (typeof defaultOutput === 'string') {
      const baseOutput: string = defaultOutput
      const prefix = baseOutput.includes('.')
        ? baseOutput.slice(0, baseOutput.lastIndexOf('.') + 1)
        : ''

      params.output = `${prefix}${sanitizeName(strategyName)}`
    }

    try {
      await onSubmit({
        template,
        processName: sanitizeName(processName),
        strategyName: sanitizeName(strategyName),
        executionMode,
        autostart,
        startImmediately,
        note: note.trim() || undefined,
        parameters: params,
      })
    } catch (error) {
      console.error('Failed to submit strategy registration', error)
    }
  }

  return (
    <Modal open={open} onClose={onClose} title='Register Strategy Process' size='lg'>
      {templates.length === 0 ? (
        <div className='space-y-3 text-sm text-muted-600'>
          <p>No strategy templates registered in backend. Define at least one strategy process.</p>
          <div className='flex justify-end pt-2'>
            <button
              type='button'
              onClick={onClose}
              className='px-4 py-2 text-sm text-muted-600 hover:text-alpine-900'
            >
              Close
            </button>
          </div>
        </div>
      ) : (
        <form onSubmit={handleSubmit} className='space-y-5'>
          <div>
            <label
              htmlFor='strategy-template'
              className='block text-sm font-medium text-muted-700 mb-2'
            >
              Strategy template
            </label>
            <select
              id='strategy-template'
              value={selectedTemplate}
              onChange={e => {
                setSelectedTemplate(e.target.value)
                setProcessName('')
                setStrategyName('')
              }}
              className='w-full px-3 py-2 bg-alpine-50 border border-dark-600 rounded-md text-alpine-900 focus:outline-hidden focus:ring-2 focus:ring-brand-500'
              required
            >
              <option value=''>Choose template...</option>
              {sortedTemplates.map(template => (
                <option key={template.name} value={template.name}>
                  {template.name}
                </option>
              ))}
            </select>
            {selectedTemplate && (
              <p className='mt-1 text-xs text-muted-500'>
                {sortedTemplates.find(template => template.name === selectedTemplate)
                  ?.description || 'Strategy registered in backend'}
              </p>
            )}
            {processSchema.isLoading && (
              <p className='mt-2 text-xs text-muted-400'>Loading template defaults…</p>
            )}
            {processSchema.error && (
              <p className='mt-2 text-xs text-warning-400'>
                Unable to load template defaults. Please check backend logs.
              </p>
            )}
          </div>
          <div className='grid grid-cols-1 sm:grid-cols-2 gap-4'>
            <div>
              <label
                htmlFor='process-name'
                className='block text-sm font-medium text-muted-700 mb-2'
              >
                Process name
              </label>
              <input
                id='process-name'
                type='text'
                value={processName}
                onChange={e => setProcessName(e.target.value)}
                placeholder='strategy_macd_custom'
                className='w-full px-3 py-2 bg-alpine-50 border border-dark-600 rounded-md text-alpine-900 focus:outline-hidden focus:ring-2 focus:ring-brand-500'
                required
              />
              <p className='mt-1 text-xs text-muted-400'>
                Allowed characters: lowercase letters, numbers, underscores
              </p>
            </div>
            <div>
              <label
                htmlFor='strategy-instance-name'
                className='block text-sm font-medium text-muted-700 mb-2'
              >
                Strategy instance name
              </label>
              <input
                id='strategy-instance-name'
                type='text'
                value={strategyName}
                onChange={e => setStrategyName(e.target.value)}
                placeholder='macd_custom'
                className='w-full px-3 py-2 bg-alpine-50 border border-dark-600 rounded-md text-alpine-900 focus:outline-hidden focus:ring-2 focus:ring-brand-500'
                required
              />
              <p className='mt-1 text-xs text-muted-400'>
                Used within strategy configuration and output topics
              </p>
            </div>
          </div>
          <div className='grid grid-cols-1 sm:grid-cols-2 gap-4'>
            <div>
              <label
                htmlFor='execution-mode'
                className='block text-sm font-medium text-muted-700 mb-2'
              >
                Execution mode
              </label>
              <select
                id='execution-mode'
                value={executionMode}
                onChange={e => setExecutionMode(e.target.value as 'thread' | 'process')}
                className='w-full px-3 py-2 bg-alpine-50 border border-dark-600 rounded-md text-alpine-900 focus:outline-hidden focus:ring-2 focus:ring-brand-500'
              >
                <option value='thread'>Embedded thread</option>
                <option value='process'>Isolated process</option>
              </select>
              <p className='mt-1 text-xs text-muted-400'>
                Threads share memory, processes provide stronger isolation.
              </p>
            </div>
            <div className='space-y-2'>
              <span className='block text-sm font-medium text-muted-700 mb-2'>Flags</span>
              <label className='flex items-center text-sm text-muted-600'>
                <input
                  type='checkbox'
                  className='mr-2 text-info-500 focus:ring-brand-500'
                  checked={autostart}
                  onChange={e => setAutostart(e.target.checked)}
                />{' '}
                Autostart on server boot
              </label>
              <label className='flex items-center text-sm text-muted-600'>
                <input
                  type='checkbox'
                  className='mr-2 text-info-500 focus:ring-brand-500'
                  checked={startImmediately}
                  onChange={e => setStartImmediately(e.target.checked)}
                />{' '}
                Start immediately after registration
              </label>
            </div>
          </div>
          <div>
            <label
              htmlFor='strategy-note'
              className='block text-sm font-medium text-muted-700 mb-2'
            >
              Note (optional)
            </label>
            <textarea
              id='strategy-note'
              value={note}
              onChange={e => setNote(e.target.value)}
              maxLength={512}
              rows={2}
              className='w-full px-3 py-2 bg-alpine-50 border border-dark-600 rounded-md text-alpine-900 focus:outline-hidden focus:ring-2 focus:ring-brand-500'
              placeholder='Describe purpose or parameters of this strategy instance.'
            />
            <p className='mt-1 text-xs text-muted-400'>
              Stored alongside configuration to help identify this process.
            </p>
          </div>
          <div className='flex justify-end space-x-3 pt-4'>
            <button
              type='button'
              onClick={onClose}
              className='px-4 py-2 text-sm text-muted-600 hover:text-alpine-900'
              disabled={isSubmitting}
            >
              Cancel
            </button>
            <button
              type='submit'
              disabled={
                readOnly ||
                isSubmitting ||
                processSchema.isLoading ||
                !!processSchema.error ||
                !selectedTemplate ||
                !processName.trim() ||
                !strategyName.trim()
              }
              className='px-4 py-2 bg-info-600 text-white text-sm font-medium rounded-md hover:bg-info-700 disabled:opacity-50 disabled:cursor-not-allowed'
            >
              {isSubmitting ? 'Registering…' : 'Register strategy'}
            </button>
          </div>
        </form>
      )}
    </Modal>
  )
}
