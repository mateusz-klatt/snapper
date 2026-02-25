import React, { useState } from 'react'
import {
  useStartProcessByName,
  useStopProcessByName,
  useConfiguredProcesses,
  useAvailableProcesses,
  useProcessRuns,
} from '../../hooks/queries'
import { useHeartbeats, type HeartbeatData } from '../../hooks/useHeartbeats'
import { ProcessControlCard } from './ProcessControlCard'
import { ExecutionModeModal } from './ExecutionModeModal'
import { ConfirmDialog } from '../../components/ui/ConfirmDialog'
import { ProcessesSkeleton } from '../../components/Skeleton'
import type { ConfiguredProcess, AvailableProcess, ProcessRun } from '../../types/api'
import { noop } from '../../lib/noop'

type ConfirmDialogState = {
  open: boolean
  title: string
  message: string
  onConfirm: () => void
}

export const Processes: React.FC = () => {
  const [executionModeModal, setExecutionModeModal] = useState<{
    open: boolean
    componentName: string
    description: string
    defaultAutostart: boolean
    onStart: (options: { executionMode: 'thread' | 'process'; autostart: boolean }) => void
  }>({
    open: false,
    componentName: '',
    description: '',
    defaultAutostart: false,
    onStart: noop,
  })
  const [confirmDialog, setConfirmDialog] = useState<ConfirmDialogState>({
    open: false,
    title: '',
    message: '',
    onConfirm: noop,
  })
  const heartbeatTopics = React.useMemo(
    () => ['system.heartbeats.executor.', 'system.heartbeats.feed.'],
    []
  )
  const allHeartbeats = useHeartbeats(heartbeatTopics)
  const { data: configuredProcesses, isLoading } = useConfiguredProcesses()
  const { data: availableProcesses } = useAvailableProcesses()
  const { data: processRuns } = useProcessRuns({
    enabled: Boolean(configuredProcesses?.count),
    limit: 50,
  })
  const registryByName = React.useMemo<Record<string, AvailableProcess>>(() => {
    const map: Record<string, AvailableProcess> = {}

    availableProcesses?.processes.forEach(process => {
      map[process.name] = process
    })

    return map
  }, [availableProcesses])
  const latestRunByProcess = React.useMemo<Record<string, ProcessRun>>(() => {
    if (!processRuns?.runs?.length) {
      return {}
    }

    return processRuns.runs.reduce<Record<string, ProcessRun>>((acc, run) => {
      const previous = acc[run.process_name]

      if (!previous) {
        acc[run.process_name] = run

        return acc
      }

      const previousStart = new Date(previous.started_at).getTime()
      const currentStart = new Date(run.started_at).getTime()

      if (currentStart >= previousStart) {
        acc[run.process_name] = run
      }

      return acc
    }, {})
  }, [processRuns])
  const getProcess = React.useCallback(
    (name: string) => configuredProcesses?.processes.find(p => p.name === name),
    [configuredProcesses]
  )
  const longRunningProcesses = React.useMemo<ConfiguredProcess[]>(() => {
    if (!configuredProcesses) return []
    const featured = new Set(['zmq_broker', 'executor', 'feed_publisher'])

    return configuredProcesses.processes.filter(
      process =>
        process.lifecycle === 'long_running' &&
        !featured.has(process.name) &&
        process.role !== 'strategy' &&
        process.role !== 'backtest'
    )
  }, [configuredProcesses])
  const taskProcesses = React.useMemo<ConfiguredProcess[]>(() => {
    if (!configuredProcesses) return []
    const tasks = configuredProcesses.processes.filter(
      process =>
        process.lifecycle === 'one_shot' &&
        process.role !== 'strategy' &&
        process.role !== 'backtest'
    )

    return tasks
  }, [configuredProcesses])
  const formatTimestamp = React.useCallback((timestamp?: string | null) => {
    if (!timestamp) return null
    const date = new Date(timestamp)

    if (Number.isNaN(date.getTime())) {
      return null
    }

    return date.toLocaleString()
  }, [])

  const startProcess = useStartProcessByName()
  const stopProcess = useStopProcessByName()

  const executeAction = React.useCallback((action: () => void, title: string, message: string) => {
    setConfirmDialog({ open: true, title, message, onConfirm: action })
  }, [])

  const showExecutionModeModal = React.useCallback(
    (
      componentName: string,
      description: string,
      defaultAutostart: boolean,
      onStart: (options: { executionMode: 'thread' | 'process'; autostart: boolean }) => void
    ) => {
      setExecutionModeModal({
        open: true,
        componentName,
        description,
        defaultAutostart,
        onStart,
      })
    },
    []
  )

  const closeExecutionModeModal = () => {
    setExecutionModeModal({
      open: false,
      componentName: '',
      description: '',
      defaultAutostart: false,
      onStart: noop,
    })
  }

  const handleTaskProcessStart = React.useCallback(
    (processName: string) => {
      showExecutionModeModal(
        processName,
        `Start ${processName.replaceAll('_', ' ')} process`,
        !!getProcess(processName)?.enabled,
        ({ executionMode, autostart }) =>
          executeAction(
            () =>
              startProcess.mutate({
                name: processName,
                mode: executionMode,
                autostart,
              }),
            `Start ${processName}`,
            `This will start the ${processName} process.`
          )
      )
    },
    [startProcess, executeAction, showExecutionModeModal, getProcess]
  )
  const handleRestart = React.useCallback(
    (processName: string, stopMessage: string, openStartModal: () => void) => {
      setConfirmDialog({
        open: true,
        title: `Restart ${processName}`,
        message: stopMessage,
        onConfirm: () => {
          stopProcess.mutate({ name: processName }, { onSuccess: openStartModal })
        },
      })
    },
    [stopProcess]
  )

  const getProcessAutostartDefault = React.useCallback(
    (name: string): boolean => {
      const process = configuredProcesses?.processes.find(item => item.name === name)

      return process?.enabled ?? false
    },
    [configuredProcesses]
  )

  if (isLoading) {
    return (
      <div className='p-6'>
        <ProcessesSkeleton />
      </div>
    )
  }

  return (
    <div className='space-y-6'>
      <div className='flex items-center justify-between'>
        <h1 className='text-2xl font-bold text-alpine-900'>Process Control</h1>
        <div className='text-sm text-muted-600'>Real-time process monitoring and control</div>
      </div>
      {}
      <div className='space-y-4'>
        <h2 className='text-lg font-semibold text-primary-600'>Long-Running Processes</h2>
        <div className='grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4'>
          {}
          <ProcessControlCard
            title='ZMQ Broker'
            description='Message routing and distribution hub'
            status={getProcess('zmq_broker')?.running ? 'running' : 'stopped'}
            details={undefined}
            onStart={() =>
              showExecutionModeModal(
                'ZMQ Broker',
                'Message routing and distribution hub for process communication.',
                getProcessAutostartDefault('zmq_broker'),
                ({ executionMode, autostart }) => {
                  startProcess.mutate({
                    name: 'zmq_broker',
                    mode: executionMode,
                    autostart,
                  })
                }
              )
            }
            onStop={() =>
              executeAction(
                () => stopProcess.mutate({ name: 'zmq_broker' }),
                'Stop ZMQ Broker',
                'This will stop the ZMQ broker. All connected processes will lose connectivity.'
              )
            }
            onRestart={() =>
              handleRestart(
                'zmq_broker',
                'This will restart the ZMQ broker. All connected processes will briefly lose connectivity.',
                () =>
                  showExecutionModeModal(
                    'ZMQ Broker',
                    'Message routing and distribution hub for process communication.',
                    getProcessAutostartDefault('zmq_broker'),
                    ({ executionMode, autostart }) => {
                      startProcess.mutate({
                        name: 'zmq_broker',
                        mode: executionMode,
                        autostart,
                      })
                    }
                  )
              )
            }
            isStarting={startProcess.isPending && startProcess.variables?.name === 'zmq_broker'}
            isStopping={stopProcess.isPending && stopProcess.variables?.name === 'zmq_broker'}
          />
          {}
          {longRunningProcesses.map(process => {
            const registryDetails = registryByName[process.name]
            let heartbeatData: Record<string, HeartbeatData> | undefined
            let heartbeatLabel: string | undefined

            if (process.name.startsWith('executor_')) {
              const executorName = process.name.replace('executor_', '')
              const componentName = process.name

              heartbeatData = {
                [executorName]: allHeartbeats[componentName] || {
                  status: 'unknown',
                  healthy: false,
                  timestamp: 0,
                  lag_ms: undefined,
                },
              }
              heartbeatLabel = 'Exchanges'
            } else if (process.name.includes('feed_publisher')) {
              const exchangeName = process.name.replace('_feed_publisher', '')
              const componentName = `feed.${exchangeName}`

              heartbeatData = {
                [componentName]: allHeartbeats[componentName] || {
                  status: 'unknown',
                  healthy: false,
                  timestamp: 0,
                  lag_ms: undefined,
                },
              }
              heartbeatLabel = 'Feeds'
            }

            return (
              <ProcessControlCard
                key={process.name}
                title={registryDetails?.description || process.name}
                description={process.note || ''}
                status={process.running ? 'running' : 'stopped'}
                details={undefined}
                heartbeatData={heartbeatData}
                heartbeatLabel={heartbeatLabel}
                onStart={() =>
                  showExecutionModeModal(
                    process.name,
                    registryDetails?.description || '',
                    process.enabled,
                    ({ executionMode, autostart }) => {
                      startProcess.mutate({
                        name: process.name,
                        mode: executionMode,
                        autostart,
                      })
                    }
                  )
                }
                onStop={() =>
                  executeAction(
                    () => stopProcess.mutate({ name: process.name }),
                    `Stop ${process.name}`,
                    `This will stop the ${process.name} process.`
                  )
                }
                onRestart={() =>
                  handleRestart(
                    process.name,
                    `This will restart the ${process.name} process.`,
                    () =>
                      showExecutionModeModal(
                        process.name,
                        registryDetails?.description || '',
                        process.enabled,
                        ({ executionMode, autostart }) => {
                          startProcess.mutate({
                            name: process.name,
                            mode: executionMode,
                            autostart,
                          })
                        }
                      )
                  )
                }
                isStarting={startProcess.isPending && startProcess.variables?.name === process.name}
                isStopping={stopProcess.isPending && stopProcess.variables?.name === process.name}
              />
            )
          })}
        </div>
      </div>
      {}
      {taskProcesses.length > 0 && (
        <div className='space-y-4'>
          <h2 className='text-lg font-semibold text-primary-600'>Task Processes</h2>
          <div className='grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4'>
            {taskProcesses.map(process => {
              const status: 'running' | 'stopped' | 'error' = process.running
                ? 'running'
                : 'stopped'

              const resolveStatusBadge = (): string => {
                if (process.is_one_shot) return 'one-shot'
                if (process.enabled) return 'auto-start'

                return 'manual'
              }

              const statusBadge = resolveStatusBadge()
              const registryDetails = registryByName[process.name]
              const latestRun = latestRunByProcess[process.name]
              const tags = registryDetails?.tags?.length ? registryDetails.tags : process.tags
              const details: Record<string, string> = {
                lifecycle: (registryDetails?.lifecycle ?? process.lifecycle).replace('_', ' '),
                role: registryDetails?.role ?? process.role,
                autostart: process.enabled ? 'enabled' : 'disabled',
                mode: process.mode,
              }

              if (tags?.length) {
                details.tags = tags.join(', ')
              }

              if (process.parameters_schema || registryDetails?.parameters_schema) {
                const parameterSource =
                  process.parameters_schema ?? registryDetails?.parameters_schema

                details.parameters_schema = JSON.stringify(parameterSource)
              }

              if (process.active_run_id) {
                details.active_run = process.active_run_id
              }

              if (latestRun) {
                details.last_run = `${latestRun.status} (${formatTimestamp(latestRun.started_at)})`
              }

              return (
                <ProcessControlCard
                  key={process.name}
                  title={process.name
                    .replaceAll('_', ' ')
                    .replaceAll(/\b\w/g, (letter: string) => letter.toUpperCase())}
                  description={
                    registryDetails?.description || `Configured process (${process.mode} mode)`
                  }
                  status={status}
                  statusBadge={statusBadge}
                  details={details}
                  onStart={() => handleTaskProcessStart(process.name)}
                  onStop={() =>
                    executeAction(
                      () => stopProcess.mutate({ name: process.name }),
                      `Stop ${process.name}`,
                      `This will stop the ${process.name} process.`
                    )
                  }
                  onRestart={() =>
                    handleRestart(
                      process.name,
                      `This will restart the ${process.name} process.`,
                      () => handleTaskProcessStart(process.name)
                    )
                  }
                  isStarting={
                    startProcess.isPending && startProcess.variables?.name === process.name
                  }
                  isStopping={stopProcess.isPending && stopProcess.variables?.name === process.name}
                />
              )
            })}
          </div>
        </div>
      )}
      {}
      <ConfirmDialog
        open={confirmDialog.open}
        title={confirmDialog.title}
        message={confirmDialog.message}
        onConfirm={() => {
          confirmDialog.onConfirm()
          setConfirmDialog((prev: ConfirmDialogState) => ({ ...prev, open: false }))
        }}
        onCancel={() => setConfirmDialog((prev: ConfirmDialogState) => ({ ...prev, open: false }))}
      />
      <ExecutionModeModal
        open={executionModeModal.open}
        onClose={closeExecutionModeModal}
        onStart={executionModeModal.onStart}
        componentName={executionModeModal.componentName}
        description={executionModeModal.description}
        defaultAutostart={executionModeModal.defaultAutostart}
      />
    </div>
  )
}
