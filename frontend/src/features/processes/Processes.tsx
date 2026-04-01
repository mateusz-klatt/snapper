import React, { useState } from 'react'
import { LiveOnlyNotice } from '../../components/LiveOnlyNotice'
import {
  useStartProcessByName,
  useStopProcessByName,
  useConfiguredProcesses,
  useAvailableProcesses,
  useProcessRuns,
} from '../../hooks/queries'
import { useHeartbeats, type HeartbeatData } from '../../hooks/useHeartbeats'
import { useConfirmDialog } from '../../hooks/useConfirmDialog'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'
import { ProcessControlCard } from './ProcessControlCard'
import { ExecutionModeModal } from './ExecutionModeModal'
import { ConfirmDialog } from '../../components/ui/ConfirmDialog'
import { ProcessesSkeleton } from '../../components/Skeleton'
import type { ConfiguredProcess, AvailableProcess, ProcessRun } from '../../types/api'
import { noop } from '../../lib/noop'

const resolveStatusBadge = (process: ConfiguredProcess): string => {
  if (process.is_one_shot) return 'one-shot'
  if (process.enabled) return 'auto-start'

  return 'manual'
}

export const Processes: React.FC = () => {
  const readOnly = useIsReadOnly()
  const { openConfirm, dialogProps: confirmDialogProps } = useConfirmDialog()
  const [executionModeModal, setExecutionModeModal] = useState<{
    open: boolean
    componentName: string
    description: string
    onStart: (options: { executionMode: 'thread' | 'process' }) => void
  }>({
    open: false,
    componentName: '',
    description: '',
    onStart: noop,
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

    availableProcesses?.payload.forEach(process => {
      map[process.name] = process
    })

    return map
  }, [availableProcesses])
  const latestRunByProcess = React.useMemo<Record<string, ProcessRun>>(() => {
    if (!processRuns?.payload?.length) {
      return {}
    }

    return processRuns.payload.reduce<Record<string, ProcessRun>>((acc, run) => {
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
  const longRunningProcesses = React.useMemo<ConfiguredProcess[]>(() => {
    if (!configuredProcesses) return []

    return configuredProcesses.payload.filter(
      process =>
        process.lifecycle === 'long_running' &&
        process.role !== 'strategy' &&
        process.role !== 'backtest'
    )
  }, [configuredProcesses])
  const taskProcesses = React.useMemo<ConfiguredProcess[]>(() => {
    if (!configuredProcesses) return []

    return configuredProcesses.payload.filter(
      process =>
        process.lifecycle === 'one_shot' &&
        process.role !== 'strategy' &&
        process.role !== 'backtest'
    )
  }, [configuredProcesses])
  const formatTimestamp = React.useCallback((timestamp?: string | null) => {
    if (!timestamp) return null
    const date = new Date(timestamp)

    if (Number.isNaN(date.getTime())) {
      return null
    }

    return date.toLocaleString()
  }, [])

  const getDescription = React.useCallback(
    (process: ConfiguredProcess): string => {
      const registryDetails = registryByName[process.name]

      return registryDetails?.description || process.note || ''
    },
    [registryByName]
  )

  const startProcess = useStartProcessByName()
  const stopProcess = useStopProcessByName()

  const closeExecutionModeModal = React.useCallback(() => {
    setExecutionModeModal({
      open: false,
      componentName: '',
      description: '',
      onStart: noop,
    })
  }, [])

  const showExecutionModeModal = React.useCallback(
    (
      componentName: string,
      description: string,
      onStart: (options: { executionMode: 'thread' | 'process' }) => void
    ) => {
      setExecutionModeModal({
        open: true,
        componentName,
        description,
        onStart,
      })
    },
    []
  )

  const handleStart = React.useCallback(
    (name: string, description: string) => {
      showExecutionModeModal(name, description, ({ executionMode }) =>
        startProcess.mutate({ name, mode: executionMode })
      )
    },
    [startProcess, showExecutionModeModal]
  )

  const handleStop = React.useCallback(
    (processName: string, message: string) => {
      openConfirm({
        title: `Stop ${processName}`,
        message,
        onConfirm: () => stopProcess.mutate({ name: processName }),
      })
    },
    [stopProcess, openConfirm]
  )

  const handleRestart = React.useCallback(
    (processName: string, stopMessage: string, openStartModal: () => void) => {
      openConfirm({
        title: `Restart ${processName}`,
        message: stopMessage,
        onConfirm: () => stopProcess.mutate({ name: processName }, { onSuccess: openStartModal }),
      })
    },
    [stopProcess, openConfirm]
  )

  const getHeartbeat = React.useCallback(
    (processName: string): HeartbeatData | undefined => {
      if (processName.startsWith('executor_')) {
        return allHeartbeats[processName] ?? { status: 'unknown', healthy: false, timestamp: 0 }
      }

      if (processName.includes('feed_publisher')) {
        const exchangeName = processName.replace('_feed_publisher', '')

        return (
          allHeartbeats[`feed.${exchangeName}`] ?? {
            status: 'unknown',
            healthy: false,
            timestamp: 0,
          }
        )
      }

      return undefined
    },
    [allHeartbeats]
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
      <LiveOnlyNotice />
      <div className='flex items-center justify-between'>
        <h1 className='text-2xl font-bold text-alpine-900'>Process Control</h1>
        <div className='text-sm text-muted-600'>Real-time process monitoring and control</div>
      </div>
      {}
      <div className='space-y-4'>
        <h2 className='text-lg font-semibold text-primary-600'>Long-Running Processes</h2>
        <div className='grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4'>
          {longRunningProcesses.map(process => {
            const description = getDescription(process)
            const heartbeat = getHeartbeat(process.name)

            return (
              <ProcessControlCard
                key={process.name}
                title={description || process.name}
                description={process.note && process.note !== description ? process.note : ''}
                status={process.running ? 'running' : 'stopped'}
                details={undefined}
                heartbeat={heartbeat}
                onStart={() => handleStart(process.name, description)}
                onStop={() =>
                  handleStop(process.name, `This will stop the ${process.name} process.`)
                }
                onRestart={() =>
                  handleRestart(
                    process.name,
                    `This will restart the ${process.name} process.`,
                    () => handleStart(process.name, description)
                  )
                }
                isStarting={startProcess.isPending && startProcess.variables?.name === process.name}
                isStopping={stopProcess.isPending && stopProcess.variables?.name === process.name}
                readOnly={readOnly}
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

              const statusBadge = resolveStatusBadge(process)
              const description = getDescription(process)
              const latestRun = latestRunByProcess[process.name]
              const details: Record<string, string> = {}

              if (latestRun) {
                details.last_run = `${latestRun.status} (${formatTimestamp(latestRun.started_at)})`
              }

              return (
                <ProcessControlCard
                  key={process.name}
                  title={description || process.name}
                  description=''
                  status={status}
                  statusBadge={statusBadge}
                  details={details}
                  readOnly={readOnly}
                  onStart={() => handleStart(process.name, description)}
                  onStop={() =>
                    handleStop(process.name, `This will stop the ${process.name} process.`)
                  }
                  onRestart={() =>
                    handleRestart(
                      process.name,
                      `This will restart the ${process.name} process.`,
                      () => handleStart(process.name, description)
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
      <ConfirmDialog {...confirmDialogProps} />
      <ExecutionModeModal
        open={executionModeModal.open}
        onClose={closeExecutionModeModal}
        onStart={executionModeModal.onStart}
        componentName={executionModeModal.componentName}
        description={executionModeModal.description}
      />
    </div>
  )
}
