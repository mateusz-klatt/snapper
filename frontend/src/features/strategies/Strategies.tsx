import React, { useState, useEffect, useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import toast from 'react-hot-toast'
import {
  useStartProcessByName,
  useStopProcessByName,
  useConfiguredProcesses,
  useAvailableProcesses,
  useCreateProcessConfig,
} from '../../hooks/queries'
import { useWebSocketStore } from '../../stores/websocket'
import { StrategyLaunchModal, type StrategyLaunchData } from './StrategyLaunchModal'
import { StrategyCard } from './StrategyCard'
import { StrategiesSkeleton } from '../../components/Skeleton'

interface FeedHealth {
  status: string
  lag_ms: number
  heartbeat_age_ms: number
  healthy: boolean
}
interface HealthStatus {
  status: 'ok' | 'warn' | 'error'
  lag_ms: number
  timestamp: number
  seq?: number
  feed_health?: Record<string, FeedHealth>
  inputs?: string[]
  outputs?: string[]
}

export const Strategies: React.FC = () => {
  const [strategyModalOpen, setStrategyModalOpen] = useState(false)
  const [activeStrategyProcess, setActiveStrategyProcess] = useState<string | null>(null)
  const [healthStatuses, setHealthStatuses] = useState<Record<string, HealthStatus>>({})
  const queryClient = useQueryClient()
  const { wsClient } = useWebSocketStore()
  const startProcess = useStartProcessByName()
  const stopProcess = useStopProcessByName()
  const createProcessConfig = useCreateProcessConfig()
  const { data: configuredProcesses, isLoading } = useConfiguredProcesses()
  const { data: availableProcesses } = useAvailableProcesses()
  const strategyTemplates = useMemo(() => {
    return availableProcesses?.processes.filter(process => process.role === 'strategy') ?? []
  }, [availableProcesses?.processes])
  const strategies = useMemo(
    () => configuredProcesses?.processes.filter(p => p.role === 'strategy') || [],
    [configuredProcesses?.processes]
  )

  useEffect(() => {
    if (!wsClient) {
      return
    }

    const activeClient = wsClient
    const heartbeatTopics = strategies.map(s => {
      const strategyId = s.name.startsWith('strategy_') ? s.name.replace(/^strategy_/, '') : s.name

      return `system.heartbeats.strategy.${strategyId}`
    })

    if (heartbeatTopics.length === 0) {
      return
    }

    activeClient.subscribe(heartbeatTopics)
    const unsubscribeConnection = activeClient.onConnection((connected: boolean) => {
      if (connected) {
        activeClient.subscribe(heartbeatTopics)
      }
    })
    const unsubscribeHeartbeat = activeClient.onMessage('heartbeat', message => {
      const meta = message.meta as
        | {
            feed_health?: Record<string, FeedHealth>
            inputs?: string[]
            outputs?: string[]
            running?: boolean
          }
        | undefined

      if (message.component.startsWith('strategy_')) {
        const strategyName = message.component.replace('strategy_', '')
        const strategy = strategies.find(s => s.name === strategyName)

        if (strategy) {
          const resolveHeartbeatStatus = (heartbeatStatus: string): 'ok' | 'warn' | 'error' => {
            if (heartbeatStatus === 'healthy') return 'ok'
            if (heartbeatStatus === 'warning') return 'warn'

            return 'error'
          }

          const status = resolveHeartbeatStatus(message.status)

          setHealthStatuses(prev => ({
            ...prev,
            [strategyName]: {
              status,
              lag_ms: message.lag_ms || 0,
              timestamp: Date.now(),
              seq: message.sequence,
              feed_health: meta?.feed_health,
              inputs: meta?.inputs,
              outputs: meta?.outputs,
            },
          }))
        }
      }
    })

    return () => {
      unsubscribeConnection()
      unsubscribeHeartbeat()
      activeClient.unsubscribe(heartbeatTopics)
    }
  }, [strategies, wsClient])

  const handleStrategyLaunch = async (data: StrategyLaunchData) => {
    try {
      await createProcessConfig.mutateAsync({
        name: data.processName,
        template: data.template,
        enabled: data.autostart,
        mode: data.executionMode,
        args: data.args,
        kwargs: data.kwargs,
        note: data.note,
      })
      toast.success(`Strategy ${data.processName} saved`)

      if (data.startImmediately) {
        await startProcess.mutateAsync({
          name: data.processName,
          mode: data.executionMode,
        })
        toast.success(`Strategy ${data.processName} started`)
        setActiveStrategyProcess(data.processName)
      }

      setStrategyModalOpen(false)
      queryClient.invalidateQueries({ queryKey: ['processes', 'configured'] })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'

      toast.error(`Failed to register strategy: ${message}`)
    }
  }

  const handleStartStrategy = (processName: string, mode: string) => {
    setActiveStrategyProcess(processName)
    startProcess.mutate(
      {
        name: processName,
        mode: mode || 'thread',
      },
      {
        onSuccess: () => {
          toast.success(`Strategy started successfully`, {
            duration: 3000,
            icon: '🚀',
          })
          queryClient.invalidateQueries({ queryKey: ['processes', 'configured'] })
        },
        onError: (error: Error) => {
          setActiveStrategyProcess(null)
          const errorMessage = error.message.toLowerCase()

          if (errorMessage.includes('already running')) {
            toast.error('Strategy is already running', {
              duration: 4000,
              icon: '⚠️',
            })
          } else if (errorMessage.includes('not found')) {
            toast.error('Strategy configuration not found. Please check database settings.', {
              duration: 5000,
              icon: '❌',
            })
          } else if (errorMessage.includes('network') || errorMessage.includes('timeout')) {
            toast.error('Network error. Please check connection and try again.', {
              duration: 5000,
              icon: '🌐',
            })
          } else {
            toast.error(`Failed to start strategy: ${error.message}`, {
              duration: 5000,
              icon: '⚠️',
            })
          }
        },
      }
    )
  }

  const handleStopStrategy = (processName: string) => {
    stopProcess.mutate(
      { name: processName },
      {
        onSuccess: () => {
          setActiveStrategyProcess(null)
          toast.success(`Strategy stopped successfully`, {
            duration: 3000,
            icon: '✋',
          })
          queryClient.invalidateQueries({ queryKey: ['processes', 'configured'] })
        },
        onError: (error: Error) => {
          const errorMessage = error.message.toLowerCase()

          if (errorMessage.includes('not running')) {
            toast.error('Strategy is not running', {
              duration: 4000,
              icon: '⚠️',
            })

            if (activeStrategyProcess === processName) {
              setActiveStrategyProcess(null)
            }
          } else if (errorMessage.includes('network') || errorMessage.includes('timeout')) {
            toast.error('Network error. Please check connection and try again.', {
              duration: 5000,
              icon: '🌐',
            })
          } else {
            toast.error(`Failed to stop strategy: ${error.message}`, {
              duration: 5000,
              icon: '⚠️',
            })
          }
        },
      }
    )
  }

  if (isLoading) {
    return (
      <div className='p-4'>
        <StrategiesSkeleton />
      </div>
    )
  }

  return (
    <div className='p-4 space-y-6'>
      <div className='flex items-center justify-between'>
        <h2 className='text-xl font-bold text-white'>Strategy Management</h2>
        <button
          onClick={() => setStrategyModalOpen(true)}
          disabled={createProcessConfig.isPending || startProcess.isPending}
          className='px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed'
        >
          {createProcessConfig.isPending ? 'Saving…' : 'Register Strategy'}
        </button>
        <p className='mt-2 text-xs text-dark-400'>
          Register new strategy processes directly from the UI. Autostart keeps the process enabled
          across restarts.
        </p>
      </div>
      {}
      <div className='space-y-4'>
        <h3 className='text-lg font-medium text-white'>Configured Strategies</h3>
        {strategies.length > 0 ? (
          <div className='grid grid-cols-1 lg:grid-cols-2 gap-4'>
            {strategies.map(strategy => (
              <StrategyCard
                key={strategy.name}
                name={strategy.name}
                running={strategy.running}
                autoStartEnabled={strategy.enabled}
                mode={strategy.mode as 'thread' | 'process'}
                health={healthStatuses[strategy.name]}
                onStart={() => handleStartStrategy(strategy.name, strategy.mode)}
                onStop={() => handleStopStrategy(strategy.name)}
                isStarting={startProcess.isPending && activeStrategyProcess === strategy.name}
                isStopping={stopProcess.isPending && activeStrategyProcess === strategy.name}
              />
            ))}
          </div>
        ) : (
          <div className='bg-dark-800 border border-dark-700 rounded-lg p-6 text-center'>
            <p className='text-dark-400'>No strategies configured</p>
            <p className='text-sm text-dark-500 mt-1'>
              Configure strategies in the database to see them here
            </p>
          </div>
        )}
      </div>
      {}
      <StrategyLaunchModal
        open={strategyModalOpen}
        onClose={() => setStrategyModalOpen(false)}
        templates={strategyTemplates}
        onSubmit={handleStrategyLaunch}
        isSubmitting={createProcessConfig.isPending || startProcess.isPending}
      />
    </div>
  )
}
