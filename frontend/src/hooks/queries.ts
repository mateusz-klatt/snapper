import React, { useEffect, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from '../lib/apiClient'
import { useAuth } from '../stores/auth'
import { useTradeStore } from '../stores/trade'
import {
  safeOrderFromAPI,
  safeExecutionFromAPI,
  safeSignalFromAPI,
  positionFromAPI,
} from '../lib/transforms'
import type {
  ConfiguredProcessesResponse,
  AvailableProcessesResponse,
  ProcessRunsResponse,
  ProcessSchemaResponse,
  ProcessCreateRequest,
  ProcessCreateResponse,
} from '../types/api'

const queryKeys = {
  systemStatus: ['system', 'status'] as const,
  processStatus: ['process', 'status'] as const,
  availableProcesses: ['processes', 'available'] as const,
  configuredProcesses: ['processes', 'configured'] as const,
  processSchema: (name: string) => ['processes', 'schema', name] as const,
  processRuns: (name?: string, limit?: number) =>
    ['processes', 'runs', name ?? 'all', limit ?? 50] as const,
  candles: (instrument: string, exchange: string, timeframe: string) =>
    ['candles', instrument, exchange, timeframe] as const,
  exchanges: ['exchanges'] as const,
  exchangeInstruments: (exchange: string) => ['exchanges', exchange, 'instruments'] as const,
  orders: (filters?: { symbol?: string; limit?: number; offset?: number }) =>
    ['orders', filters] as const,
  executions: (filters?: { limit?: number }) => ['executions', filters] as const,
  positions: ['positions'] as const,
  signals: (strategyId?: string, limit?: number, instrument?: string, hours?: number) =>
    ['signals', strategyId, limit, instrument, hours] as const,
}

export const useSystemStatus = () => {
  const { isAuthenticated } = useAuth()

  return useQuery({
    queryKey: queryKeys.systemStatus,
    queryFn: () => apiClient.getSystemStatus(),
    refetchInterval: isAuthenticated ? 30000 : false,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useCandles = (
  instrument: string,
  exchange: string,
  timeframe: string = '1m',
  limit: number = 100,
  enabled: boolean = true
) => {
  const { isAuthenticated } = useAuth()

  return useQuery({
    queryKey: queryKeys.candles(instrument, exchange, timeframe),
    queryFn: () => apiClient.getCandles(instrument, exchange, timeframe, limit),
    enabled: enabled && !!instrument && !!exchange && isAuthenticated,
    staleTime: 2000,
    throwOnError: false,
    retry: 2,
  })
}

export const useExchanges = () => {
  const { isAuthenticated } = useAuth()

  return useQuery({
    queryKey: queryKeys.exchanges,
    queryFn: () => apiClient.getExchanges(),
    enabled: isAuthenticated,
    staleTime: 5 * 60 * 1000,
    throwOnError: false,
  })
}

export const useExchangeInstruments = (exchange: string | null) => {
  const { isAuthenticated } = useAuth()
  const exchangeKey = exchange ?? ''

  return useQuery({
    queryKey: queryKeys.exchangeInstruments(exchangeKey),
    queryFn: () => apiClient.getExchangeInstruments(exchangeKey),
    enabled: isAuthenticated && !!exchange,
    staleTime: 5 * 60 * 1000,
    throwOnError: false,
  })
}

export const useOrders = (filters?: { symbol?: string; limit?: number; offset?: number }) => {
  const { isAuthenticated } = useAuth()
  const updateOrders = useTradeStore(state => state.updateOrders)
  const selectOrders = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getOrders>>) =>
      data.map(safeOrderFromAPI).filter((o): o is NonNullable<typeof o> => o !== null),
    []
  )
  const query = useQuery({
    queryKey: queryKeys.orders(filters),
    queryFn: () => apiClient.getOrders(filters?.symbol, filters?.limit, filters?.offset),
    select: selectOrders,
    enabled: isAuthenticated,
    throwOnError: false,
  })

  useEffect(() => {
    if (query.data) {
      updateOrders(query.data)
    }
  }, [query.data, updateOrders])

  return query
}

export const useExecutions = (filters?: { limit?: number }) => {
  const { isAuthenticated } = useAuth()
  const updateExecutions = useTradeStore(state => state.updateExecutions)
  const selectExecutions = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getExecutions>>) =>
      data.map(safeExecutionFromAPI).filter((e): e is NonNullable<typeof e> => e !== null),
    []
  )
  const query = useQuery({
    queryKey: queryKeys.executions(filters),
    queryFn: () => apiClient.getExecutions(filters?.limit),
    select: selectExecutions,
    enabled: isAuthenticated,
    throwOnError: false,
  })

  useEffect(() => {
    if (query.data) {
      updateExecutions(query.data)
    }
  }, [query.data, updateExecutions])

  return query
}

const usePositions = () => {
  const { isAuthenticated } = useAuth()
  const updatePositions = useTradeStore(state => state.updatePositions)
  const query = useQuery({
    queryKey: queryKeys.positions,
    queryFn: () => apiClient.getPositions(),
    refetchInterval: isAuthenticated ? 10000 : false,
    enabled: isAuthenticated,
    throwOnError: false,
  })

  useEffect(() => {
    if (query.data) {
      updatePositions(query.data.map(positionFromAPI))
    }
  }, [query.data, updatePositions])

  return query
}

const useSignals = (
  strategyId: string | undefined,
  limit: number,
  instrument?: string,
  hours = 24
) => {
  const { isAuthenticated } = useAuth()
  const updateSignals = useTradeStore(state => state.updateSignals)
  const selectSignals = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getSignals>>) =>
      data.map(safeSignalFromAPI).filter((s): s is NonNullable<typeof s> => s !== null),
    []
  )
  const query = useQuery({
    queryKey: queryKeys.signals(strategyId, limit, instrument, hours),
    queryFn: () => apiClient.getSignals(strategyId, limit, instrument, hours),
    select: selectSignals,
    enabled: isAuthenticated,
    throwOnError: false,
  })

  useEffect(() => {
    if (query.data) {
      updateSignals(query.data)
    }
  }, [query.data, updateSignals])

  return query
}

export const useOrdersGrouped = (filters?: {
  symbol?: string
  limit?: number
  offset?: number
}) => {
  const { data: orders, ...rest } = useOrders(filters)
  const groupedData = React.useMemo(() => {
    if (!orders) return null

    return {
      new: orders.filter(o => o.status.toLowerCase() === 'new'),
      open: orders.filter(o => o.status.toLowerCase() === 'open'),
      filled: orders.filter(o => o.status.toLowerCase() === 'filled'),
      partially_filled: orders.filter(o => o.status.toLowerCase() === 'partially_filled'),
      cancelled: orders.filter(o => o.status.toLowerCase() === 'cancelled'),
      rejected: orders.filter(o => o.status.toLowerCase() === 'rejected'),
    }
  }, [orders])

  return {
    data: groupedData,
    orders,
    ...rest,
  }
}

export const usePositionsSummary = () => {
  const { data: positions, ...rest } = usePositions()
  const summary = React.useMemo(() => {
    if (!positions) return null
    const totalCost = positions.reduce((sum, p) => sum + p.quantity * p.average_price, 0)
    const totalPnL = positions.reduce((sum, p) => sum + p.unrealized_pnl + p.realized_pnl, 0)
    const totalValue = totalCost + totalPnL
    const pnlPercent = totalCost > 0 ? (totalPnL / totalCost) * 100 : 0

    return {
      count: positions.length,
      totalValue,
      totalPnL,
      totalCost,
      pnlPercent,
      instruments: [...new Set(positions.map(p => p.instrument))],
    }
  }, [positions])

  return {
    data: summary,
    positions,
    ...rest,
  }
}

export const useLatestSignals = (limit: number = 10) => {
  const { data: signals, ...rest } = useSignals(undefined, 50)
  const latestSignals = React.useMemo(() => {
    if (!signals) return []

    return signals
      .toSorted((a, b) => (b.timestamp?.getTime() ?? 0) - (a.timestamp?.getTime() ?? 0))
      .slice(0, limit)
  }, [signals, limit])

  return {
    data: latestSignals,
    ...rest,
  }
}

export const useStartProcessByName = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({
      name,
      mode,
      args,
      kwargs,
      autostart,
    }: {
      name: string
      mode?: 'thread' | 'process'
      args?: unknown[]
      kwargs?: Record<string, unknown>
      autostart?: boolean
    }) => apiClient.startProcessByName(name, { mode, args, kwargs, autostart }),
    retry: 2,
    retryDelay: attemptIndex => Math.min(1000 * 2 ** attemptIndex, 30000),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: queryKeys.processStatus })
      queryClient.invalidateQueries({ queryKey: ['process', 'runtime', variables.name] })
      queryClient.invalidateQueries({ queryKey: queryKeys.configuredProcesses })
      queryClient.invalidateQueries({ queryKey: queryKeys.availableProcesses })
      queryClient.invalidateQueries({ queryKey: queryKeys.processRuns() })
    },
  })
}

export const useStopProcessByName = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ name }: { name: string }) => apiClient.stopProcessByName(name),
    retry: 2,
    retryDelay: attemptIndex => Math.min(1000 * 2 ** attemptIndex, 30000),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: queryKeys.processStatus })
      queryClient.invalidateQueries({ queryKey: ['process', 'runtime', variables.name] })
      queryClient.invalidateQueries({ queryKey: queryKeys.configuredProcesses })
      queryClient.invalidateQueries({ queryKey: queryKeys.availableProcesses })
      queryClient.invalidateQueries({ queryKey: queryKeys.processRuns() })
    },
  })
}

export const useConfiguredProcesses = () => {
  return useQuery<ConfiguredProcessesResponse>({
    queryKey: queryKeys.configuredProcesses,
    queryFn: () => apiClient.getConfiguredProcesses(),
    refetchInterval: 5000,
  })
}

export const useAvailableProcesses = () => {
  return useQuery<AvailableProcessesResponse>({
    queryKey: queryKeys.availableProcesses,
    queryFn: () => apiClient.getAvailableProcesses(),
    staleTime: 5 * 60 * 1000,
  })
}

export const useProcessRuns = (options?: { name?: string; limit?: number; enabled?: boolean }) => {
  return useQuery<ProcessRunsResponse>({
    queryKey: queryKeys.processRuns(options?.name, options?.limit),
    queryFn: () => apiClient.getProcessRuns({ name: options?.name, limit: options?.limit }),
    refetchInterval: 5000,
    enabled: options?.enabled ?? true,
  })
}

export const useProcessSchema = (name: string, options?: { enabled?: boolean }) => {
  return useQuery<ProcessSchemaResponse>({
    queryKey: queryKeys.processSchema(name),
    queryFn: () => apiClient.getProcessSchema(name),
    enabled: options?.enabled ?? true,
    staleTime: 5 * 60 * 1000,
  })
}

export const useCreateProcessConfig = () => {
  const queryClient = useQueryClient()

  return useMutation<ProcessCreateResponse, Error, ProcessCreateRequest>({
    mutationFn: body => apiClient.createProcessConfig(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.configuredProcesses })
      queryClient.invalidateQueries({ queryKey: queryKeys.availableProcesses })
      queryClient.invalidateQueries({ queryKey: queryKeys.processRuns() })
    },
  })
}
