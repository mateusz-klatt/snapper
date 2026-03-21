import React, { useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from '../lib/apiClient'
import { useAuth } from '../stores/auth'
import {
  safeOrderFromAPI,
  safeExecutionFromAPI,
  safeSignalFromAPI,
  positionFromAPI,
} from '../lib/transforms'
import type {
  ConfiguredProcessesResponse,
  ProcessSummaryResponse,
  AvailableProcessesResponse,
  ProcessRunsResponse,
  ProcessSchemaResponse,
  ProcessCreateRequest,
  ProcessCreateResponse,
  StrategyListResponse,
  SettingResponse,
  SettingUpdate,
  UserListResponse,
  CreateUserRequest,
  UpdateUserRequest,
  AdminResetPasswordRequest,
} from '../types/api'

const queryKeys = {
  systemStatus: ['system', 'status'] as const,
  processStatus: ['process', 'status'] as const,
  availableProcesses: ['processes', 'available'] as const,
  configuredProcesses: ['processes', 'configured'] as const,
  processSummary: ['processes', 'summary'] as const,
  strategies: ['strategies'] as const,
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
  settings: (category?: string) => ['settings', category] as const,
  settingCategories: ['settings', 'categories'] as const,
  users: (includeInactive: boolean) => ['users', includeInactive] as const,
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
  const selectOrders = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getOrders>>) =>
      data.items.map(safeOrderFromAPI).filter((o): o is NonNullable<typeof o> => o !== null),
    []
  )

  return useQuery({
    queryKey: queryKeys.orders(filters),
    queryFn: () => apiClient.getOrders(filters?.symbol, filters?.limit, filters?.offset),
    select: selectOrders,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useExecutions = (filters?: { limit?: number }) => {
  const { isAuthenticated } = useAuth()
  const selectExecutions = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getExecutions>>) =>
      data.items.map(safeExecutionFromAPI).filter((e): e is NonNullable<typeof e> => e !== null),
    []
  )

  return useQuery({
    queryKey: queryKeys.executions(filters),
    queryFn: () => apiClient.getExecutions(filters?.limit),
    select: selectExecutions,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

const usePositions = () => {
  const { isAuthenticated } = useAuth()

  return useQuery({
    queryKey: queryKeys.positions,
    queryFn: async () => {
      const data = await apiClient.getPositions()

      return data.items.map(positionFromAPI)
    },
    refetchInterval: isAuthenticated ? 10000 : false,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useSignals = (
  strategyId: string | undefined,
  limit: number,
  instrument?: string,
  hours = 24
) => {
  const { isAuthenticated } = useAuth()
  const selectSignals = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getSignals>>) =>
      data.items.map(safeSignalFromAPI).filter((s): s is NonNullable<typeof s> => s !== null),
    []
  )

  return useQuery({
    queryKey: queryKeys.signals(strategyId, limit, instrument, hours),
    queryFn: () => apiClient.getSignals(strategyId, limit, instrument, hours),
    select: selectSignals,
    enabled: isAuthenticated,
    throwOnError: false,
  })
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
    const totalCost = positions.reduce((sum, p) => sum + p.quantity * p.averagePrice, 0)
    const totalPnL = positions.reduce((sum, p) => sum + p.unrealizedPnl + p.realizedPnl, 0)
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
      .toSorted((a, b) => (b.firedAt?.getTime() ?? 0) - (a.firedAt?.getTime() ?? 0))
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
      queryClient.invalidateQueries({ queryKey: queryKeys.processSummary })
      queryClient.invalidateQueries({ queryKey: queryKeys.strategies })
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
      queryClient.invalidateQueries({ queryKey: queryKeys.processSummary })
      queryClient.invalidateQueries({ queryKey: queryKeys.strategies })
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

export const useProcessSummary = () => {
  return useQuery<ProcessSummaryResponse>({
    queryKey: queryKeys.processSummary,
    queryFn: () => apiClient.getProcessSummary(),
    refetchInterval: 5000,
  })
}

export const useStrategies = () => {
  return useQuery<StrategyListResponse>({
    queryKey: queryKeys.strategies,
    queryFn: () => apiClient.getStrategies(),
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
      queryClient.invalidateQueries({ queryKey: queryKeys.processSummary })
      queryClient.invalidateQueries({ queryKey: queryKeys.strategies })
      queryClient.invalidateQueries({ queryKey: queryKeys.availableProcesses })
      queryClient.invalidateQueries({ queryKey: queryKeys.processRuns() })
    },
  })
}

export const useSettings = (category?: string) => {
  const { isAuthenticated } = useAuth()

  return useQuery({
    queryKey: queryKeys.settings(category),
    queryFn: () => apiClient.getSettings(category),
    select: data => data.items,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useSettingCategories = () => {
  const { isAuthenticated } = useAuth()

  return useQuery<string[]>({
    queryKey: queryKeys.settingCategories,
    queryFn: () => apiClient.getSettingCategories(),
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useUpdateSetting = () => {
  const queryClient = useQueryClient()

  return useMutation<SettingResponse, Error, { key: string; data: SettingUpdate }>({
    mutationFn: ({ key, data }) => apiClient.updateSetting(key, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings'] })
    },
  })
}

export const useDeleteSetting = () => {
  const queryClient = useQueryClient()

  return useMutation<{ message: string }, Error, string>({
    mutationFn: key => apiClient.deleteSetting(key),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings'] })
    },
  })
}

export const useUsers = (includeInactive: boolean) => {
  const { isAuthenticated } = useAuth()

  return useQuery<UserListResponse>({
    queryKey: queryKeys.users(includeInactive),
    queryFn: () => apiClient.listUsers(includeInactive),
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useCreateUser = () => {
  const queryClient = useQueryClient()

  return useMutation<{ message: string }, Error, CreateUserRequest>({
    mutationFn: data => apiClient.createUser(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
}

export const useUpdateUser = () => {
  const queryClient = useQueryClient()

  return useMutation<{ message: string }, Error, { userId: string; data: UpdateUserRequest }>({
    mutationFn: ({ userId, data }) => apiClient.updateUser(userId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
}

export const useDeactivateUser = () => {
  const queryClient = useQueryClient()

  return useMutation<{ message: string }, Error, string>({
    mutationFn: userId => apiClient.deactivateUser(userId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
}

export const useAdminResetPassword = () => {
  const queryClient = useQueryClient()

  return useMutation<
    { message: string },
    Error,
    { userId: string; data: AdminResetPasswordRequest }
  >({
    mutationFn: ({ userId, data }) => apiClient.adminResetPassword(userId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
}

export const useChangePassword = () =>
  useMutation<
    { message: string },
    Error,
    { userId: string; currentPassword: string; newPassword: string }
  >({
    mutationFn: ({ userId, currentPassword, newPassword }) =>
      apiClient.changePassword(userId, currentPassword, newPassword),
  })
