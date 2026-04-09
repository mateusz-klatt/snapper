import React, { useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from '../lib/apiClient'
import { useAppStore } from '../stores/app'
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
  ProcessCreateBody,
  ProcessCreateResponse,
  StrategyListResponse,
  SettingResponse,
  SettingUpdateBody,
  UserListResponse,
  UserResponse,
  CreateUserBody,
  UpdateUserBody,
  AdminResetPasswordBody,
  ScopeGrantListResponse,
  ScopeGrantResponse,
  CreateScopeGrantBody,
  HandoverScopeGrantBody,
  HandoverScopeGrantResponse,
  CredentialListResponse,
  CredentialResponse,
  CreateCredentialBody,
  RotateCredentialBody,
} from '../types/api'

const queryKeys = {
  systemStatus: ['system', 'status'] as const,
  processStatus: ['process', 'status'] as const,
  availableProcesses: ['processes', 'available'] as const,
  configuredProcesses: (asOf: string | null) => ['processes', 'configured', asOf] as const,
  processSummary: (asOf: string | null) => ['processes', 'summary', asOf] as const,
  strategies: (asOf: string | null) => ['strategies', asOf] as const,
  processSchema: (name: string) => ['processes', 'schema', name] as const,
  processRuns: (name?: string, limit?: number, asOf?: string | null) =>
    ['processes', 'runs', name ?? 'all', limit ?? 50, asOf] as const,
  candles: (instrument: string, exchange: string, timeframe: string, asOf: string | null) =>
    ['candles', instrument, exchange, timeframe, asOf] as const,
  exchanges: (asOf: string | null) => ['exchanges', asOf] as const,
  exchangeInstruments: (exchange: string, asOf: string | null) =>
    ['exchanges', exchange, 'instruments', asOf] as const,
  orders: (
    filters?: { symbol?: string; limit?: number; offset?: number },
    asOf?: string | null,
    opId?: string | null,
    walletId?: string | null
  ) => ['orders', filters, asOf, opId, walletId] as const,
  executions: (
    filters?: { limit?: number },
    asOf?: string | null,
    opId?: string | null,
    walletId?: string | null
  ) => ['executions', filters, asOf, opId, walletId] as const,
  positions: (asOf: string | null, opId?: string | null, walletId?: string | null) =>
    ['positions', asOf, opId, walletId] as const,
  signals: (
    strategyId?: string,
    limit?: number,
    instrument?: string,
    hours?: number,
    asOf?: string | null,
    opId?: string | null,
    walletId?: string | null
  ) => ['signals', strategyId, limit, instrument, hours, asOf, opId, walletId] as const,
  operators: (asOf: string | null) => ['operators', asOf] as const,
  wallets: (asOf: string | null, opId?: string | null) => ['wallets', asOf, opId] as const,
  scopeGrants: (walletPublicId: string, asOf: string | null) =>
    ['scope-grants', walletPublicId, asOf] as const,
  credentials: (walletPublicId: string, asOf: string | null) =>
    ['credentials', walletPublicId, asOf] as const,
  settings: (category?: string, asOf?: string | null) => ['settings', category, asOf] as const,
  settingCategories: (asOf: string | null) => ['settings', 'categories', asOf] as const,
  users: (includeInactive: boolean, asOf: string | null) =>
    ['users', includeInactive, asOf] as const,
}

export const useSystemStatus = () => {
  const { isAuthenticated } = useAuth()
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)

  return useQuery({
    queryKey: queryKeys.systemStatus,
    queryFn: () => apiClient.getSystemStatus(),
    refetchInterval: isAuthenticated && !isTimeTraveling ? 30000 : false,
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
  const asOf = useAppStore(s => s.asOf)

  return useQuery({
    queryKey: queryKeys.candles(instrument, exchange, timeframe, asOf),
    queryFn: () => apiClient.getCandles(instrument, exchange, timeframe, limit),
    enabled: enabled && !!instrument && !!exchange && isAuthenticated,
    staleTime: 2000,
    throwOnError: false,
    retry: 2,
  })
}

export const useExchanges = () => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)

  return useQuery({
    queryKey: queryKeys.exchanges(asOf),
    queryFn: () => apiClient.getExchanges(),
    enabled: isAuthenticated,
    staleTime: 5 * 60 * 1000,
    throwOnError: false,
  })
}

export const useExchangeInstruments = (exchange: string | null) => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)
  const exchangeKey = exchange ?? ''

  return useQuery({
    queryKey: queryKeys.exchangeInstruments(exchangeKey, asOf),
    queryFn: () => apiClient.getExchangeInstruments(exchangeKey),
    enabled: isAuthenticated && !!exchange,
    staleTime: 5 * 60 * 1000,
    throwOnError: false,
  })
}

export const useOperators = () => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)

  return useQuery({
    queryKey: queryKeys.operators(asOf),
    queryFn: () => apiClient.getOperators(),
    enabled: isAuthenticated,
    staleTime: 60 * 1000,
    throwOnError: false,
  })
}

export const useWallets = () => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)
  const opId = useAppStore(s => s.currentOperatorPublicId)

  return useQuery({
    queryKey: queryKeys.wallets(asOf, opId),
    queryFn: () => apiClient.getWallets(),
    enabled: isAuthenticated,
    staleTime: 60 * 1000,
    throwOnError: false,
  })
}

export const useScopeGrants = (walletPublicId: string) => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)

  return useQuery<ScopeGrantListResponse>({
    queryKey: queryKeys.scopeGrants(walletPublicId, asOf),
    queryFn: () => apiClient.getScopeGrants(walletPublicId),
    enabled: isAuthenticated && !!walletPublicId,
    staleTime: 30 * 1000,
    throwOnError: false,
  })
}

export const useCreateScopeGrant = () => {
  const queryClient = useQueryClient()

  return useMutation<ScopeGrantResponse, Error, CreateScopeGrantBody>({
    mutationFn: data => apiClient.createScopeGrant(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scope-grants'] })
    },
  })
}

export const useHandoverScopeGrant = () => {
  const queryClient = useQueryClient()

  return useMutation<HandoverScopeGrantResponse, Error, HandoverScopeGrantBody>({
    mutationFn: data => apiClient.handoverScopeGrant(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scope-grants'] })
    },
  })
}

export const useCredentials = (walletPublicId: string) => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)

  return useQuery<CredentialListResponse>({
    queryKey: queryKeys.credentials(walletPublicId, asOf),
    queryFn: () => apiClient.getCredentials(walletPublicId),
    enabled: isAuthenticated && !!walletPublicId,
    staleTime: 30 * 1000,
    throwOnError: false,
  })
}

export const useCreateCredential = () => {
  const queryClient = useQueryClient()

  return useMutation<
    CredentialResponse,
    Error,
    { walletPublicId: string; data: CreateCredentialBody }
  >({
    mutationFn: ({ walletPublicId, data }) => apiClient.createCredential(walletPublicId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

export const useRotateCredential = () => {
  const queryClient = useQueryClient()

  return useMutation<
    CredentialResponse,
    Error,
    { walletPublicId: string; credentialPublicId: string; data: RotateCredentialBody }
  >({
    mutationFn: ({ walletPublicId, credentialPublicId, data }) =>
      apiClient.rotateCredential(walletPublicId, credentialPublicId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

export const useOrders = (filters?: { symbol?: string; limit?: number; offset?: number }) => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)
  const opId = useAppStore(s => s.currentOperatorPublicId)
  const walletId = useAppStore(s => s.currentWalletPublicId)
  const selectOrders = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getOrders>>) =>
      data.payload.map(safeOrderFromAPI).filter((o): o is NonNullable<typeof o> => o !== null),
    []
  )

  return useQuery({
    queryKey: queryKeys.orders(filters, asOf, opId, walletId),
    queryFn: () => apiClient.getOrders(filters?.symbol, filters?.limit, filters?.offset),
    select: selectOrders,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useExecutions = (filters?: { limit?: number }) => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)
  const opId = useAppStore(s => s.currentOperatorPublicId)
  const walletId = useAppStore(s => s.currentWalletPublicId)
  const selectExecutions = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getExecutions>>) =>
      data.payload.map(safeExecutionFromAPI).filter((e): e is NonNullable<typeof e> => e !== null),
    []
  )

  return useQuery({
    queryKey: queryKeys.executions(filters, asOf, opId, walletId),
    queryFn: () => apiClient.getExecutions(filters?.limit),
    select: selectExecutions,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const usePositions = () => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)
  const opId = useAppStore(s => s.currentOperatorPublicId)
  const walletId = useAppStore(s => s.currentWalletPublicId)

  return useQuery({
    queryKey: queryKeys.positions(asOf, opId, walletId),
    queryFn: async () => {
      const data = await apiClient.getPositions()

      return data.payload.map(positionFromAPI)
    },
    refetchInterval: isAuthenticated && !asOf ? 10000 : false,
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
  const asOf = useAppStore(s => s.asOf)
  const opId = useAppStore(s => s.currentOperatorPublicId)
  const walletId = useAppStore(s => s.currentWalletPublicId)
  const selectSignals = useCallback(
    (data: Awaited<ReturnType<typeof apiClient.getSignals>>) =>
      data.payload.map(safeSignalFromAPI).filter((s): s is NonNullable<typeof s> => s !== null),
    []
  )

  return useQuery({
    queryKey: queryKeys.signals(strategyId, limit, instrument, hours, asOf, opId, walletId),
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
    const longCost = positions
      .filter(p => p.quantity > 0)
      .reduce((sum, p) => sum + p.quantity * p.averagePrice, 0)
    const shortCost = positions
      .filter(p => p.quantity < 0)
      .reduce((sum, p) => sum + Math.abs(p.quantity) * p.averagePrice, 0)
    const totalExposure = longCost + shortCost
    const totalPnL = positions.reduce((sum, p) => sum + p.unrealizedPnl + p.realizedPnl, 0)
    const totalValue = totalExposure + totalPnL
    const pnlPercent = totalExposure > 0 ? (totalPnL / totalExposure) * 100 : 0

    return {
      count: positions.length,
      totalValue,
      totalPnL,
      longCost,
      shortCost,
      totalExposure,
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
      parameters,
    }: {
      name: string
      mode?: 'thread' | 'process'
      parameters?: Record<string, unknown>
    }) => apiClient.startProcessByName(name, { mode, parameters }),
    retry: 2,
    retryDelay: attemptIndex => Math.min(1000 * 2 ** attemptIndex, 30000),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: queryKeys.processStatus })
      queryClient.invalidateQueries({ queryKey: ['process', 'runtime', variables.name] })
      queryClient.invalidateQueries({ queryKey: ['processes', 'configured'] })
      queryClient.invalidateQueries({ queryKey: ['processes', 'summary'] })
      queryClient.invalidateQueries({ queryKey: ['strategies'] })
      queryClient.invalidateQueries({ queryKey: queryKeys.availableProcesses })
      queryClient.invalidateQueries({ queryKey: ['processes', 'runs'] })
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
      queryClient.invalidateQueries({ queryKey: ['processes', 'configured'] })
      queryClient.invalidateQueries({ queryKey: ['processes', 'summary'] })
      queryClient.invalidateQueries({ queryKey: ['strategies'] })
      queryClient.invalidateQueries({ queryKey: queryKeys.availableProcesses })
      queryClient.invalidateQueries({ queryKey: ['processes', 'runs'] })
    },
  })
}

export const useConfiguredProcesses = () => {
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const asOf = useAppStore(s => s.asOf)

  return useQuery<ConfiguredProcessesResponse>({
    queryKey: queryKeys.configuredProcesses(asOf),
    queryFn: () => apiClient.getConfiguredProcesses(),
    refetchInterval: isTimeTraveling ? false : 5000,
  })
}

export const useProcessSummary = () => {
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const asOf = useAppStore(s => s.asOf)

  return useQuery<ProcessSummaryResponse>({
    queryKey: queryKeys.processSummary(asOf),
    queryFn: () => apiClient.getProcessSummary(),
    refetchInterval: isTimeTraveling ? false : 5000,
  })
}

export const useStrategies = () => {
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const asOf = useAppStore(s => s.asOf)

  return useQuery<StrategyListResponse>({
    queryKey: queryKeys.strategies(asOf),
    queryFn: () => apiClient.getStrategies(),
    refetchInterval: isTimeTraveling ? false : 5000,
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
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const asOf = useAppStore(s => s.asOf)

  return useQuery<ProcessRunsResponse>({
    queryKey: queryKeys.processRuns(options?.name, options?.limit, asOf),
    queryFn: () => apiClient.getProcessRuns({ name: options?.name, limit: options?.limit }),
    refetchInterval: isTimeTraveling ? false : 5000,
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

  return useMutation<ProcessCreateResponse, Error, ProcessCreateBody>({
    mutationFn: body => apiClient.createProcessConfig(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['processes', 'configured'] })
      queryClient.invalidateQueries({ queryKey: ['processes', 'summary'] })
      queryClient.invalidateQueries({ queryKey: ['strategies'] })
      queryClient.invalidateQueries({ queryKey: queryKeys.availableProcesses })
      queryClient.invalidateQueries({ queryKey: ['processes', 'runs'] })
    },
  })
}

export const useSettings = (category?: string) => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)

  return useQuery({
    queryKey: queryKeys.settings(category, asOf),
    queryFn: () => apiClient.getSettings(category),
    select: data => data.payload,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useSettingCategories = () => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)

  return useQuery<string[]>({
    queryKey: queryKeys.settingCategories(asOf),
    queryFn: () => apiClient.getSettingCategories(),
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useUpdateSetting = () => {
  const queryClient = useQueryClient()

  return useMutation<SettingResponse, Error, { key: string; data: SettingUpdateBody }>({
    mutationFn: ({ key, data }) => apiClient.updateSetting(key, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings'] })
    },
  })
}

export const useDeleteSetting = () => {
  const queryClient = useQueryClient()

  return useMutation<{ payload: string }, Error, string>({
    mutationFn: key => apiClient.removeSetting(key),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings'] })
    },
  })
}

export const useUsers = (includeInactive: boolean) => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)

  return useQuery<UserListResponse>({
    queryKey: queryKeys.users(includeInactive, asOf),
    queryFn: () => apiClient.listUsers(includeInactive),
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useCreateUser = () => {
  const queryClient = useQueryClient()

  return useMutation<UserResponse, Error, CreateUserBody>({
    mutationFn: data => apiClient.createUser(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
}

export const useUpdateUser = () => {
  const queryClient = useQueryClient()

  return useMutation<UserResponse, Error, { userId: string; data: UpdateUserBody }>({
    mutationFn: ({ userId, data }) => apiClient.updateUser(userId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
}

export const useDeactivateUser = () => {
  const queryClient = useQueryClient()

  return useMutation<{ payload: string }, Error, string>({
    mutationFn: userId => apiClient.deactivateUser(userId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
}

export const useAdminResetPassword = () => {
  const queryClient = useQueryClient()

  return useMutation<{ payload: string }, Error, { userId: string; data: AdminResetPasswordBody }>({
    mutationFn: ({ userId, data }) => apiClient.adminResetPassword(userId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
}

export const useChangePassword = () =>
  useMutation<
    { payload: string },
    Error,
    { userId: string; currentPassword: string; newPassword: string }
  >({
    mutationFn: ({ userId, currentPassword, newPassword }) =>
      apiClient.changePassword(userId, currentPassword, newPassword),
  })
