import React, { useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient, APIError } from '../lib/apiClient'
import { useAppStore } from '../stores/app'
import { useAuth } from '../stores/auth'
import {
  type AiReviewActivityFrame,
  AI_REVIEW_ACTIVITY_QUERY_KEY_ROOT,
  aiReviewActivityQueryKey,
} from '../stores/wsDispatcher'
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
  ExecutionPlanResponse,
  BracketCreateBody,
  TrailingStopCreateBody,
  BacktestCompareBody,
  DelegateCreateBody,
  DelegateCreatedResponse,
  DelegateResponse,
  DelegateCapsUpdateBody,
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
  exchangeInstrumentsDetail: (exchange: string, asOf: string | null) =>
    ['exchanges', exchange, 'instruments', 'detail', asOf] as const,
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
  featureFlags: () => ['feature-flags'] as const,
  aiDelegates: () => ['ai-delegates'] as const,
  aiDelegate: (publicId: string) => ['ai-delegates', publicId] as const,
  pendingAiReviews: (
    userPublicId: string | null,
    walletPublicId: string | null,
    limit: number | null
  ) => ['ai-reviews', 'pending', userPublicId, walletPublicId, limit] as const,
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

export const useSystemMetrics = () => {
  const { isAuthenticated } = useAuth()
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)

  return useQuery({
    queryKey: ['system', 'metrics'] as const,
    queryFn: () => apiClient.getSystemMetrics(),
    refetchInterval: isAuthenticated && !isTimeTraveling ? 10000 : false,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useDbStats = () => {
  const { isAuthenticated } = useAuth()
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)

  return useQuery({
    queryKey: ['system', 'db-stats'] as const,
    queryFn: () => apiClient.getDbStats(),
    refetchInterval: isAuthenticated && !isTimeTraveling ? 30000 : false,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useNotificationMetrics = () => {
  const { isAuthenticated } = useAuth()
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)

  return useQuery({
    queryKey: ['system', 'notification-metrics'] as const,
    queryFn: () => apiClient.getNotificationMetrics(),
    refetchInterval: isAuthenticated && !isTimeTraveling ? 30000 : false,
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useRetentionRun = () => {
  const { isAuthenticated } = useAuth()
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)

  return useQuery({
    queryKey: ['system', 'retention'] as const,
    queryFn: () => apiClient.getRetentionRun(),
    refetchInterval: isAuthenticated && !isTimeTraveling ? 60000 : false,
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

export const useExchangeInstrumentsDetail = (exchange: string | null) => {
  const { isAuthenticated } = useAuth()
  const asOf = useAppStore(s => s.asOf)
  const exchangeKey = exchange ?? ''

  return useQuery({
    queryKey: queryKeys.exchangeInstrumentsDetail(exchangeKey, asOf),
    queryFn: () => apiClient.getExchangeInstrumentsDetail(exchangeKey),
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

export const useCreateOrder = () => {
  const queryClient = useQueryClient()

  return useMutation<Record<string, unknown>, Error, Record<string, unknown>>({
    mutationFn: body => apiClient.createOrder(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['orders'] })
    },
  })
}

export const useCancelOrder = () => {
  const queryClient = useQueryClient()

  return useMutation<Record<string, unknown>, Error, string>({
    mutationFn: clientOrderId => apiClient.cancelOrder(clientOrderId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['orders'] })
    },
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

export const useCreateBracket = () => {
  const queryClient = useQueryClient()

  return useMutation<ExecutionPlanResponse, Error, BracketCreateBody>({
    mutationFn: body => apiClient.createBracket(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['positions'] })
      queryClient.invalidateQueries({ queryKey: ['orders'] })
    },
  })
}

export const useCreateTrailingStop = () => {
  const queryClient = useQueryClient()

  return useMutation<ExecutionPlanResponse, Error, TrailingStopCreateBody>({
    mutationFn: body => apiClient.createTrailingStop(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['positions'] })
      queryClient.invalidateQueries({ queryKey: ['orders'] })
      queryClient.invalidateQueries({ queryKey: ['trailingStopState'] })
    },
  })
}

export const useTrailingStopForCycle = (cyclePublicId: string | undefined) => {
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)

  return useQuery({
    queryKey: ['trailingStopState', cyclePublicId],
    queryFn: () => apiClient.getTrailingStopByCycle(cyclePublicId as string),
    enabled: !!cyclePublicId && !isTimeTraveling,
    refetchInterval: 5000,
  })
}

export const useBacktests = (strategy?: string, status?: string) => {
  return useQuery({
    queryKey: ['backtests', strategy, status],
    queryFn: () => apiClient.getBacktests(50, 0, strategy, status),
  })
}

/**
 * Auto-pair candidate lookup — returns recent terminal runs sharing
 * the same pairing-stable `config_hash` as the current run. Used by
 * CompareLauncher to seed the manual-pair combobox and gate the
 * "compare with most recent" button.
 *
 * Disabled (enabled: false) when `configHash` is `null` — older runs
 * have no hash and auto-pair is not applicable.
 */
export const useBacktestRunsByConfigHash = (configHash: string | null, limit: number = 20) => {
  return useQuery({
    queryKey: ['backtests', 'by-hash', configHash, limit],
    queryFn: () => apiClient.getBacktests(limit, 0, undefined, undefined, configHash),
    enabled: configHash !== null,
  })
}

export const useBacktest = (runId: string | undefined) => {
  return useQuery({
    queryKey: ['backtests', runId],
    queryFn: () => apiClient.getBacktest(runId as string),
    enabled: !!runId,
  })
}

export const useBacktestTrades = (runId: string | undefined) => {
  return useQuery({
    queryKey: ['backtests', runId, 'trades'],
    queryFn: () => apiClient.getBacktestTrades(runId as string),
    enabled: !!runId,
  })
}

export const useBacktestSignals = (runId: string | undefined) => {
  return useQuery({
    queryKey: ['backtests', runId, 'signals'],
    queryFn: () => apiClient.getBacktestSignals(runId as string),
    enabled: !!runId,
  })
}

export const useCreateBacktest = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (body: Parameters<typeof apiClient.createBacktest>[0]) =>
      apiClient.createBacktest(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backtests'] })
    },
  })
}

export const useBacktestComparison = (comparisonId: string | undefined) => {
  const walletId = useAppStore(s => s.currentWalletPublicId)

  return useQuery({
    queryKey: ['backtest-compare', walletId, comparisonId],
    queryFn: () => apiClient.getBacktestComparison(comparisonId as string),
    enabled: !!comparisonId,
    retry: (failureCount, error) => {
      if (error instanceof APIError && error.status === 404) return false

      return failureCount < 3
    },
  })
}

export const useBacktestComparisons = (limit: number = 20, offset: number = 0) => {
  const walletId = useAppStore(s => s.currentWalletPublicId)

  return useQuery({
    queryKey: ['backtest-compare', 'list', walletId, limit, offset],
    queryFn: () => apiClient.getBacktestComparisons(limit, offset),
  })
}

export const useCreateBacktestComparison = () => {
  const queryClient = useQueryClient()
  const walletId = useAppStore(s => s.currentWalletPublicId)

  return useMutation({
    mutationFn: (body: BacktestCompareBody) => apiClient.createBacktestComparison(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backtest-compare', 'list', walletId] })
    },
  })
}

export const useCancelBacktest = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (runId: string) => apiClient.cancelBacktest(runId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backtests'] })
    },
  })
}

export const useRerunBacktest = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (runId: string) => apiClient.rerunBacktest(runId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backtests'] })
    },
  })
}

export function useFeatureFlags(): { isEnabled: boolean; isLoading: boolean } {
  const { isAuthenticated } = useAuth()
  const query = useQuery({
    queryKey: queryKeys.featureFlags(),
    queryFn: () => apiClient.getFeatureFlags(),
    enabled: isAuthenticated,
    throwOnError: false,
    select: data => data.payload.ai_integration_enabled === true,
  })

  return {
    isEnabled: query.data === true,
    isLoading: query.isLoading,
  }
}

export const useAiDelegates = () => {
  const { isAuthenticated } = useAuth()

  return useQuery({
    queryKey: queryKeys.aiDelegates(),
    queryFn: () => apiClient.listAiDelegates(),
    enabled: isAuthenticated,
    throwOnError: false,
  })
}

export const useAiDelegate = (publicId: string | null) => {
  const { isAuthenticated } = useAuth()

  return useQuery({
    queryKey: queryKeys.aiDelegate(publicId ?? ''),
    queryFn: () => apiClient.getAiDelegate(publicId as string),
    enabled: isAuthenticated && !!publicId,
    throwOnError: false,
  })
}

export const useCreateAiDelegate = () => {
  const queryClient = useQueryClient()

  return useMutation<DelegateCreatedResponse, Error, DelegateCreateBody>({
    mutationFn: body => apiClient.createAiDelegate(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.aiDelegates() })
    },
    gcTime: 0,
  })
}

export const useUpdateAiDelegateCaps = () => {
  const queryClient = useQueryClient()

  return useMutation<DelegateResponse, Error, { publicId: string; body: DelegateCapsUpdateBody }>({
    mutationFn: ({ publicId, body }) => apiClient.updateAiDelegateCaps(publicId, body),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: queryKeys.aiDelegates() })
      queryClient.invalidateQueries({ queryKey: queryKeys.aiDelegate(variables.publicId) })
    },
  })
}

export const useDeactivateAiDelegate = () => {
  const queryClient = useQueryClient()

  return useMutation<DelegateResponse, Error, string>({
    mutationFn: publicId => apiClient.deactivateAiDelegate(publicId),
    onSuccess: (_data, publicId) => {
      queryClient.invalidateQueries({ queryKey: queryKeys.aiDelegates() })
      queryClient.invalidateQueries({ queryKey: queryKeys.aiDelegate(publicId) })
    },
  })
}

const PENDING_AI_REVIEWS_REFETCH_MS = 5_000

/**
 * List pending CONSULT reviews for the authenticated AI delegate.
 *
 * Gated by ``role === 'ai_delegate'`` because the REST endpoint
 * (`GET /api/ai-reviews/pending`) returns 422 for any other role:
 * the snapshot is keyed by ``AuthPrincipal.delegate_public_id`` which
 * is only populated for delegate principals. Pre-empting the call
 * client-side keeps non-delegate UIs free of misleading 422 errors.
 *
 * Refetches every 5s plus on window focus so the inbox stays
 * eventually consistent with the WS-driven activity stream after
 * disconnect / reconnect.
 */
export const usePendingAiReviews = (
  params: Readonly<{ walletPublicId?: string | null; limit?: number }> = {}
) => {
  const { isAuthenticated, user } = useAuth()
  const walletPublicId = params.walletPublicId ?? null
  const limit = params.limit ?? null
  const isDelegate = user?.role === 'ai_delegate'
  const userPublicId = user?.public_id ?? null

  return useQuery({
    queryKey: queryKeys.pendingAiReviews(userPublicId, walletPublicId, limit),
    queryFn: () =>
      apiClient.listPendingAiReviews({
        wallet_public_id: walletPublicId ?? undefined,
        limit: limit ?? undefined,
      }),
    enabled: isAuthenticated && isDelegate && userPublicId !== null,
    refetchInterval: PENDING_AI_REVIEWS_REFETCH_MS,
    refetchOnWindowFocus: true,
    throwOnError: false,
  })
}

/**
 * Read-only view onto the WS-driven AI review activity ring buffer
 * maintained by :class:`WSDispatcher`.
 *
 * The dispatcher merges :data:`AiReviewRequestFrameData`,
 * :data:`AiReviewDecisionAckFrameData`, and
 * :data:`AiReviewCapsViolationFrameData` envelopes into the
 * ``['ai-review-activity']`` cache deduped by
 * ``(type, review_public_id, dispatch_version)`` and capped at
 * :data:`AI_REVIEW_ACTIVITY_RING_CAP`.
 *
 * Unlike :func:`usePendingAiReviews` this hook does not gate by role
 * because non-delegate sockets receive zero ai_reviews frames anyway
 * (the WS scope filter at ``snapper.interface.websocket.scope_filter``
 * drops them), so the cache stays empty for non-delegates without
 * extra logic here.
 */
export const useAiReviewActivity = () => {
  const queryClient = useQueryClient()
  const { user } = useAuth()
  const userPublicId = user?.public_id ?? null
  const queryKey = React.useMemo(() => aiReviewActivityQueryKey(userPublicId), [userPublicId])
  const subscribe = React.useCallback(
    (notify: () => void) => {
      const unsubscribe = queryClient.getQueryCache().subscribe(event => {
        const eventKey = event.query.queryKey

        if (eventKey[0] === AI_REVIEW_ACTIVITY_QUERY_KEY_ROOT && eventKey[1] === userPublicId) {
          notify()
        }
      })

      return unsubscribe
    },
    [queryClient, userPublicId]
  )
  const getSnapshot = React.useCallback(
    () =>
      (queryClient.getQueryData(queryKey) ??
        EMPTY_AI_REVIEW_ACTIVITY) as readonly AiReviewActivityFrame[],
    [queryClient, queryKey]
  )

  return React.useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}

const EMPTY_AI_REVIEW_ACTIVITY: readonly AiReviewActivityFrame[] = []
