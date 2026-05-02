import { v7 as uuid7 } from 'uuid'
import { getCookie } from './utils'
import { storeWsTicket } from './wsTicketCache'
import { validateResponse } from './schemas/api'
import { getTracker } from './sequenceTracker'
import {
  ScopeGrantListResponseSchema,
  ScopeGrantResponseSchema,
  HandoverScopeGrantResponseSchema,
  CredentialListResponseSchema,
  CredentialResponseSchema,
  OperatorListResponseSchema,
  WalletListResponseSchema,
  SettingCategoriesResponseSchema,
  SettingListResponseSchema,
  SettingResponseSchema,
  SystemMetricsResponseSchema,
  DbStatsResponseSchema,
  NotificationMetricsResponseSchema,
  RetentionRunResponseSchema,
  SystemStatusResponseSchema,
  ConfiguredProcessesResponseSchema,
  ProcessSummaryResponseSchema,
  AvailableProcessesResponseSchema,
  ProcessRunsResponseSchema,
  ProcessSchemaResponseSchema,
  ProcessCreateResponseSchema,
  ProcessStartResponseSchema,
  ProcessStopResponseSchema,
  StrategyListResponseSchema,
  MessageResponseSchema,
  HealthCheckResponseSchema,
  UserListResponseSchema,
  UserResponseSchema,
  ExchangeListResponseSchema,
  InstrumentDetailListResponseSchema,
  InstrumentListResponseSchema,
  OrderListResponseSchema,
  ExecutionListResponseSchema,
  PositionListResponseSchema,
  SignalListResponseSchema,
  ExecutionPlanResponseSchema,
  FeatureFlagsResponseSchema,
  DelegateListResponseSchema,
  DelegateResponseSchema,
  DelegateCreatedResponseSchema,
  PendingReviewListResponseSchema,
} from './schemas/api.generated.zod'
import type { PendingReviewListResponse } from './schemas/api.generated.zod'
import type {
  ScopeGrantListResponse,
  ScopeGrantResponse,
  HandoverScopeGrantResponse,
  CreateScopeGrantBody,
  HandoverScopeGrantBody,
  CredentialListResponse,
  CredentialResponse,
  CreateCredentialBody,
  RotateCredentialBody,
  OperatorListResponse,
  WalletListResponse,
  CandleData,
  SystemMetricsResponse,
  DbStatsResponse,
  NotificationMetricsResponse,
  RetentionRunResponse,
  SystemStatusResponse,
  SettingListResponse,
  SettingResponse,
  SettingUpdateBody,
  ConfiguredProcessesResponse,
  ProcessSummaryResponse,
  AvailableProcessesResponse,
  ProcessRunsResponse,
  ProcessSchemaResponse,
  ProcessCreateBody,
  ProcessCreateResponse,
  ProcessStartBody,
  ProcessStartResponse,
  ProcessStopResponse,
  StrategyListResponse,
  ChangePasswordBody,
  UserListResponse,
  UserResponse,
  CreateUserBody,
  UpdateUserBody,
  AdminResetPasswordBody,
  ExchangeListResponse,
  InstrumentDetailListResponse,
  InstrumentListResponse,
  OrderListResponse,
  ExecutionListResponse,
  PositionListResponse,
  SignalListResponse,
  HealthCheckResponse,
  ExecutionPlanResponse,
  BracketCreateBody,
  BracketCancelBody,
  TrailingStopCreateBody,
  TrailingStopCancelBody,
  TrailingStopByCycleResult,
  BacktestRunListResponse,
  BacktestRunResponse,
  BacktestTradeListResponse,
  BacktestSignalListResponse,
  BacktestCreateBody,
  BacktestCompareBody,
  BacktestComparisonResponse,
  BacktestComparisonDetailResponse,
  BacktestComparisonListResponse,
  FeatureFlagsResponse,
  DelegateListResponse,
  DelegateResponse,
  DelegateCreatedResponse,
  DelegateCreateBody,
  DelegateCapsUpdateBody,
} from '../types/api'

interface RequestOptions {
  skipCSRF?: boolean
  skipRetry?: boolean
  method?: string
  headers?: Record<string, string> | Headers
  body?: string | FormData | URLSearchParams | null
}

interface ErrorPayload {
  message: string
  details?: unknown
}

const MUTATING_METHODS = new Set(['POST', 'PUT', 'DELETE', 'PATCH'])

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object'
}

function fallbackErrorMessage(response: Pick<Response, 'status' | 'statusText'>): string {
  return `HTTP ${response.status}: ${response.statusText}`
}

function stringifyErrorValue(value: unknown, fallbackMessage: string): string {
  if (typeof value === 'string') {
    return value
  }

  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value)
  }

  if (value === null || value === undefined) {
    return fallbackMessage
  }

  if (typeof value === 'object') {
    try {
      const serialized = JSON.stringify(value)

      if (serialized !== undefined) {
        return serialized
      }
    } catch {
      return fallbackMessage
    }
  }

  return fallbackMessage
}

function extractArrayDetail(detail: unknown[], fallbackMessage: string): ErrorPayload {
  const first = detail[0]

  if (isRecord(first) && Object.hasOwn(first, 'msg')) {
    return {
      message: stringifyErrorValue(first.msg, fallbackMessage),
      details: detail,
    }
  }

  return { message: fallbackMessage, details: detail }
}

function extractObjectDetail(
  detail: Record<string, unknown>,
  fallbackMessage: string
): ErrorPayload {
  const reason = detail.reason

  if (typeof reason === 'string' && reason.length > 0) {
    return { message: reason, details: detail }
  }

  const errorCode = detail.error_code

  if (typeof errorCode === 'string' && errorCode.length > 0) {
    return { message: errorCode, details: detail }
  }

  return { message: fallbackMessage, details: detail }
}

function extractDetailPayload(detail: unknown, fallbackMessage: string): ErrorPayload {
  if (detail === null) {
    return { message: fallbackMessage }
  }

  if (Array.isArray(detail)) {
    return extractArrayDetail(detail, fallbackMessage)
  }

  if (isRecord(detail)) {
    return extractObjectDetail(detail, fallbackMessage)
  }

  return { message: stringifyErrorValue(detail, fallbackMessage) }
}

function extractErrorPayload(data: unknown, fallbackMessage: string): ErrorPayload {
  if (!isRecord(data)) {
    return { message: fallbackMessage }
  }

  if (Object.hasOwn(data, 'detail')) {
    return extractDetailPayload(data.detail, fallbackMessage)
  }

  if (Object.hasOwn(data, 'message')) {
    return { message: stringifyErrorValue(data.message, fallbackMessage) }
  }

  return { message: fallbackMessage }
}

/**
 * HTTP error thrown by getJSON/postJSON when the response is not OK.
 *
 * Preserves the response status + statusText so callers (notably
 * ComparePage) can branch on `error instanceof APIError && error.status === 404`
 * for wallet-scope misses, while existing `.message`-based assertions
 * keep working because APIError extends Error and reuses the same
 * message format.
 */
export class APIError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly statusText: string,
    public readonly details?: unknown
  ) {
    super(message)
    this.name = 'APIError'
  }
}

class APIClient {
  private static instance: APIClient
  private isLoggingOut = false
  private timeTravelAsOf: string | null = null
  private operatorPublicId: string | null = null
  private walletPublicId: string | null = null
  private constructor() {}
  public setTimeTravelAsOf(asOf: string | null): void {
    this.timeTravelAsOf = asOf
  }
  public getTimeTravelAsOf(): string | null {
    return this.timeTravelAsOf
  }
  public setOperatorScope(id: string | null): void {
    this.operatorPublicId = id
  }
  public setWalletScope(id: string | null): void {
    this.walletPublicId = id
  }
  public getOperatorScope(): string | null {
    return this.operatorPublicId
  }
  public getWalletScope(): string | null {
    return this.walletPublicId
  }
  public static getInstance(): APIClient {
    if (!APIClient.instance) {
      APIClient.instance = new APIClient()
    }

    return APIClient.instance
  }
  private getCSRFToken(): string {
    return getCookie('csrf_token')
  }
  private applyCSRFHeader(headers: Headers, skipCSRF: boolean, method: string | undefined): void {
    if (skipCSRF) {
      return
    }

    if (!MUTATING_METHODS.has(method?.toUpperCase() || 'GET')) {
      return
    }

    const csrfToken = this.getCSRFToken()

    if (csrfToken) {
      headers.set('X-CSRF-Token', csrfToken)
    }
  }
  private async refreshAndRetry(url: string, options: RequestOptions): Promise<Response> {
    try {
      const csrfToken = this.getCSRFToken()
      const refreshResponse = await fetch('/api/auth/refresh', {
        method: 'POST',
        credentials: 'include',
        headers: {
          'X-CSRF-Token': csrfToken,
          'Content-Type': 'application/json',
        },
      })

      if (refreshResponse.ok) {
        await cacheWsTicketFromResponse(refreshResponse)

        return this.request(url, { ...options, skipRetry: true })
      }

      this.handleAuthenticationFailure()
      throw new Error('Authentication required')
    } catch {
      this.handleAuthenticationFailure()
      throw new Error('Authentication required')
    }
  }
  private async handleCSRFError(
    url: string,
    options: RequestOptions,
    response: Response
  ): Promise<Response> {
    const errorData = await response
      .clone()
      .json()
      .catch(() => ({}))

    if (errorData.detail?.includes('CSRF')) {
      return this.request(url, { ...options, skipRetry: true, skipCSRF: true })
    }

    throw new Error('Access denied')
  }
  public async request(url: string, options: RequestOptions = {}): Promise<Response> {
    const method = (options.method ?? 'GET').toUpperCase()

    if (
      this.timeTravelAsOf &&
      MUTATING_METHODS.has(method) &&
      !APIClient.AUTH_PATHS.has(url.split('?')[0])
    ) {
      throw new Error('Write operations are disabled in time-travel mode')
    }

    const { skipCSRF = false, skipRetry = false, ...fetchOptions } = options
    const headers = new Headers(fetchOptions.headers)

    this.applyCSRFHeader(headers, skipCSRF, options.method)

    if (fetchOptions.body && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json')
    }

    const response = await fetch(url, {
      ...fetchOptions,
      headers,
      credentials: 'include',
    })

    if (response.status === 401 && !skipRetry) {
      return this.refreshAndRetry(url, options)
    }

    if (response.status === 403 && !skipCSRF && !skipRetry) {
      return this.handleCSRFError(url, options, response)
    }

    return response
  }
  private handleAuthenticationFailure(): void {
    if (this.isLoggingOut) {
      return
    }

    this.isLoggingOut = true
    this.clearCSRFToken()

    const authCallback = (globalThis as { authLogoutCallback?: () => void }).authLogoutCallback

    if (authCallback) {
      authCallback()
    }

    setTimeout(() => {
      this.isLoggingOut = false
    }, 1000)
  }
  public async get(url: string, options: RequestOptions = {}): Promise<Response> {
    if (this.timeTravelAsOf) {
      const separator = url.includes('?') ? '&' : '?'

      url = `${url}${separator}as_of=${encodeURIComponent(this.timeTravelAsOf)}`
    }

    if (this.operatorPublicId) {
      const separator = url.includes('?') ? '&' : '?'

      url = `${url}${separator}operator_public_id=${encodeURIComponent(this.operatorPublicId)}`
    }

    if (this.walletPublicId) {
      const separator = url.includes('?') ? '&' : '?'

      url = `${url}${separator}wallet_public_id=${encodeURIComponent(this.walletPublicId)}`
    }

    return this.request(url, { ...options, method: 'GET' })
  }
  private static readonly AUTH_PATHS = new Set([
    '/api/auth/login',
    '/api/auth/refresh',
    '/api/auth/logout',
  ])
  public async post(url: string, body?: unknown, options: RequestOptions = {}): Promise<Response> {
    return this.request(url, {
      ...options,
      method: 'POST',
      body: body ? JSON.stringify(stampProvenance(body)) : undefined,
    })
  }
  private async raiseHttpError(response: Response): Promise<never> {
    const { message, details } = await this.extractError(response)

    throw new APIError(message, response.status, response.statusText, details)
  }
  private async extractError(response: Response): Promise<ErrorPayload> {
    const fallbackMessage = fallbackErrorMessage(response)

    try {
      const data: unknown = await response.json()

      return extractErrorPayload(data, fallbackMessage)
    } catch {
      return { message: fallbackMessage }
    }
  }
  public async getJSON<T>(url: string, options: RequestOptions = {}): Promise<T> {
    const response = await this.get(url, options)

    if (!response.ok) {
      await this.raiseHttpError(response)
    }

    return response.json()
  }
  public async postJSON<T>(url: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    const response = await this.post(url, body, options)

    if (!response.ok) {
      await this.raiseHttpError(response)
    }

    return response.json()
  }
  public async patchJSON<T>(url: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    const response = await this.request(url, {
      ...options,
      method: 'PATCH',
      body: body ? JSON.stringify(stampProvenance(body)) : undefined,
    })

    if (!response.ok) {
      await this.raiseHttpError(response)
    }

    return response.json()
  }
  public clearCSRFToken(): void {
    this.setCsrfToken(null)
  }
  public setCsrfToken(token: string | null): void {
    const name = 'csrf_token'

    if (!token) {
      document.cookie = `${name}=; Path=/; Max-Age=0; SameSite=Lax`

      return
    }

    const encoded = encodeURIComponent(token)

    document.cookie = `${name}=${encoded}; Path=/; SameSite=Lax`
  }
  public hasAuthCookies(): boolean {
    const cookies = document.cookie.split(';')

    for (const cookie of cookies) {
      const [name] = cookie.trim().split('=')

      if (name === 'refresh_token' || name === 'csrf_token' || name === 'access_token') {
        return true
      }
    }

    return false
  }
  async getHealth(): Promise<HealthCheckResponse> {
    const data = await this.getJSON('/api/health')

    return validateResponse(data, HealthCheckResponseSchema, '/health')
  }
  async getSystemStatus(): Promise<SystemStatusResponse> {
    const data = await this.getJSON('/api/status')

    return validateResponse(data, SystemStatusResponseSchema, '/status')
  }
  async getSystemMetrics(): Promise<SystemMetricsResponse> {
    const data = await this.getJSON('/api/metrics/system')

    return validateResponse(data, SystemMetricsResponseSchema, '/metrics/system')
  }
  async getDbStats(): Promise<DbStatsResponse> {
    const data = await this.getJSON('/api/metrics/db/tables')

    return validateResponse(data, DbStatsResponseSchema, '/metrics/db/tables')
  }
  async getNotificationMetrics(): Promise<NotificationMetricsResponse> {
    const data = await this.getJSON('/api/metrics/notifications')

    return validateResponse(data, NotificationMetricsResponseSchema, '/metrics/notifications')
  }
  async getRetentionRun(): Promise<RetentionRunResponse> {
    const data = await this.getJSON('/api/metrics/retention')

    return validateResponse(data, RetentionRunResponseSchema, '/metrics/retention')
  }
  async getCandles(
    instrument: string,
    exchange: string,
    timeframe: string = '1m',
    limit: number = 100
  ): Promise<CandleData[]> {
    const params = new URLSearchParams({ instrument, exchange, timeframe, limit: String(limit) })
    const response = await this.get(`/api/candles?${params}`)

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()

    return data.payload ?? []
  }
  async getOrders(
    symbol?: string,
    limit: number = 100,
    offset: number = 0,
    exchange?: string
  ): Promise<OrderListResponse> {
    const params = new URLSearchParams({ limit: String(limit), offset: String(offset) })

    if (symbol) {
      params.set('symbol', symbol)
    }

    if (exchange) {
      params.set('exchange', exchange)
    }

    const data = await this.getJSON(`/api/orders?${params}`)

    return validateResponse(data, OrderListResponseSchema, '/orders')
  }
  async getExecutions(limit: number = 100): Promise<ExecutionListResponse> {
    const params = new URLSearchParams({ limit: String(limit) })
    const data = await this.getJSON(`/api/executions?${params}`)

    return validateResponse(data, ExecutionListResponseSchema, '/executions')
  }
  async getPositions(): Promise<PositionListResponse> {
    const data = await this.getJSON('/api/positions')

    return validateResponse(data, PositionListResponseSchema, '/positions')
  }
  async getSignals(
    strategy?: string,
    limit: number = 100,
    instrument?: string,
    hours: number = 24,
    exchange?: string
  ): Promise<SignalListResponse> {
    const params = new URLSearchParams({
      limit: String(limit),
      hours: String(hours),
    })

    if (strategy) params.set('strategy', strategy)
    if (instrument) params.set('instrument', instrument)
    if (exchange) params.set('exchange', exchange)
    const data = await this.getJSON(`/api/signals?${params}`)

    return validateResponse(data, SignalListResponseSchema, '/signals')
  }
  async getExchanges(): Promise<ExchangeListResponse> {
    const data = await this.getJSON('/api/exchanges')

    return validateResponse(data, ExchangeListResponseSchema, '/exchanges')
  }
  async getExchangeInstruments(exchange: string): Promise<InstrumentListResponse> {
    const data = await this.getJSON(`/api/exchanges/${encodeURIComponent(exchange)}/instruments`)

    return validateResponse(
      data,
      InstrumentListResponseSchema,
      `/exchanges/${exchange}/instruments`
    )
  }
  async getExchangeInstrumentsDetail(exchange: string): Promise<InstrumentDetailListResponse> {
    const data = await this.getJSON(
      `/api/exchanges/${encodeURIComponent(exchange)}/instruments/detail`
    )

    return validateResponse(
      data,
      InstrumentDetailListResponseSchema,
      `/exchanges/${exchange}/instruments/detail`
    )
  }
  async getOperators(): Promise<OperatorListResponse> {
    const data = await this.getJSON('/api/operators')

    return validateResponse(data, OperatorListResponseSchema, '/operators')
  }
  async getWallets(): Promise<WalletListResponse> {
    const data = await this.getJSON('/api/wallets')

    return validateResponse(data, WalletListResponseSchema, '/wallets')
  }
  async getScopeGrants(walletPublicId: string): Promise<ScopeGrantListResponse> {
    const params = new URLSearchParams({ wallet_public_id: walletPublicId })

    if (this.timeTravelAsOf) {
      params.set('as_of', this.timeTravelAsOf)
    }

    const response = await this.request(`/api/scope-grants?${params}`, { method: 'GET' })

    if (!response.ok) {
      await this.raiseHttpError(response)
    }

    const data = await response.json()

    return validateResponse(data, ScopeGrantListResponseSchema, '/scope-grants')
  }
  async createScopeGrant(body: CreateScopeGrantBody): Promise<ScopeGrantResponse> {
    const data = await this.postJSON('/api/scope-grants', body)

    return validateResponse(data, ScopeGrantResponseSchema, '/scope-grants POST')
  }
  async handoverScopeGrant(body: HandoverScopeGrantBody): Promise<HandoverScopeGrantResponse> {
    const data = await this.postJSON('/api/scope-grants/handover', body)

    return validateResponse(data, HandoverScopeGrantResponseSchema, '/scope-grants/handover POST')
  }
  async getCredentials(walletPublicId: string): Promise<CredentialListResponse> {
    let url = `/api/wallets/${encodeURIComponent(walletPublicId)}/credentials`

    if (this.timeTravelAsOf) {
      url = `${url}?as_of=${encodeURIComponent(this.timeTravelAsOf)}`
    }

    const response = await this.request(url, { method: 'GET' })

    if (!response.ok) {
      await this.raiseHttpError(response)
    }

    const data = await response.json()

    return validateResponse(data, CredentialListResponseSchema, '/wallets/:id/credentials')
  }
  async createCredential(
    walletPublicId: string,
    body: CreateCredentialBody
  ): Promise<CredentialResponse> {
    const data = await this.postJSON(
      `/api/wallets/${encodeURIComponent(walletPublicId)}/credentials`,
      body
    )

    return validateResponse(data, CredentialResponseSchema, '/wallets/:id/credentials POST')
  }
  async rotateCredential(
    walletPublicId: string,
    credentialPublicId: string,
    body: RotateCredentialBody
  ): Promise<CredentialResponse> {
    const data = await this.postJSON(
      `/api/wallets/${encodeURIComponent(walletPublicId)}/credentials/${encodeURIComponent(credentialPublicId)}/rotate`,
      body
    )

    return validateResponse(
      data,
      CredentialResponseSchema,
      '/wallets/:id/credentials/:id/rotate POST'
    )
  }
  async getSettings(category?: string): Promise<SettingListResponse> {
    const params = category ? new URLSearchParams({ category }) : ''
    const data = await this.getJSON(`/api/settings${params ? '?' + params : ''}`)

    return validateResponse(data, SettingListResponseSchema, '/settings')
  }
  async getSettingCategories(): Promise<string[]> {
    const data = await this.getJSON('/api/settings/categories')
    const response = validateResponse(data, SettingCategoriesResponseSchema, '/settings/categories')

    return response.payload
  }
  async updateSetting(key: string, data: SettingUpdateBody): Promise<SettingResponse> {
    const response = await this.postJSON(`/api/settings/${encodeURIComponent(key)}/set`, data)

    return validateResponse(response, SettingResponseSchema, '/settings/:key/set')
  }
  async removeSetting(key: string): Promise<{ payload: string }> {
    const data = await this.postJSON(`/api/settings/${encodeURIComponent(key)}/remove`, {})

    return validateResponse(data, MessageResponseSchema, '/settings/:key/remove')
  }
  async getProcessSchema(name: string): Promise<ProcessSchemaResponse> {
    const data = await this.getJSON(`/api/processes/schema/${encodeURIComponent(name)}`)

    return validateResponse(data, ProcessSchemaResponseSchema, '/processes/schema/:name')
  }
  async createProcessConfig(body: ProcessCreateBody): Promise<ProcessCreateResponse> {
    const data = await this.postJSON('/api/processes', body)

    return validateResponse(data, ProcessCreateResponseSchema, '/processes')
  }
  async createOrder(body: Record<string, unknown>): Promise<Record<string, unknown>> {
    return this.postJSON('/api/orders', body)
  }
  async cancelOrder(clientOrderId: string): Promise<Record<string, unknown>> {
    return this.postJSON(
      `/api/orders/by-client-order-id/${encodeURIComponent(clientOrderId)}/cancel`,
      {}
    )
  }
  async createBracket(body: BracketCreateBody): Promise<ExecutionPlanResponse> {
    const data = await this.postJSON('/api/execution-plans', body)

    return validateResponse(data, ExecutionPlanResponseSchema, '/execution-plans POST')
  }
  async cancelBracket(
    planPublicId: string,
    body?: BracketCancelBody
  ): Promise<ExecutionPlanResponse> {
    const data = await this.postJSON(
      `/api/execution-plans/${encodeURIComponent(planPublicId)}/cancel`,
      body ?? {}
    )

    return validateResponse(data, ExecutionPlanResponseSchema, '/execution-plans/:id/cancel POST')
  }
  async getBracket(planPublicId: string): Promise<ExecutionPlanResponse> {
    const data = await this.getJSON(`/api/execution-plans/${encodeURIComponent(planPublicId)}`)

    return validateResponse(data, ExecutionPlanResponseSchema, '/execution-plans/:id')
  }
  async createTrailingStop(body: TrailingStopCreateBody): Promise<ExecutionPlanResponse> {
    const data = await this.postJSON('/api/trailing-stops', body)

    return validateResponse(data, ExecutionPlanResponseSchema, '/trailing-stops POST')
  }
  async cancelTrailingStop(
    planPublicId: string,
    body?: TrailingStopCancelBody
  ): Promise<ExecutionPlanResponse> {
    const data = await this.postJSON(
      `/api/trailing-stops/${encodeURIComponent(planPublicId)}/cancel`,
      body ?? {}
    )

    return validateResponse(data, ExecutionPlanResponseSchema, '/trailing-stops/:id/cancel POST')
  }
  async getTrailingStopByCycle(cyclePublicId: string): Promise<TrailingStopByCycleResult> {
    const data = await this.getJSON(
      `/api/trailing-stops/by-cycle/${encodeURIComponent(cyclePublicId)}`
    )

    return data as TrailingStopByCycleResult
  }
  async getConfiguredProcesses(): Promise<ConfiguredProcessesResponse> {
    const data = await this.getJSON('/api/processes/configured')

    return validateResponse(data, ConfiguredProcessesResponseSchema, '/processes/configured')
  }
  async getProcessSummary(): Promise<ProcessSummaryResponse> {
    const data = await this.getJSON('/api/processes/summary')

    return validateResponse(data, ProcessSummaryResponseSchema, '/processes/summary')
  }
  async getStrategies(): Promise<StrategyListResponse> {
    const data = await this.getJSON('/api/strategies')

    return validateResponse(data, StrategyListResponseSchema, '/strategies')
  }
  async getAvailableProcesses(): Promise<AvailableProcessesResponse> {
    const data = await this.getJSON('/api/processes/available')

    return validateResponse(data, AvailableProcessesResponseSchema, '/processes/available')
  }
  async getProcessRuns(options?: { limit?: number; name?: string }): Promise<ProcessRunsResponse> {
    const params = new URLSearchParams()

    if (options?.limit) params.set('limit', String(options.limit))
    if (options?.name) params.set('name', options.name)
    const query = params.toString()
    const data = await this.getJSON(`/api/processes/runs${query ? '?' + query : ''}`)

    return validateResponse(data, ProcessRunsResponseSchema, '/processes/runs')
  }
  async startProcessByName(
    name: string,
    options?: ProcessStartBody
  ): Promise<ProcessStartResponse> {
    const data = await this.postJSON(
      `/api/processes/${encodeURIComponent(name)}/start`,
      options || {}
    )

    return validateResponse(data, ProcessStartResponseSchema, '/processes/:name/start')
  }
  async stopProcessByName(name: string): Promise<ProcessStopResponse> {
    const data = await this.postJSON(`/api/processes/${encodeURIComponent(name)}/stop`)

    return validateResponse(data, ProcessStopResponseSchema, '/processes/:name/stop')
  }
  async changePassword(
    userId: string,
    currentPassword: string,
    newPassword: string
  ): Promise<{ payload: string }> {
    const body: ChangePasswordBody = {
      current_password: currentPassword,
      new_password: newPassword,
    }
    const data = await this.postJSON(
      `/api/auth/users/${encodeURIComponent(userId)}/change-password`,
      body
    )

    return validateResponse(data, MessageResponseSchema, '/auth/users/:id/change-password')
  }
  async listUsers(includeInactive: boolean): Promise<UserListResponse> {
    const data = await this.getJSON(`/api/auth/users?include_inactive=${includeInactive}`)

    return validateResponse(data, UserListResponseSchema, '/auth/users')
  }
  async createUser(body: CreateUserBody): Promise<UserResponse> {
    const data = await this.postJSON('/api/auth/users', body)

    return validateResponse(data, UserResponseSchema, '/auth/users POST')
  }
  async updateUser(userId: string, body: UpdateUserBody): Promise<UserResponse> {
    const data = await this.postJSON(`/api/auth/users/${encodeURIComponent(userId)}/update`, body)

    return validateResponse(data, UserResponseSchema, '/auth/users/:id/update')
  }
  async deactivateUser(userId: string): Promise<{ payload: string }> {
    const data = await this.postJSON(`/api/auth/users/${encodeURIComponent(userId)}/deactivate`, {})

    return validateResponse(data, MessageResponseSchema, '/auth/users/:id/deactivate')
  }
  async adminResetPassword(
    userId: string,
    body: AdminResetPasswordBody
  ): Promise<{ payload: string }> {
    const data = await this.postJSON(
      `/api/auth/users/${encodeURIComponent(userId)}/admin-reset-password`,
      body
    )

    return validateResponse(data, MessageResponseSchema, '/auth/users/:id/admin-reset-password')
  }
  async getBacktests(
    limit = 20,
    offset = 0,
    strategy?: string,
    status?: string,
    configHash?: string | null
  ): Promise<BacktestRunListResponse> {
    const params = new URLSearchParams()

    params.set('limit', String(limit))
    params.set('offset', String(offset))
    if (strategy) params.set('strategy', strategy)
    if (status) params.set('status', status)
    if (configHash) params.set('config_hash', configHash)
    const data = await this.getJSON(`/api/backtests?${params.toString()}`)

    return data as BacktestRunListResponse
  }
  async getBacktest(runId: string): Promise<BacktestRunResponse> {
    const data = await this.getJSON(`/api/backtests/${encodeURIComponent(runId)}`)

    return data as BacktestRunResponse
  }
  async createBacktest(body: BacktestCreateBody): Promise<BacktestRunResponse> {
    const data = await this.postJSON('/api/backtests', body)

    return data as BacktestRunResponse
  }
  async cancelBacktest(runId: string): Promise<BacktestRunResponse> {
    const data = await this.postJSON(`/api/backtests/${encodeURIComponent(runId)}/cancel`, {
      reason: '',
    })

    return data as BacktestRunResponse
  }
  async rerunBacktest(runId: string): Promise<BacktestRunResponse> {
    const data = await this.postJSON(`/api/backtests/${encodeURIComponent(runId)}/rerun`, {})

    return data as BacktestRunResponse
  }
  async getBacktestTrades(runId: string, limit = 100): Promise<BacktestTradeListResponse> {
    const data = await this.getJSON(
      `/api/backtests/${encodeURIComponent(runId)}/trades?limit=${limit}`
    )

    return data as BacktestTradeListResponse
  }
  async getBacktestSignals(runId: string, limit = 100): Promise<BacktestSignalListResponse> {
    const data = await this.getJSON(
      `/api/backtests/${encodeURIComponent(runId)}/signals?limit=${limit}`
    )

    return data as BacktestSignalListResponse
  }
  async createBacktestComparison(body: BacktestCompareBody): Promise<BacktestComparisonResponse> {
    const data = await this.postJSON('/api/backtests/compare', body)

    return data as BacktestComparisonResponse
  }
  async getBacktestComparison(
    comparisonPublicId: string
  ): Promise<BacktestComparisonDetailResponse> {
    const data = await this.getJSON(
      `/api/backtests/compare/${encodeURIComponent(comparisonPublicId)}`
    )

    return data as BacktestComparisonDetailResponse
  }
  async getBacktestComparisons(limit = 20, offset = 0): Promise<BacktestComparisonListResponse> {
    const params = new URLSearchParams()

    params.set('limit', String(limit))
    params.set('offset', String(offset))
    const data = await this.getJSON(`/api/backtests/compare?${params.toString()}`)

    return data as BacktestComparisonListResponse
  }
  async getFeatureFlags(): Promise<FeatureFlagsResponse> {
    const data = await this.getJSON('/api/settings/features')

    return validateResponse(data, FeatureFlagsResponseSchema, '/settings/features')
  }
  async listAiDelegates(): Promise<DelegateListResponse> {
    const data = await this.getJSON('/api/ai-delegates')

    return validateResponse(data, DelegateListResponseSchema, '/ai-delegates')
  }
  async getAiDelegate(publicId: string): Promise<DelegateResponse> {
    const data = await this.getJSON(`/api/ai-delegates/${encodeURIComponent(publicId)}`)

    return validateResponse(data, DelegateResponseSchema, '/ai-delegates/{id}')
  }
  async createAiDelegate(body: DelegateCreateBody): Promise<DelegateCreatedResponse> {
    const data = await this.postJSON('/api/ai-delegates', body)

    return validateResponse(data, DelegateCreatedResponseSchema, '/ai-delegates')
  }
  async updateAiDelegateCaps(
    publicId: string,
    body: DelegateCapsUpdateBody
  ): Promise<DelegateResponse> {
    const data = await this.patchJSON(`/api/ai-delegates/${encodeURIComponent(publicId)}`, body)

    return validateResponse(data, DelegateResponseSchema, '/ai-delegates/{id}')
  }
  async deactivateAiDelegate(publicId: string): Promise<DelegateResponse> {
    const data = await this.postJSON(`/api/ai-delegates/${encodeURIComponent(publicId)}/deactivate`)

    return validateResponse(data, DelegateResponseSchema, '/ai-delegates/{id}/deactivate')
  }
  async listPendingAiReviews(
    params: Readonly<{ wallet_public_id?: string; limit?: number }> = {}
  ): Promise<PendingReviewListResponse> {
    const search = new URLSearchParams()

    if (params.wallet_public_id !== undefined) {
      search.set('wallet_public_id', params.wallet_public_id)
    }

    if (params.limit !== undefined) {
      search.set('limit', String(params.limit))
    }

    const qs = search.toString()
    const path = qs ? `/api/ai-reviews/pending?${qs}` : '/api/ai-reviews/pending'
    const response = await this.request(path, { method: 'GET' })

    if (!response.ok) {
      await this.raiseHttpError(response)
    }

    const data = await response.json()

    return validateResponse(data, PendingReviewListResponseSchema, '/ai-reviews/pending')
  }
}

export const apiClient = APIClient.getInstance()

function stampProvenance(body: unknown): unknown {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) return body

  const tracker = getTracker()

  return {
    public_id: uuid7(),
    session_id: tracker.sessionId,
    sequence_id: tracker.nextSequence('control'),
    timestamp: new Date().toISOString(),
    payload: body,
  }
}

async function cacheWsTicketFromResponse(response: Response): Promise<void> {
  try {
    const envelope = (await response.clone().json()) as {
      payload?: {
        ws_token?: string
        ws_token_exp?: string
      }
    }
    const data = envelope?.payload

    if (typeof data?.ws_token === 'string' && typeof data?.ws_token_exp === 'string') {
      const expSeconds = Math.floor(new Date(data.ws_token_exp).getTime() / 1000)

      storeWsTicket({ token: data.ws_token, exp: expSeconds })
    } else {
      storeWsTicket(null)
    }
  } catch {
    storeWsTicket(null)
  }
}
