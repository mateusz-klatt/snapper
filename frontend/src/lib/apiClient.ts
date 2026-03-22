import { v7 as uuid7 } from 'uuid'
import { getCookie } from './utils'
import { storeWsTicket } from './wsTicketCache'
import { validateResponse } from './schemas/api'
import { getTracker } from './sequenceTracker'
import {
  SettingCategoriesResponseSchema,
  SettingListResponseSchema,
  SettingResponseSchema,
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
  InstrumentListResponseSchema,
  OrderListResponseSchema,
  ExecutionListResponseSchema,
  PositionListResponseSchema,
  SignalListResponseSchema,
} from './schemas/api.generated.zod'
import type {
  CandleData,
  SystemStatusResponse,
  SettingListResponse,
  SettingResponse,
  SettingUpdate,
  ConfiguredProcessesResponse,
  ProcessSummaryResponse,
  AvailableProcessesResponse,
  ProcessRunsResponse,
  ProcessSchemaResponse,
  ProcessCreateRequest,
  ProcessCreateResponse,
  ProcessStartRequest,
  ProcessStartResponse,
  ProcessStopResponse,
  StrategyListResponse,
  ChangePasswordRequest,
  UserListResponse,
  UserResponse,
  CreateUserRequest,
  UpdateUserRequest,
  AdminResetPasswordRequest,
  ExchangeListResponse,
  InstrumentListResponse,
  OrderListResponse,
  ExecutionListResponse,
  PositionListResponse,
  SignalListResponse,
  HealthCheckResponse,
} from '../types/api'

interface RequestOptions {
  skipCSRF?: boolean
  skipRetry?: boolean
  method?: string
  headers?: Record<string, string> | Headers
  body?: string | FormData | URLSearchParams | null
}

const MUTATING_METHODS = new Set(['POST', 'PUT', 'DELETE', 'PATCH'])

class APIClient {
  private static instance: APIClient
  private isLoggingOut = false
  private constructor() {}
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
    return this.request(url, { ...options, method: 'GET' })
  }
  public async post(url: string, body?: unknown, options: RequestOptions = {}): Promise<Response> {
    return this.request(url, {
      ...options,
      method: 'POST',
      body: body ? JSON.stringify(stampProvenance(body)) : undefined,
    })
  }
  public async put(url: string, body?: unknown, options: RequestOptions = {}): Promise<Response> {
    return this.request(url, {
      ...options,
      method: 'PUT',
      body: body ? JSON.stringify(stampProvenance(body)) : undefined,
    })
  }
  public async delete(url: string, options: RequestOptions = {}): Promise<Response> {
    return this.request(url, { ...options, method: 'DELETE' })
  }
  private async extractErrorMessage(response: Response): Promise<string> {
    try {
      const data = await response.json()

      if (data && typeof data === 'object') {
        if ('detail' in data) {
          return String(data.detail)
        }

        if ('message' in data) {
          return String(data.message)
        }
      }
    } catch {
      void 0
    }

    return `HTTP ${response.status}: ${response.statusText}`
  }
  public async getJSON<T>(url: string, options: RequestOptions = {}): Promise<T> {
    const response = await this.get(url, options)

    if (!response.ok) {
      throw new Error(await this.extractErrorMessage(response))
    }

    return response.json()
  }
  public async postJSON<T>(url: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    const response = await this.post(url, body, options)

    if (!response.ok) {
      throw new Error(await this.extractErrorMessage(response))
    }

    return response.json()
  }
  public async putJSON<T>(url: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    const response = await this.put(url, body, options)

    if (!response.ok) {
      throw new Error(await this.extractErrorMessage(response))
    }

    return response.json()
  }
  public async deleteJSON<T>(url: string, options: RequestOptions = {}): Promise<T> {
    const response = await this.delete(url, options)

    if (!response.ok) {
      throw new Error(await this.extractErrorMessage(response))
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
  async updateSetting(key: string, data: SettingUpdate): Promise<SettingResponse> {
    const response = await this.putJSON(`/api/settings/${encodeURIComponent(key)}`, data)

    return validateResponse(response, SettingResponseSchema, '/settings/:key')
  }
  async deleteSetting(key: string): Promise<{ payload: string }> {
    const data = await this.deleteJSON(`/api/settings/${encodeURIComponent(key)}`)

    return validateResponse(data, MessageResponseSchema, '/settings/:key DELETE')
  }
  async getProcessSchema(name: string): Promise<ProcessSchemaResponse> {
    const data = await this.getJSON(`/api/processes/schema/${encodeURIComponent(name)}`)

    return validateResponse(data, ProcessSchemaResponseSchema, '/processes/schema/:name')
  }
  async createProcessConfig(body: ProcessCreateRequest): Promise<ProcessCreateResponse> {
    const data = await this.postJSON('/api/processes', body)

    return validateResponse(data, ProcessCreateResponseSchema, '/processes')
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
    options?: ProcessStartRequest
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
    const body: ChangePasswordRequest = {
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
  async createUser(body: CreateUserRequest): Promise<UserResponse> {
    const data = await this.postJSON('/api/auth/users', body)

    return validateResponse(data, UserResponseSchema, '/auth/users POST')
  }
  async updateUser(userId: string, body: UpdateUserRequest): Promise<UserResponse> {
    const data = await this.putJSON(`/api/auth/users/${encodeURIComponent(userId)}`, body)

    return validateResponse(data, UserResponseSchema, '/auth/users/:id PUT')
  }
  async deactivateUser(userId: string): Promise<{ payload: string }> {
    const data = await this.deleteJSON(`/api/auth/users/${encodeURIComponent(userId)}`)

    return validateResponse(data, MessageResponseSchema, '/auth/users/:id DELETE')
  }
  async adminResetPassword(
    userId: string,
    body: AdminResetPasswordRequest
  ): Promise<{ payload: string }> {
    const data = await this.postJSON(
      `/api/auth/users/${encodeURIComponent(userId)}/admin-reset-password`,
      body
    )

    return validateResponse(data, MessageResponseSchema, '/auth/users/:id/admin-reset-password')
  }
}

export const apiClient = APIClient.getInstance()

function stampProvenance(body: unknown): unknown {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) return body

  const tracker = getTracker()

  return {
    ...(body as Record<string, unknown>),
    public_id: uuid7(),
    session_id: tracker.sessionId,
    sequence_id: tracker.nextSequence('control'),
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
