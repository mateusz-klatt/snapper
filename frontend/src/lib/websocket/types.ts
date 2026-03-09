import type {
  WebSocketMessages,
  TickEnvelope,
  CandleEnvelope,
  TradeEnvelope,
  SignalEnvelope,
  OrderStatusEnvelope,
  FillEnvelope,
  HeartbeatEnvelope,
  WSErrorResponse,
  WSAuthRequiredResponse,
  WSAuthOkResponse,
  WSAuthFailedResponse,
  WSAuthExpiredResponse,
  WSAuthCompleteResponse,
  WSReauthRequiredResponse,
  WSReauthOkResponse,
  WSSubscriptionSuccessResponse,
  WSSubscriptionsListResponse,
  WSTopicSuggestionsResponse,
  WSPongResponse,
} from '../../types/ws'
import type { Components } from '../../types/api.generated'

export interface WebSocketMessageTypeMap {
  tick: TickEnvelope
  bar: CandleEnvelope
  trade: TradeEnvelope
  signal: SignalEnvelope
  order_status: OrderStatusEnvelope
  fill: FillEnvelope
  heartbeat: HeartbeatEnvelope
  error: WSErrorResponse
  auth_required: WSAuthRequiredResponse
  auth_ok: WSAuthOkResponse
  auth_failed: WSAuthFailedResponse
  auth_expired: WSAuthExpiredResponse
  auth_complete: WSAuthCompleteResponse
  reauth_required: WSReauthRequiredResponse
  reauth_ok: WSReauthOkResponse
  subscription_success: WSSubscriptionSuccessResponse
  subscriptions_list: WSSubscriptionsListResponse
  topic_suggestions: WSTopicSuggestionsResponse
  pong: WSPongResponse
}
export type WebSocketMessageType = keyof WebSocketMessageTypeMap
export type AuthControlMessageType =
  | 'auth_complete'
  | 'auth_required'
  | 'auth_failed'
  | 'auth_expired'
  | 'auth_ok'
  | 'reauth_required'
  | 'reauth_ok'
export const AUTH_CONTROL_MESSAGES: ReadonlySet<AuthControlMessageType> = new Set([
  'auth_complete',
  'auth_required',
  'auth_failed',
  'auth_expired',
  'auth_ok',
  'reauth_required',
  'reauth_ok',
])
export type RefreshWsTokenResponse = Components['schemas']['RefreshResponse']
export type MessageHandler = (message: WebSocketMessages) => void
export type TypedMessageHandler<T extends WebSocketMessageType> = (
  message: WebSocketMessageTypeMap[T]
) => void
export type ConnectionHandler = (connected: boolean) => void
export type UnsubscribeFn = () => void
export type ConnectionState =
  | 'disconnected'
  | 'connecting'
  | 'connected'
  | 'authenticating'
  | 'authenticated'
export interface WebSocketClientOptions {
  url?: string
  reconnectInterval?: number
  maxReconnectAttempts?: number
  heartbeatInterval?: number
  throttleInterval?: number
  secure?: boolean
}
