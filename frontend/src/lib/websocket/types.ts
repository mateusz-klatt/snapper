import type {
  AiReviewCapsViolationFrameData,
  AiReviewDecisionAckFrameData,
  AiReviewRequestFrameData,
  BacktestProgressData,
  WebSocketMessages,
  TickData,
  CandleData,
  TradeData,
  SignalData,
  OrderData,
  ExecutionData,
  HeartbeatData,
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
  WSPongResponse,
} from '../../types/ws'
import type { Components } from '../../types/api.generated'

export interface WebSocketMessageTypeMap {
  tick: TickData
  candle: CandleData
  trade: TradeData
  signal: SignalData
  order: OrderData
  execution: ExecutionData
  heartbeat: HeartbeatData
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
  pong: WSPongResponse
  backtest_progress: BacktestProgressData
  'ai_review.request': AiReviewRequestFrameData
  'ai_review.decision_ack': AiReviewDecisionAckFrameData
  'ai_review.caps_violation': AiReviewCapsViolationFrameData
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
export interface WebSocketClientOptions {
  url?: string
  reconnectInterval?: number
  maxReconnectAttempts?: number
  heartbeatInterval?: number
  throttleInterval?: number
  secure?: boolean
}
