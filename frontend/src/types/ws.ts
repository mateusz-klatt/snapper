export type {
  AiReviewCapsViolationFrameData,
  AiReviewDecisionAckFrameData,
  AiReviewRequestFrameData,
  BacktestProgressData,
  TickData,
  CandleData,
  TradeData,
  SignalData,
  OrderData,
  OrderCancelData,
  OrderEventData,
  OrderReplaceData,
  OrderRequestData,
  ExecutionData,
  HeartbeatData,
  PositionData,
  ReplayEndData,
  ReplayStartData,
  SettingChangedData,
  SymbolAliasUpdateData,
  WSErrorResponse,
  WSAuthRequiredResponse,
  WSAuthOkResponse,
  WSAuthFailedResponse,
  WSAuthExpiredResponse,
  WSAuthCompleteResponse,
  WSReauthRequiredResponse,
  WSReauthOkResponse,
  WSAuthenticateRequest,
  WSReauthRequest,
  WSPingRequest,
  WSGetSubscriptionsRequest,
  WSSubscribeRequest,
  WSUnsubscribeRequest,
  WSSubscriptionSuccessResponse,
  WSSubscriptionsListResponse,
  WSPongResponse,
} from './ws.generated'
import type {
  AiReviewCapsViolationFrameData,
  AiReviewDecisionAckFrameData,
  AiReviewRequestFrameData,
  BacktestProgressData,
  TickData,
  CandleData,
  TradeData,
  SignalData,
  OrderData,
  OrderCancelData,
  OrderEventData,
  OrderReplaceData,
  OrderRequestData,
  ExecutionData,
  HeartbeatData,
  PositionData,
  ReplayEndData,
  ReplayStartData,
  SettingChangedData,
  SymbolAliasUpdateData,
  WSErrorResponse,
  WSAuthRequiredResponse,
  WSAuthOkResponse,
  WSAuthFailedResponse,
  WSAuthExpiredResponse,
  WSAuthCompleteResponse,
  WSReauthRequiredResponse,
  WSReauthOkResponse,
  WSAuthenticateRequest,
  WSReauthRequest,
  WSPingRequest,
  WSGetSubscriptionsRequest,
  WSSubscribeRequest,
  WSUnsubscribeRequest,
  WSSubscriptionSuccessResponse,
  WSSubscriptionsListResponse,
  WSPongResponse,
} from './ws.generated'

export type WebSocketMessages =
  | AiReviewCapsViolationFrameData
  | AiReviewDecisionAckFrameData
  | AiReviewRequestFrameData
  | BacktestProgressData
  | TickData
  | CandleData
  | TradeData
  | SignalData
  | OrderData
  | OrderCancelData
  | OrderEventData
  | OrderReplaceData
  | OrderRequestData
  | ExecutionData
  | HeartbeatData
  | PositionData
  | ReplayEndData
  | ReplayStartData
  | SettingChangedData
  | SymbolAliasUpdateData
  | WSErrorResponse
  | WSAuthRequiredResponse
  | WSAuthOkResponse
  | WSAuthFailedResponse
  | WSAuthExpiredResponse
  | WSAuthCompleteResponse
  | WSReauthRequiredResponse
  | WSReauthOkResponse
  | WSAuthenticateRequest
  | WSReauthRequest
  | WSPingRequest
  | WSGetSubscriptionsRequest
  | WSSubscribeRequest
  | WSUnsubscribeRequest
  | WSSubscriptionSuccessResponse
  | WSSubscriptionsListResponse
  | WSPongResponse

export type PongWithRtt = WSPongResponse & { rtt_ms: number }

export function isCandle(msg: WebSocketMessages): msg is CandleData {
  return msg.type === 'candle'
}

export function isTick(msg: WebSocketMessages): msg is TickData {
  return msg.type === 'tick'
}

export function isTrade(msg: WebSocketMessages): msg is TradeData {
  return msg.type === 'trade'
}

export function isOrder(msg: WebSocketMessages): msg is OrderData {
  return msg.type === 'order'
}

export function isExecution(msg: WebSocketMessages): msg is ExecutionData {
  return msg.type === 'execution'
}

export function isSignal(msg: WebSocketMessages): msg is SignalData {
  return msg.type === 'signal'
}

export function isHeartbeat(msg: WebSocketMessages): msg is HeartbeatData {
  return msg.type === 'heartbeat'
}

export function isAiReviewRequest(msg: WebSocketMessages): msg is AiReviewRequestFrameData {
  return msg.type === 'ai_review.request'
}

export function isAiReviewDecisionAck(msg: WebSocketMessages): msg is AiReviewDecisionAckFrameData {
  return msg.type === 'ai_review.decision_ack'
}

export function isAiReviewCapsViolation(
  msg: WebSocketMessages
): msg is AiReviewCapsViolationFrameData {
  return msg.type === 'ai_review.caps_violation'
}
