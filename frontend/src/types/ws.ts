export type {
  WsMessageSchema,
  TickEnvelope,
  BarEnvelope,
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
  WSAuthenticateRequest,
  WSReauthRequest,
  WSPingRequest,
  WSGetSubscriptionsRequest,
  WSGetTopicSuggestionsRequest,
  WSSubscribeRequest,
  WSUnsubscribeRequest,
  WSSubscriptionSuccessResponse,
  WSSubscriptionsListResponse,
  WSTopicSuggestionsResponse,
  WSPongResponse,
} from './ws.generated'
import type {
  TickEnvelope,
  BarEnvelope,
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
  WSAuthenticateRequest,
  WSReauthRequest,
  WSPingRequest,
  WSGetSubscriptionsRequest,
  WSGetTopicSuggestionsRequest,
  WSSubscribeRequest,
  WSUnsubscribeRequest,
  WSSubscriptionSuccessResponse,
  WSSubscriptionsListResponse,
  WSTopicSuggestionsResponse,
  WSPongResponse,
} from './ws.generated'

export type WebSocketMessages =
  | TickEnvelope
  | BarEnvelope
  | TradeEnvelope
  | SignalEnvelope
  | OrderStatusEnvelope
  | FillEnvelope
  | HeartbeatEnvelope
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
  | WSGetTopicSuggestionsRequest
  | WSSubscribeRequest
  | WSUnsubscribeRequest
  | WSSubscriptionSuccessResponse
  | WSSubscriptionsListResponse
  | WSTopicSuggestionsResponse
  | WSPongResponse

export function isCandle(msg: WebSocketMessages): msg is BarEnvelope {
  return msg.type === 'bar'
}

export function isTick(msg: WebSocketMessages): msg is TickEnvelope {
  return msg.type === 'tick'
}

export function isTrade(msg: WebSocketMessages): msg is TradeEnvelope {
  return msg.type === 'trade'
}

export function isOrder(msg: WebSocketMessages): msg is OrderStatusEnvelope {
  return msg.type === 'order_status'
}

export function isExecution(msg: WebSocketMessages): msg is FillEnvelope {
  return msg.type === 'fill'
}

export function isSignal(msg: WebSocketMessages): msg is SignalEnvelope {
  return msg.type === 'signal'
}

export function isHeartbeat(msg: WebSocketMessages): msg is HeartbeatEnvelope {
  return msg.type === 'heartbeat'
}
