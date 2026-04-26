import { z } from 'zod/v4'

export {
  TickDataSchema as tickSchema,
  CandleDataSchema as candleSchema,
  TradeDataSchema as tradeSchema,
  SignalDataSchema as signalSchema,
  OrderDataSchema as orderSchema,
  OrderCancelDataSchema as orderCancelSchema,
  OrderEventDataSchema as orderEventSchema,
  OrderReplaceDataSchema as orderReplaceSchema,
  OrderRequestDataSchema as orderRequestSchema,
  ExecutionDataSchema as executionSchema,
  HeartbeatDataSchema as heartbeatSchema,
  PositionDataSchema as positionSchema,
  ReplayEndDataSchema as replayEndSchema,
  ReplayStartDataSchema as replayStartSchema,
  SettingChangedDataSchema as settingChangedSchema,
  SymbolAliasUpdateDataSchema as symbolAliasUpdateSchema,
  AiReviewCapsViolationFrameDataSchema as aiReviewCapsViolationFrameSchema,
  AiReviewRequestFrameDataSchema as aiReviewRequestFrameSchema,
  AiReviewDecisionAckFrameDataSchema as aiReviewDecisionAckFrameSchema,
  WSAuthRequiredResponseSchema as authRequiredMessageSchema,
  WSAuthOkResponseSchema as authOkMessageSchema,
  WSAuthFailedResponseSchema as authFailedMessageSchema,
  WSAuthCompleteResponseSchema as authCompleteMessageSchema,
  WSAuthExpiredResponseSchema as authExpiredMessageSchema,
  WSReauthRequiredResponseSchema as reauthRequiredMessageSchema,
  WSReauthOkResponseSchema as reauthOkMessageSchema,
  WSErrorResponseSchema as errorMessageSchema,
  WSSubscribeRequestSchema as subscribeRequestSchema,
  WSUnsubscribeRequestSchema as unsubscribeRequestSchema,
  WSSubscriptionSuccessResponseSchema as subscriptionSuccessResponseSchema,
  WSSubscriptionsListResponseSchema as subscriptionsListResponseSchema,
  WSPongResponseSchema as pongMessageSchema,
} from './ws.generated.zod'
import {
  TickDataSchema,
  CandleDataSchema,
  TradeDataSchema,
  SignalDataSchema,
  OrderDataSchema,
  OrderCancelDataSchema,
  OrderEventDataSchema,
  OrderReplaceDataSchema,
  OrderRequestDataSchema,
  ExecutionDataSchema,
  HeartbeatDataSchema,
  PositionDataSchema,
  ReplayEndDataSchema,
  ReplayStartDataSchema,
  SettingChangedDataSchema,
  SymbolAliasUpdateDataSchema,
  AiReviewCapsViolationFrameDataSchema,
  AiReviewRequestFrameDataSchema,
  AiReviewDecisionAckFrameDataSchema,
  WSAuthRequiredResponseSchema,
  WSAuthOkResponseSchema,
  WSAuthFailedResponseSchema,
  WSAuthCompleteResponseSchema,
  WSAuthExpiredResponseSchema,
  WSReauthRequiredResponseSchema,
  WSReauthOkResponseSchema,
  WSErrorResponseSchema,
  WSSubscriptionSuccessResponseSchema,
  WSSubscriptionsListResponseSchema,
  WSPongResponseSchema,
} from './ws.generated.zod'

export const wsMessageUnionSchema = z.discriminatedUnion('type', [
  TickDataSchema,
  CandleDataSchema,
  TradeDataSchema,
  SignalDataSchema,
  OrderDataSchema,
  OrderCancelDataSchema,
  OrderEventDataSchema,
  OrderReplaceDataSchema,
  OrderRequestDataSchema,
  ExecutionDataSchema,
  HeartbeatDataSchema,
  PositionDataSchema,
  ReplayEndDataSchema,
  ReplayStartDataSchema,
  SettingChangedDataSchema,
  SymbolAliasUpdateDataSchema,
  AiReviewCapsViolationFrameDataSchema,
  AiReviewRequestFrameDataSchema,
  AiReviewDecisionAckFrameDataSchema,
  WSAuthRequiredResponseSchema,
  WSAuthOkResponseSchema,
  WSAuthFailedResponseSchema,
  WSAuthCompleteResponseSchema,
  WSAuthExpiredResponseSchema,
  WSReauthRequiredResponseSchema,
  WSReauthOkResponseSchema,
  WSErrorResponseSchema,
  WSSubscriptionSuccessResponseSchema,
  WSSubscriptionsListResponseSchema,
  WSPongResponseSchema,
])
export const wsMessageBaseSchema = z.looseObject({
  type: z.string(),
  timestamp: z.string().optional(),
})
export type WsMessageBase = z.infer<typeof wsMessageBaseSchema>
export type WsMessageUnion = z.infer<typeof wsMessageUnionSchema>
export type Tick = z.infer<typeof TickDataSchema>
export type Candle = z.infer<typeof CandleDataSchema>
export type Trade = z.infer<typeof TradeDataSchema>
export type Signal = z.infer<typeof SignalDataSchema>
export type Order = z.infer<typeof OrderDataSchema>
export type Execution = z.infer<typeof ExecutionDataSchema>
export type Heartbeat = z.infer<typeof HeartbeatDataSchema>
export type AiReviewCapsViolation = z.infer<typeof AiReviewCapsViolationFrameDataSchema>
export type AiReviewRequest = z.infer<typeof AiReviewRequestFrameDataSchema>
export type AiReviewDecisionAck = z.infer<typeof AiReviewDecisionAckFrameDataSchema>
const KNOWN_MESSAGE_TYPES = new Set([
  'tick',
  'candle',
  'trade',
  'signal',
  'order',
  'order_cancel',
  'order_event',
  'order_replace',
  'order_request',
  'execution',
  'heartbeat',
  'position',
  'replay_end',
  'replay_start',
  'setting_changed',
  'symbol_alias_update',
  'ai_review.caps_violation',
  'ai_review.decision_ack',
  'ai_review.request',
  'auth_expired',
  'auth_failed',
  'auth_ok',
  'auth_required',
  'auth_complete',
  'reauth_required',
  'reauth_ok',
  'error',
  'pong',
  'subscribe',
  'subscribed',
  'unsubscribe',
  'unsubscribed',
  'subscription_success',
  'subscriptions_list',
])

export function parseWsMessage(raw: unknown): WsMessageUnion | null {
  const unionResult = wsMessageUnionSchema.safeParse(raw)

  if (unionResult.success) {
    return unionResult.data
  }

  const rawType =
    typeof raw === 'object' &&
    raw !== null &&
    'type' in raw &&
    typeof (raw as { type: unknown }).type === 'string'
      ? (raw as { type: string }).type
      : '<no type>'

  if (KNOWN_MESSAGE_TYPES.has(rawType)) {
    console.error(
      `Schema validation FAILED for message type "${rawType}" - message BLOCKED:`,
      unionResult.error.issues
    )
  } else {
    console.warn(`Unknown message type "${rawType}" - message BLOCKED`)
  }

  return null
}
