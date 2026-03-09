import { z } from 'zod/v4'

export {
  TickEnvelopeSchema as tickSchema,
  CandleEnvelopeSchema as candleSchema,
  TradeEnvelopeSchema as tradeSchema,
  SignalEnvelopeSchema as signalSchema,
  OrderStatusEnvelopeSchema as orderStatusSchema,
  FillEnvelopeSchema as fillSchema,
  HeartbeatEnvelopeSchema as heartbeatSchema,
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
  WSTopicSuggestionsResponseSchema as topicSuggestionsResponseSchema,
  WSPongResponseSchema as pongMessageSchema,
} from './ws.generated.zod'
import {
  TickEnvelopeSchema,
  CandleEnvelopeSchema,
  TradeEnvelopeSchema,
  SignalEnvelopeSchema,
  OrderStatusEnvelopeSchema,
  FillEnvelopeSchema,
  HeartbeatEnvelopeSchema,
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
  WSTopicSuggestionsResponseSchema,
  WSPongResponseSchema,
} from './ws.generated.zod'

export const wsMessageUnionSchema = z.discriminatedUnion('type', [
  TickEnvelopeSchema,
  CandleEnvelopeSchema,
  TradeEnvelopeSchema,
  SignalEnvelopeSchema,
  OrderStatusEnvelopeSchema,
  FillEnvelopeSchema,
  HeartbeatEnvelopeSchema,
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
  WSTopicSuggestionsResponseSchema,
  WSPongResponseSchema,
])
export const wsMessageBaseSchema = z.looseObject({
  type: z.string(),
  timestamp: z.string().optional(),
})
export type WsMessageBase = z.infer<typeof wsMessageBaseSchema>
export type WsMessageUnion = z.infer<typeof wsMessageUnionSchema>
export type Tick = z.infer<typeof TickEnvelopeSchema>
export type Candle = z.infer<typeof CandleEnvelopeSchema>
export type Trade = z.infer<typeof TradeEnvelopeSchema>
export type Signal = z.infer<typeof SignalEnvelopeSchema>
export type OrderStatus = z.infer<typeof OrderStatusEnvelopeSchema>
export type Fill = z.infer<typeof FillEnvelopeSchema>
export type Heartbeat = z.infer<typeof HeartbeatEnvelopeSchema>
const KNOWN_MESSAGE_TYPES = new Set([
  'tick',
  'candle',
  'trade',
  'signal',
  'order_status',
  'fill',
  'heartbeat',
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
  'topic_suggestions',
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
