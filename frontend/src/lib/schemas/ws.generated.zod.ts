/**
 * Generated Zod schemas for WebSocket message validation.
 * DO NOT EDIT - regenerate with: make ui-gen-zod
 */

import { z } from 'zod/v4'

export const WsMessageBaseSchema = z
  .object({
    type: z.string(),
    timestamp: z.string().datetime().optional(),
  })
  .strict()

export const BarEnvelopeSchema = z
  .object({
    type: z.literal('bar'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    instrument: z.string(),
    timeframe: z.string(),
    open: z.number(),
    high: z.number(),
    low: z.number(),
    close: z.number(),
    volume: z.number(),
    vwap: z.number().nullable(),
    trades: z.number().int().nullable(),
    exchange: z.string(),
  })
  .strict()

export const FillEnvelopeSchema = z
  .object({
    type: z.literal('fill'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    trade_id: z.string().nullable(),
    exchange_order_id: z.string().nullable(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.string(),
    side: z.enum(['buy', 'sell']),
    size: z.number(),
    price: z.number(),
    fee: z.number(),
    fee_asset: z.string(),
    status: z.enum(['filled', 'partial', 'rejected', 'cancelled']),
    executed_at: z.string().datetime().optional(),
  })
  .strict()

export const HeartbeatEnvelopeSchema = z
  .object({
    type: z.literal('heartbeat'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    component: z.string(),
    sequence: z.number().int(),
    status: z.enum(['healthy', 'warning', 'error']),
    lag_ms: z.number().int(),
  })
  .strict()

export const OrderCancelEnvelopeSchema = z
  .object({
    type: z.literal('order_cancel'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
  })
  .strict()

export const OrderEventEnvelopeSchema = z
  .object({
    type: z.literal('order_event'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    event: z.enum(['submitted', 'accepted', 'rejected', 'cancelled', 'expired', 'replaced']),
    reason: z.string().nullable(),
  })
  .strict()

export const OrderReplaceEnvelopeSchema = z
  .object({
    type: z.literal('order_replace'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
    new_quantity: z.number().nullable(),
    new_price: z.number().nullable(),
  })
  .strict()

export const OrderRequestEnvelopeSchema = z
  .object({
    type: z.literal('order_req'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    strategy_id: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    mode: z.enum(['live', 'paper']),
    side: z.enum(['buy', 'sell']),
    order_type: z.enum(['market', 'limit', 'stop', 'stop_limit']),
    quantity: z.number(),
    price: z.number().nullable(),
    client_order_id: z.string(),
    signaled_at: z.string().datetime().nullable(),
  })
  .strict()

export const OrderStatusEnvelopeSchema = z
  .object({
    type: z.literal('order_status'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    exchange_order_id: z.string().nullable(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.string(),
    side: z.enum(['buy', 'sell']),
    status: z.enum(['submitted', 'accepted', 'rejected', 'cancelled', 'expired', 'replaced']),
    order_type: z.enum(['market', 'limit', 'stop', 'stop_limit']),
    size: z.number(),
    filled_size: z.number(),
    price: z.number().nullable(),
    average_price: z.number().nullable(),
    reason: z.string().nullable(),
    created_at: z.string().datetime().optional(),
    updated_at: z.string().datetime().nullable(),
  })
  .strict()

export const ReplayEndEnvelopeSchema = z
  .object({
    type: z.literal('replay_end'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
  })
  .strict()

export const ReplayStartEnvelopeSchema = z
  .object({
    type: z.literal('replay_start'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    started_at: z.string().datetime().nullable(),
  })
  .strict()

export const SettingChangedEnvelopeSchema = z
  .object({
    type: z.literal('setting_changed'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    key: z.string(),
    value: z.string(),
    category: z.string(),
    updated_by: z.string().nullable(),
  })
  .strict()

export const SignalEnvelopeSchema = z
  .object({
    type: z.literal('signal'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    instrument: z.string(),
    side: z.enum(['buy', 'sell']),
    strength: z.number(),
    reason: z.string(),
    price: z.number().nullable(),
    strategy_name: z.string().nullable(),
    id: z.string().nullable(),
    exchange: z.string(),
  })
  .strict()

export const SymbolMappingUpdateEnvelopeSchema = z
  .object({
    type: z.literal('symbol_mapping_update'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    event: z.literal('symbol_mappings_updated'),
    action: z.literal('clear_cache'),
  })
  .strict()

export const TickEnvelopeSchema = z
  .object({
    type: z.literal('tick'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    instrument: z.string(),
    volume: z.number(),
    bid: z.number().nullable(),
    ask: z.number().nullable(),
    last: z.number().nullable(),
    exchange: z.string(),
  })
  .strict()

export const TradeEnvelopeSchema = z
  .object({
    type: z.literal('trade'),
    timestamp: z.string().datetime().optional(),
    meta: z.record(z.string(), z.unknown()).optional(),
    instrument: z.string(),
    price: z.number(),
    volume: z.number(),
    side: z.string().nullable(),
    exchange: z.string(),
  })
  .strict()

export const WSAuthCompleteResponseSchema = z
  .object({
    type: z.literal('auth_complete'),
    timestamp: z.string().datetime().optional(),
    available_topics: z.array(z.string()),
    user_role: z.string(),
    session_expires_at: z.string().datetime().nullable(),
    ws_token_exp: z.string().datetime(),
  })
  .strict()

export const WSAuthExpiredResponseSchema = z
  .object({
    type: z.literal('auth_expired'),
    timestamp: z.string().datetime().optional(),
  })
  .strict()

export const WSAuthFailedResponseSchema = z
  .object({
    type: z.literal('auth_failed'),
    timestamp: z.string().datetime().optional(),
    reason: z.string().nullable(),
  })
  .strict()

export const WSAuthOkResponseSchema = z
  .object({
    type: z.literal('auth_ok'),
    timestamp: z.string().datetime().optional(),
    exp: z.string().datetime(),
  })
  .strict()

export const WSAuthRequiredResponseSchema = z
  .object({
    type: z.literal('auth_required'),
    timestamp: z.string().datetime().optional(),
    timeout: z.number().int(),
  })
  .strict()

export const WSAuthenticateRequestSchema = z
  .object({
    type: z.literal('authenticate'),
    timestamp: z.string().datetime().optional(),
    ws_token: z.string(),
  })
  .strict()

export const WSErrorResponseSchema = z
  .object({
    type: z.literal('error'),
    timestamp: z.string().datetime().optional(),
    message: z.string(),
  })
  .strict()

export const WSGetSubscriptionsRequestSchema = z
  .object({
    type: z.literal('get_subscriptions'),
    timestamp: z.string().datetime().optional(),
  })
  .strict()

export const WSGetTopicSuggestionsRequestSchema = z
  .object({
    type: z.literal('get_topic_suggestions'),
    timestamp: z.string().datetime().optional(),
    prefix: z.string(),
  })
  .strict()

export const WSPingRequestSchema = z
  .object({
    type: z.literal('ping'),
    timestamp: z.string().datetime().optional(),
  })
  .strict()

export const WSPongResponseSchema = z
  .object({
    type: z.literal('pong'),
    timestamp: z.string().datetime(),
    active_connections: z.number().int(),
  })
  .strict()

export const WSReauthOkResponseSchema = z
  .object({
    type: z.literal('reauth_ok'),
    timestamp: z.string().datetime().optional(),
    exp: z.string().datetime(),
  })
  .strict()

export const WSReauthRequestSchema = z
  .object({
    type: z.literal('reauth'),
    timestamp: z.string().datetime().optional(),
    ws_token: z.string(),
  })
  .strict()

export const WSReauthRequiredResponseSchema = z
  .object({
    type: z.literal('reauth_required'),
    timestamp: z.string().datetime().optional(),
    deadline: z.string().datetime(),
  })
  .strict()

export const WSSubscribeRequestSchema = z
  .object({
    type: z.literal('subscribe'),
    timestamp: z.string().datetime().optional(),
    topics: z.array(z.string()),
  })
  .strict()

export const WSSubscriptionSuccessResponseSchema = z
  .object({
    type: z.literal('subscription_success'),
    timestamp: z.string().datetime().optional(),
    action: z.enum(['subscribe', 'unsubscribe']),
    status: z.enum(['subscribed', 'unsubscribed', 'partial', 'denied', 'no_topics']),
    topics: z.array(z.string()),
    denied_topics: z.array(z.string()).optional(),
    active_subscriptions: z.array(z.string()),
    zmq_topics: z.array(z.string()).optional(),
    message: z.string().nullable(),
  })
  .strict()

export const WSSubscriptionsListResponseSchema = z
  .object({
    type: z.literal('subscriptions_list'),
    timestamp: z.string().datetime().optional(),
    subscriptions: z.array(z.string()),
    available_topics: z.array(z.string()),
    total_available: z.number().int(),
  })
  .strict()

export const WSTopicSuggestionsResponseSchema = z
  .object({
    type: z.literal('topic_suggestions'),
    timestamp: z.string().datetime().optional(),
    prefix: z.string(),
    suggestions: z.array(z.string()),
  })
  .strict()

export const WSUnsubscribeRequestSchema = z
  .object({
    type: z.literal('unsubscribe'),
    timestamp: z.string().datetime().optional(),
    topics: z.array(z.string()),
  })
  .strict()
