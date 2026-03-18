/**
 * Generated Zod schemas for WebSocket message validation.
 * DO NOT EDIT - regenerate with: make ui-gen-zod
 */

import { z } from 'zod/v4'

export const WsMessageBaseSchema = z
  .object({
    type: z.string(),
    timestamp: z.iso.datetime().optional(),
  })
  .strict()

export const CandleDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('candle'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    instrument: z.string(),
    exchange: z.enum(['kraken', 'zonda', 'walutomat', 'polygon']),
    timeframe: z.string(),
    open_at: z.iso.datetime(),
    open: z.number(),
    high: z.number(),
    low: z.number(),
    close: z.number(),
    volume: z.number(),
    vwap: z.number().nullable(),
    trades: z.number().int().nullable(),
  })
  .strict()

export const ExecutionDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('execution'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    trade_id: z.string().nullable(),
    exchange_order_id: z.string().nullable(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    side: z.enum(['buy', 'sell']),
    size: z.number(),
    price: z.number(),
    fee: z.number(),
    fee_asset: z.string(),
    status: z.enum(['filled', 'partial']),
    executed_at: z.iso.datetime().optional(),
  })
  .strict()

export const HeartbeatDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('heartbeat'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    component: z.string(),
    sequence: z.number().int(),
    status: z.enum(['healthy', 'warning', 'error']),
    lag_ms: z.number().int(),
    meta: z.record(z.string(), z.unknown()).optional(),
  })
  .strict()

export const OrderCancelDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('order_cancel'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
  })
  .strict()

export const OrderDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('order'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    exchange_order_id: z.string().nullable(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    side: z.enum(['buy', 'sell']),
    status: z.string(),
    order_type: z.enum(['market', 'limit', 'stop', 'stop_limit']),
    size: z.number(),
    filled_size: z.number(),
    price: z.number().nullable(),
    average_price: z.number().nullable(),
    reason: z.string().nullable(),
    time_in_force: z.string().nullable(),
    error: z.string().nullable(),
    created_at: z.iso.datetime().optional(),
    updated_at: z.iso.datetime().nullable(),
  })
  .strict()

export const OrderEventDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('order_event'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    event: z.enum(['submitted', 'accepted', 'rejected', 'cancelled', 'expired', 'replaced']),
    reason: z.string().nullable(),
  })
  .strict()

export const OrderReplaceDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('order_replace'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
    new_quantity: z.number().nullable(),
    new_price: z.number().nullable(),
  })
  .strict()

export const OrderRequestDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('order_request'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    strategy_id: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    mode: z.enum(['live', 'paper']),
    side: z.enum(['buy', 'sell']),
    order_type: z.enum(['market', 'limit', 'stop', 'stop_limit']),
    quantity: z.number(),
    price: z.number().nullable(),
    client_order_id: z.string(),
    signaled_at: z.iso.datetime().nullable(),
  })
  .strict()

export const PositionDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('position'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    quantity: z.number(),
    average_price: z.number(),
    unrealized_pnl: z.number(),
    realized_pnl: z.number(),
  })
  .strict()

export const ReplayEndDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('replay_end'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
  })
  .strict()

export const ReplayStartDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('replay_start'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    started_at: z.iso.datetime().nullable(),
  })
  .strict()

export const SettingChangedDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('setting_changed'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    key: z.string(),
    value: z.string(),
    category: z.string(),
    updated_by: z.string().nullable(),
  })
  .strict()

export const SignalDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('signal'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    side: z.enum(['buy', 'sell']),
    strength: z.number(),
    reason: z.string(),
    price: z.number().nullable(),
    strategy_name: z.string().nullable(),
    fired_at: z.iso.datetime().optional(),
  })
  .strict()

export const SymbolAliasUpdateDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('symbol_alias_update'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    event: z.literal('symbol_aliases_updated'),
    action: z.literal('clear_cache'),
  })
  .strict()

export const TickDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('tick'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    instrument: z.string(),
    exchange: z.enum(['kraken', 'zonda', 'walutomat', 'polygon']),
    volume: z.number(),
    bid: z.number().nullable(),
    ask: z.number().nullable(),
    last: z.number().nullable(),
  })
  .strict()

export const TradeDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('trade'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    instrument: z.string(),
    exchange: z.enum(['kraken', 'zonda', 'walutomat', 'polygon']),
    executed_at: z.iso.datetime().nullable(),
    price: z.number(),
    volume: z.number(),
    side: z.string().nullable(),
  })
  .strict()

export const UserRoleSchema = z.enum(['viewer', 'operator', 'admin'])

export const WSAuthExpiredResponseSchema = z
  .object({
    type: z.literal('auth_expired'),
    timestamp: z.iso.datetime().optional(),
  })
  .strict()

export const WSAuthFailedResponseSchema = z
  .object({
    type: z.literal('auth_failed'),
    timestamp: z.iso.datetime().optional(),
    reason: z.string().nullable(),
  })
  .strict()

export const WSAuthOkResponseSchema = z
  .object({
    type: z.literal('auth_ok'),
    timestamp: z.iso.datetime().optional(),
    exp: z.iso.datetime(),
  })
  .strict()

export const WSAuthRequiredResponseSchema = z
  .object({
    type: z.literal('auth_required'),
    timestamp: z.iso.datetime().optional(),
    timeout: z.number().int(),
  })
  .strict()

export const WSAuthenticateRequestSchema = z
  .object({
    type: z.literal('authenticate'),
    timestamp: z.iso.datetime().optional(),
    ws_token: z.string(),
  })
  .strict()

export const WSErrorResponseSchema = z
  .object({
    type: z.literal('error'),
    timestamp: z.iso.datetime().optional(),
    message: z.string(),
  })
  .strict()

export const WSGetSubscriptionsRequestSchema = z
  .object({
    type: z.literal('get_subscriptions'),
    timestamp: z.iso.datetime().optional(),
  })
  .strict()

export const WSGetTopicSuggestionsRequestSchema = z
  .object({
    type: z.literal('get_topic_suggestions'),
    timestamp: z.iso.datetime().optional(),
    prefix: z.string(),
  })
  .strict()

export const WSPingRequestSchema = z
  .object({
    type: z.literal('ping'),
    timestamp: z.iso.datetime().optional(),
  })
  .strict()

export const WSPongResponseSchema = z
  .object({
    type: z.literal('pong'),
    timestamp: z.iso.datetime(),
    active_connections: z.number().int(),
  })
  .strict()

export const WSReauthOkResponseSchema = z
  .object({
    type: z.literal('reauth_ok'),
    timestamp: z.iso.datetime().optional(),
    exp: z.iso.datetime(),
  })
  .strict()

export const WSReauthRequestSchema = z
  .object({
    type: z.literal('reauth'),
    timestamp: z.iso.datetime().optional(),
    ws_token: z.string(),
  })
  .strict()

export const WSReauthRequiredResponseSchema = z
  .object({
    type: z.literal('reauth_required'),
    timestamp: z.iso.datetime().optional(),
    deadline: z.iso.datetime(),
  })
  .strict()

export const WSSubscribeRequestSchema = z
  .object({
    type: z.literal('subscribe'),
    timestamp: z.iso.datetime().optional(),
    topics: z.array(z.string()),
  })
  .strict()

export const WSSubscriptionSuccessResponseSchema = z
  .object({
    type: z.literal('subscription_success'),
    timestamp: z.iso.datetime().optional(),
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
    timestamp: z.iso.datetime().optional(),
    subscriptions: z.array(z.string()),
    available_topics: z.array(z.string()),
    total_available: z.number().int(),
  })
  .strict()

export const WSTopicSuggestionsResponseSchema = z
  .object({
    type: z.literal('topic_suggestions'),
    timestamp: z.iso.datetime().optional(),
    prefix: z.string(),
    suggestions: z.array(z.string()),
  })
  .strict()

export const WSUnsubscribeRequestSchema = z
  .object({
    type: z.literal('unsubscribe'),
    timestamp: z.iso.datetime().optional(),
    topics: z.array(z.string()),
  })
  .strict()

export const WSAuthCompleteResponseSchema = z
  .object({
    type: z.literal('auth_complete'),
    timestamp: z.iso.datetime().optional(),
    available_topics: z.array(z.string()),
    user_role: UserRoleSchema,
    session_expires_at: z.iso.datetime().nullable(),
    ws_token_exp: z.iso.datetime(),
  })
  .strict()
