/**
 * Generated Zod schemas for WebSocket message validation.
 * DO NOT EDIT - regenerate with: make ui-gen-zod
 */

import { z } from 'zod/v4'

export const WsMessageBaseSchema = z
  .object({
    type: z.string(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
  })
  .strict()

export const CandleDataSchema = z
  .object({
    type: z.literal('candle'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    type: z.literal('execution'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    executed_at: z.iso.datetime(),
  })
  .strict()

export const JsonPrimitiveSchema = z.unknown()

export const OrderCancelDataSchema = z
  .object({
    type: z.literal('order_cancel'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    instrument: z.string(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
  })
  .strict()

export const OrderDataSchema = z
  .object({
    type: z.literal('order'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    created_at: z.iso.datetime(),
    updated_at: z.iso.datetime().nullable(),
  })
  .strict()

export const OrderEventDataSchema = z
  .object({
    type: z.literal('order_event'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    type: z.literal('order_replace'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    type: z.literal('order_request'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    type: z.literal('position'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    type: z.literal('replay_end'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
  })
  .strict()

export const ReplayStartDataSchema = z
  .object({
    type: z.literal('replay_start'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    started_at: z.iso.datetime().nullable(),
  })
  .strict()

export const SettingChangedDataSchema = z
  .object({
    type: z.literal('setting_changed'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    key: z.string(),
    value: z.string(),
    category: z.string(),
    updated_by: z.string().nullable(),
  })
  .strict()

export const SignalDataSchema = z
  .object({
    type: z.literal('signal'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
    side: z.enum(['buy', 'sell']),
    strength: z.number(),
    reason: z.string(),
    price: z.number().nullable(),
    strategy_name: z.string().nullable(),
    fired_at: z.iso.datetime(),
  })
  .strict()

export const SymbolAliasUpdateDataSchema = z
  .object({
    type: z.literal('symbol_alias_update'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    event: z.literal('symbol_aliases_updated'),
    action: z.literal('clear_cache'),
  })
  .strict()

export const TickDataSchema = z
  .object({
    type: z.literal('tick'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    type: z.literal('trade'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
  })
  .strict()

export const WSAuthFailedResponseSchema = z
  .object({
    type: z.literal('auth_failed'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    reason: z.string().nullable(),
  })
  .strict()

export const WSAuthOkResponseSchema = z
  .object({
    type: z.literal('auth_ok'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    exp: z.iso.datetime(),
  })
  .strict()

export const WSAuthRequiredResponseSchema = z
  .object({
    type: z.literal('auth_required'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    timeout: z.number().int(),
  })
  .strict()

export const WSAuthenticateRequestSchema = z
  .object({
    type: z.literal('authenticate'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    ws_token: z.string(),
  })
  .strict()

export const WSErrorResponseSchema = z
  .object({
    type: z.literal('error'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    message: z.string(),
  })
  .strict()

export const WSGetSubscriptionsRequestSchema = z
  .object({
    type: z.literal('get_subscriptions'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
  })
  .strict()

export const WSPingRequestSchema = z
  .object({
    type: z.literal('ping'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
  })
  .strict()

export const WSPongResponseSchema = z
  .object({
    type: z.literal('pong'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    active_connections: z.number().int(),
  })
  .strict()

export const WSReauthOkResponseSchema = z
  .object({
    type: z.literal('reauth_ok'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    exp: z.iso.datetime(),
  })
  .strict()

export const WSReauthRequestSchema = z
  .object({
    type: z.literal('reauth'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    ws_token: z.string(),
  })
  .strict()

export const WSReauthRequiredResponseSchema = z
  .object({
    type: z.literal('reauth_required'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    deadline: z.iso.datetime(),
  })
  .strict()

export const WSSubscribeRequestSchema = z
  .object({
    type: z.literal('subscribe'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topics: z.array(z.string()),
  })
  .strict()

export const WSSubscriptionSuccessResponseSchema = z
  .object({
    type: z.literal('subscription_success'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    action: z.enum(['subscribe', 'unsubscribe']),
    status: z.enum(['subscribed', 'unsubscribed', 'partial', 'denied', 'no_topics']),
    topics: z.array(z.string()),
    denied_topics: z.array(z.string()),
    active_subscriptions: z.array(z.string()),
    message: z.string().nullable(),
  })
  .strict()

export const WSSubscriptionsListResponseSchema = z
  .object({
    type: z.literal('subscriptions_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    subscriptions: z.array(z.string()),
    available_topics: z.array(z.string()),
    total_available: z.number().int(),
  })
  .strict()

export const WSUnsubscribeRequestSchema = z
  .object({
    type: z.literal('unsubscribe'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topics: z.array(z.string()),
  })
  .strict()

export const JsonValueSchema = z.unknown()

export const WSAuthCompleteResponseSchema = z
  .object({
    type: z.literal('auth_complete'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    available_topics: z.array(z.string()),
    user_role: UserRoleSchema,
    session_expires_at: z.iso.datetime().nullable(),
    ws_token_exp: z.iso.datetime(),
  })
  .strict()

export const JsonObjectSchema = z.record(z.string(), z.any())

export const HeartbeatDataSchema = z
  .object({
    type: z.literal('heartbeat'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    component: z.string(),
    sequence: z.number().int(),
    status: z.enum(['healthy', 'warning', 'error']),
    lag_ms: z.number().int(),
    meta: z.record(z.string(), z.any()),
  })
  .strict()
