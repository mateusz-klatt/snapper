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
    topic: z.string().nullable().optional(),
  })
  .strict()

export const AiReviewCapsViolationFrameDataSchema = z
  .object({
    type: z.literal('ai_review.caps_violation'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    review_public_id: z.string(),
    user_public_id: z.string(),
    strategy_public_id: z.string(),
    wallet_public_id: z.string(),
    instrument_public_id: z.string(),
    cap_type: z.string(),
    attempted: z.number(),
    limit: z.number(),
    dispatch_version: z.number().int(),
  })
  .strict()

export const AiReviewDecisionAckFrameDataSchema = z
  .object({
    type: z.literal('ai_review.decision_ack'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    review_public_id: z.string(),
    user_public_id: z.string(),
    strategy_public_id: z.string(),
    wallet_public_id: z.string(),
    instrument_public_id: z.string(),
    responding_delegate_public_id: z.string(),
    decision: z.string(),
    new_status: z.string(),
    resolution_mode: z.string(),
    rationale: z.string().nullable(),
    dispatch_version: z.number().int(),
  })
  .strict()

export const AiReviewDecisionDataSchema = z
  .object({
    type: z.literal('ai_review_decision'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    review_public_id: z.string(),
    responding_delegate_public_id: z.string(),
    decision: z.string(),
    new_status: z.string(),
    resolution_mode: z.string(),
    dispatch_version: z.number().int(),
  })
  .strict()

export const JsonPrimitiveSchema = z.unknown()

export const BacktestProgressDataSchema = z
  .object({
    type: z.literal('backtest_progress'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    run_public_id: z.string(),
    wallet_public_id: z.string(),
    event: z.enum(['started', 'progress', 'milestone', 'completed', 'failed', 'cancelled']),
    milestone: z.enum(['25pct', '50pct', '75pct']).nullable().optional(),
    candles_done: z.number().int(),
    total_candles: z.number().int().nullable(),
    signals_count: z.number().int(),
    trades_count: z.number().int(),
    equity: z.number(),
    progress_pct: z.number(),
  })
  .strict()

export const CandleDataSchema = z
  .object({
    type: z.literal('candle'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument: z.string(),
    exchange: z.enum(['kraken', 'kraken_futures', 'kraken_equities', 'walutomat', 'polygon']),
    timeframe: z.string(),
    open_at: z.iso.datetime(),
    open: z.number(),
    high: z.number(),
    low: z.number(),
    close: z.number(),
    volume: z.number(),
    vwap: z.number().nullable().optional(),
    trades: z.number().int().nullable().optional(),
  })
  .strict()

export const CapsViolationAfterAiApproveDataSchema = z
  .object({
    type: z.literal('caps_violation_after_ai_approve'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    review_public_id: z.string(),
    user_public_id: z.string(),
    strategy_public_id: z.string(),
    wallet_public_id: z.string(),
    instrument_public_id: z.string(),
    cap_type: z.string(),
    attempted: z.number(),
    limit: z.number(),
    dispatch_version: z.number().int(),
  })
  .strict()

export const ContinuousCandleDataSchema = z
  .object({
    type: z.literal('continuous_candle'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    open_at: z.iso.datetime(),
    timeframe: z.string(),
    open: z.number(),
    high: z.number(),
    low: z.number(),
    close: z.number(),
    volume: z.number(),
    vwap: z.number().nullable(),
    trades: z.number().int().nullable(),
    source_contract: z.string(),
    adjustment_factor: z.number().nullable(),
  })
  .strict()

export const ContractDataSchema = z
  .object({
    type: z.literal('contract'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument_public_id: z.string(),
    native_symbol: z.string(),
    exchange: z.string(),
    expiry_at: z.iso.datetime().nullable(),
    instrument_kind: z.string().nullable(),
    relationship_type: z.string(),
    contract_family: z.string().nullable(),
    is_front_month: z.boolean(),
  })
  .strict()

export const DelegateOfflineDataSchema = z
  .object({
    type: z.literal('delegate_offline'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    user_public_id: z.string(),
    delegate_public_id: z.string(),
    last_seen_at: z.iso.datetime(),
  })
  .strict()

export const ExecutionDataSchema = z
  .object({
    type: z.literal('execution'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    trade_id: z.string().nullable().optional(),
    exchange_order_id: z.string().nullable().optional(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    side: z.enum(['buy', 'sell']),
    size: z.number(),
    price: z.number(),
    last_size: z.number(),
    last_price: z.number(),
    fee: z.number(),
    fee_asset: z.string(),
    status: z.enum(['filled', 'partial']),
    executed_at: z.iso.datetime(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    user_public_id: z.string().nullable().optional(),
    liquidity_role: z.string(),
  })
  .strict()

export const ExecutionPlanDataSchema = z
  .object({
    type: z.literal('execution_plan'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    plan_type: z.string(),
    status: z.string(),
    instrument_public_id: z.string(),
    exchange: z.string(),
    mode: z.string(),
    side: z.string(),
    total_quantity: z.number(),
    filled_quantity: z.number(),
    created_at: z.iso.datetime(),
    created_via: z.string(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable(),
    params: z.record(z.string(), z.unknown()),
    position_cycle_public_id: z.string().nullable(),
    parent_plan_public_id: z.string().nullable(),
    last_error: z.string().nullable(),
    idempotency_key: z.string().nullable(),
  })
  .strict()

export const ExecutionPlanDecisionEventDataSchema = z
  .object({
    type: z.literal('execution_plan_decision_event'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    decision_public_id: z.string(),
    plan_public_id: z.string(),
    decision_type: z.string(),
    trigger_type: z.string(),
    reason: z.string(),
    triggered_at: z.iso.datetime(),
  })
  .strict()

export const FrontMonthDataSchema = z
  .object({
    type: z.literal('front_month'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument_public_id: z.string(),
    native_symbol: z.string(),
    exchange: z.string(),
    expiry_at: z.iso.datetime(),
    relationship_type: z.string(),
    contract_family: z.string().nullable(),
  })
  .strict()

export const FundingAccrualDataSchema = z
  .object({
    type: z.literal('funding_accrual'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    mode: z.enum(['live', 'paper']),
    accrual_type: z.enum(['funding', 'rollover', 'borrow']),
    accrued_at: z.iso.datetime(),
    amount: z.number(),
    amount_asset: z.string(),
    rate: z.number(),
    notional: z.number(),
    position_quantity: z.number(),
  })
  .strict()

export const InstrumentCapabilityDataSchema = z
  .object({
    type: z.literal('instrument_capability'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument_public_id: z.string(),
    exchange: z.string(),
    supported_order_types: z.array(z.string()),
    supports_post_only: z.boolean(),
    supports_reduce_only: z.boolean(),
    supports_amend_in_place: z.boolean(),
    supports_native_stop_loss: z.boolean(),
    supports_native_take_profit: z.boolean(),
    supports_trailing_stop_client_side: z.boolean(),
    supports_market_making: z.boolean(),
    supports_short_selling: z.boolean(),
    supports_leverage: z.boolean(),
    max_leverage_long: z.number(),
    max_leverage_short: z.number(),
    min_notional: z.number().nullable(),
    max_order_size: z.number().nullable(),
    top_of_book_quality: z.string(),
  })
  .strict()

export const InstrumentDetailDataSchema = z
  .object({
    type: z.literal('instrument_detail'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument_public_id: z.string(),
    symbol_public_id: z.string(),
    symbol: z.string(),
    exchange: z.string(),
    can_trade: z.boolean(),
    can_market_data: z.boolean(),
    instrument_resolved: z.boolean(),
    instrument_kind: z.string().nullable(),
    expiry_at: z.iso.datetime().nullable(),
  })
  .strict()

export const OrderCancelDataSchema = z
  .object({
    type: z.literal('order_cancel'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    instrument: z.string(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    user_public_id: z.string().nullable().optional(),
  })
  .strict()

export const OrderDataSchema = z
  .object({
    type: z.literal('order'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    exchange_order_id: z.string().nullable().optional(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    mode: z.enum(['live', 'paper']),
    side: z.enum(['buy', 'sell']),
    status: z.string(),
    order_type: z.enum(['market', 'limit', 'stop', 'stop_limit']),
    size: z.number(),
    filled_size: z.number(),
    price: z.number().nullable().optional(),
    average_price: z.number().nullable().optional(),
    reason: z.string().nullable().optional(),
    time_in_force: z.string().nullable().optional(),
    error: z.string().nullable().optional(),
    created_at: z.iso.datetime(),
    updated_at: z.iso.datetime().nullable().optional(),
    leverage: z.number().int().nullable().optional(),
    reduce_only: z.boolean(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    user_public_id: z.string().nullable().optional(),
    plan_public_id: z.string().nullable().optional(),
  })
  .strict()

export const OrderEventDataSchema = z
  .object({
    type: z.literal('order_event'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    instrument: z.string(),
    event: z.enum(['submitted', 'accepted', 'rejected', 'cancelled', 'expired', 'replaced']),
    reason: z.string().nullable().optional(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    user_public_id: z.string().nullable().optional(),
  })
  .strict()

export const OrderReplaceDataSchema = z
  .object({
    type: z.literal('order_replace'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    instrument: z.string(),
    exchange_order_id: z.string(),
    client_order_id: z.string(),
    new_quantity: z.number().nullable().optional(),
    new_price: z.number().nullable().optional(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    user_public_id: z.string().nullable().optional(),
  })
  .strict()

export const OrderRequestDataSchema = z
  .object({
    type: z.literal('order_request'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    strategy_id: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    instrument: z.string(),
    mode: z.enum(['live', 'paper']),
    side: z.enum(['buy', 'sell']),
    order_type: z.enum(['market', 'limit', 'stop', 'stop_limit']),
    quantity: z.number(),
    price: z.number().nullable().optional(),
    client_order_id: z.string(),
    signaled_at: z.iso.datetime().nullable().optional(),
    strategy_tag: z.string().nullable().optional(),
    leverage: z.number().int().nullable().optional(),
    reduce_only: z.boolean(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    user_public_id: z.string().nullable().optional(),
  })
  .strict()

export const PositionDataSchema = z
  .object({
    type: z.literal('position'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument: z.string(),
    instrument_public_id: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    mode: z.enum(['live', 'paper']),
    quantity: z.number(),
    average_price: z.number(),
    unrealized_pnl: z.number(),
    realized_pnl: z.number(),
    position_cycle_public_id: z.string().nullable().optional(),
    wallet_public_id: z.string(),
  })
  .strict()

export const ReplayEndDataSchema = z
  .object({
    type: z.literal('replay_end'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
  })
  .strict()

export const ReplayStartDataSchema = z
  .object({
    type: z.literal('replay_start'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    started_at: z.iso.datetime().nullable().optional(),
  })
  .strict()

export const ScopeRevokedDataSchema = z
  .object({
    type: z.literal('scope_revoked'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    grant_public_id: z.string(),
    operator_public_id: z.string(),
    wallet_public_id: z.string(),
    scope_kind: z.enum(['underlying', 'instrument']),
    underlying_public_id: z.string().nullable().optional(),
    instrument_public_id: z.string().nullable().optional(),
    revoked_at: z.iso.datetime(),
    revoked_by_user_public_id: z.string().nullable().optional(),
    reason: z.string().nullable().optional(),
  })
  .strict()

export const SettingChangedDataSchema = z
  .object({
    type: z.literal('setting_changed'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    key: z.string(),
    value: z.string(),
    category: z.string(),
    updated_by: z.string().nullable().optional(),
  })
  .strict()

export const SignalDataSchema = z
  .object({
    type: z.literal('signal'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'walutomat']),
    side: z.enum(['buy', 'sell']),
    strength: z.number(),
    reason: z.string(),
    price: z.number().nullable().optional(),
    strategy_name: z.string().nullable().optional(),
    fired_at: z.iso.datetime(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    user_public_id: z.string().nullable().optional(),
    ai_review_public_id: z.string().nullable().optional(),
    ai_review_dispatch_version: z.number().int().nullable().optional(),
  })
  .strict()

export const SymbolAliasUpdateDataSchema = z
  .object({
    type: z.literal('symbol_alias_update'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    instrument: z.string(),
    exchange: z.enum(['kraken', 'kraken_futures', 'kraken_equities', 'walutomat', 'polygon']),
    volume: z.number(),
    bid: z.number().nullable().optional(),
    ask: z.number().nullable().optional(),
    last: z.number().nullable().optional(),
    is_delayed: z.boolean(),
    is_extended_hours: z.boolean().nullable().optional(),
  })
  .strict()

export const TradeDataSchema = z
  .object({
    type: z.literal('trade'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument: z.string(),
    exchange: z.enum(['kraken', 'kraken_futures', 'kraken_equities', 'walutomat', 'polygon']),
    executed_at: z.iso.datetime().nullable().optional(),
    price: z.number(),
    volume: z.number(),
    side: z.string().nullable().optional(),
    trade_id: z.string().nullable().optional(),
  })
  .strict()

export const UnderlyingAssetDataSchema = z
  .object({
    type: z.literal('underlying_asset'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    ticker: z.string(),
    name: z.string(),
    asset_class: z.string(),
    sector: z.string().nullable(),
    instrument_count: z.number().int(),
  })
  .strict()

export const UnderlyingInstrumentDataSchema = z
  .object({
    type: z.literal('underlying_instrument'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    instrument_public_id: z.string(),
    native_symbol: z.string(),
    exchange: z.string(),
    asset_type: z.string(),
    relationship_type: z.string(),
    contract_family: z.string().nullable(),
  })
  .strict()

export const UserDeactivatedDataSchema = z
  .object({
    type: z.literal('user_deactivated'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    user_public_id: z.string(),
    deactivated_at: z.iso.datetime(),
    reason: z.string().nullable().optional(),
  })
  .strict()

export const VenueFeeScheduleDataSchema = z
  .object({
    type: z.literal('venue_fee_schedule'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    exchange: z.string(),
    instrument_public_id: z.string().nullable(),
    fee_tier: z.string(),
    maker_bps: z.number(),
    taker_bps: z.number(),
    min_volume_30d: z.number().nullable(),
    currency: z.string(),
  })
  .strict()

export const UserRoleSchema = z.enum(['ai_delegate', 'viewer', 'operator', 'admin'])

export const WSAuthExpiredResponseSchema = z
  .object({
    type: z.literal('auth_expired'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
  })
  .strict()

export const WSAuthFailedResponseSchema = z
  .object({
    type: z.literal('auth_failed'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    reason: z.string().nullable().optional(),
  })
  .strict()

export const WSAuthOkResponseSchema = z
  .object({
    type: z.literal('auth_ok'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
  })
  .strict()

export const WSPingRequestSchema = z
  .object({
    type: z.literal('ping'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
  })
  .strict()

export const WSPongResponseSchema = z
  .object({
    type: z.literal('pong'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    action: z.enum(['subscribe', 'unsubscribe']),
    status: z.enum(['subscribed', 'unsubscribed', 'partial', 'denied', 'no_topics']),
    topics: z.array(z.string()),
    denied_topics: z.array(z.string()),
    active_subscriptions: z.array(z.string()),
    message: z.string().nullable().optional(),
  })
  .strict()

export const WSSubscriptionsListResponseSchema = z
  .object({
    type: z.literal('subscriptions_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    available_topics: z.array(z.string()),
    user_role: UserRoleSchema,
    session_expires_at: z.iso.datetime().nullable().optional(),
    ws_token_exp: z.iso.datetime(),
  })
  .strict()

export const JsonObjectSchema = z.record(z.string(), z.any())

export const AiReviewRequestFrameDataSchema = z
  .object({
    type: z.literal('ai_review.request'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    review_public_id: z.string(),
    user_public_id: z.string(),
    strategy_public_id: z.string(),
    wallet_public_id: z.string(),
    instrument_public_id: z.string(),
    selected_delegate_public_id: z.string(),
    deadline: z.iso.datetime(),
    signal_envelope: z.record(z.string(), z.any()),
    instrument_metadata: z.record(z.string(), z.any()),
    dispatch_version: z.number().int(),
  })
  .strict()

export const AlertEventDataSchema = z
  .object({
    type: z.literal('alert_event'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    user_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    wallet_public_id: z.string().nullable().optional(),
    alert_type: z.enum([
      'order_fill_full',
      'order_rejected',
      'position_stop_loss_fired',
      'margin_warning',
      'critical_system_error',
    ]),
    priority: z.enum(['low', 'medium', 'high']),
    is_safety_critical: z.boolean(),
    title: z.string().min(1).max(160),
    body: z.string().min(1).max(512),
    payload: z.record(z.string(), z.any()).nullable().optional(),
    dedup_key: z.string().max(128).nullable().optional(),
    thread_key: z.string().max(64).nullable().optional(),
    source_topic: z.string().nullable().optional(),
  })
  .strict()

export const HeartbeatDataSchema = z
  .object({
    type: z.literal('heartbeat'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    component: z.string(),
    sequence: z.number().int(),
    status: z.enum(['healthy', 'warning', 'error']),
    lag_ms: z.number().int(),
    meta: z.record(z.string(), z.any()),
  })
  .strict()
