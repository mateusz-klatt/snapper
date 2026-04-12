/**
 * Generated Zod schemas for REST API validation.
 * DO NOT EDIT - regenerate with: make ui-gen-api-zod
 */

import { z } from 'zod'

export const ConnectionStatsSchema = z
  .object({
    active_connections: z.number().int(),
    zmq_subscribers: z.number().int(),
    subscriber_tasks: z.number().int(),
    active_topics: z.number().int(),
    active_clients: z.number().int(),
  })
  .strict()

export const ContinuousCandleDataSchema = z
  .object({
    type: z.literal('continuous_candle'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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

export const CredentialSummarySchema = z
  .object({
    type: z.literal('credential_summary'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    wallet_public_id: z.string(),
    exchange: z.string(),
    credential_type: z.string(),
    label: z.string().nullable().optional(),
  })
  .strict()

export const ExchangeListResponseSchema = z
  .object({
    type: z.literal('exchange_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(z.string()),
    count: z.number().int(),
  })
  .strict()

export const ExecutionDataSchema = z
  .object({
    type: z.literal('execution'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    trade_id: z.string().nullable().optional(),
    exchange_order_id: z.string().nullable().optional(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'zonda', 'walutomat']),
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

export const FrontMonthDataSchema = z
  .object({
    type: z.literal('front_month'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    instrument_public_id: z.string(),
    native_symbol: z.string(),
    exchange: z.string(),
    expiry_at: z.iso.datetime(),
    relationship_type: z.string(),
    contract_family: z.string().nullable(),
  })
  .strict()

export const GapStatsSchema = z
  .object({
    gaps_detected: z.number().int(),
    session_resets: z.number().int(),
    duplicates: z.number().int(),
    mid_stream_joins: z.number().int(),
    rejected_unstamped: z.number().int(),
  })
  .strict()

export const HealthTopicsSchema = z
  .object({
    active: z.number().int(),
  })
  .strict()

export const InstrumentListResponseSchema = z
  .object({
    type: z.literal('instrument_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(z.string()),
    count: z.number().int(),
  })
  .strict()

export const JsonPrimitiveSchema = z.unknown()

export const MessageResponseSchema = z
  .object({
    type: z.literal('message'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.string(),
  })
  .strict()

export const OperatorInfoSchema = z
  .object({
    type: z.literal('operator_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    label: z.string(),
    description: z.string().nullable().optional(),
  })
  .strict()

export const OrderDataSchema = z
  .object({
    type: z.literal('order'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    exchange_order_id: z.string().nullable().optional(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'zonda', 'walutomat']),
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
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'zonda', 'walutomat']),
    mode: z.enum(['live', 'paper']),
    quantity: z.number(),
    average_price: z.number(),
    unrealized_pnl: z.number(),
    realized_pnl: z.number(),
    position_cycle_public_id: z.string().nullable().optional(),
  })
  .strict()

export const ProcessCategoryCountSchema = z
  .object({
    running: z.number().int(),
    total: z.number().int(),
  })
  .strict()

export const ProcessCreatedInfoSchema = z
  .object({
    name: z.string(),
    template: z.string(),
  })
  .strict()

export const ProcessStartDataSchema = z
  .object({
    type: z.literal('process_start'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    status: z.enum(['success', 'already_running', 'error']),
    name: z.string(),
    process_public_id: z.string().nullable().optional(),
    message: z.string().nullable().optional(),
  })
  .strict()

export const ProcessStatusSchema = z
  .object({
    status: z.enum(['not_running', 'running', 'stopped', 'completed', 'error']),
    pid: z.number().int().nullable().optional(),
    started_at: z.string().nullable().optional(),
    command: z.string().nullable().optional(),
    exit_code: z.number().int().nullable().optional(),
    error: z.string().nullable().optional(),
  })
  .strict()

export const ProcessStopDataSchema = z
  .object({
    type: z.literal('process_stop'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    status: z.enum(['success', 'not_running', 'error']),
    name: z.string(),
    message: z.string().nullable().optional(),
  })
  .strict()

export const RelationshipTypeEnumSchema = z.enum(['exact', 'derivative', 'proxy'])

export const RollPointDetailSchema = z
  .object({
    from_contract: z.string(),
    to_contract: z.string(),
    roll_at: z.string(),
  })
  .strict()

export const ScopeGrantInfoSchema = z
  .object({
    type: z.literal('scope_grant_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    operator_public_id: z.string(),
    wallet_public_id: z.string(),
    granted_by_user_public_id: z.string(),
    scope_kind: z.string(),
    underlying_public_id: z.string().nullable().optional(),
    instrument_public_id: z.string().nullable().optional(),
    note: z.string().nullable().optional(),
    known_to: z.iso.datetime(),
  })
  .strict()

export const SettingCategoriesResponseSchema = z
  .object({
    type: z.literal('setting_categories'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(z.string()),
    count: z.number().int(),
  })
  .strict()

export const SettingReadSchema = z
  .object({
    type: z.literal('setting_read'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    key: z.string(),
    value: z.string(),
    category: z.string(),
    description: z.string().nullable().optional(),
    updated_at: z.iso.datetime(),
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
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'kraken_futures', 'zonda', 'walutomat']),
    side: z.enum(['buy', 'sell']),
    strength: z.number(),
    reason: z.string(),
    price: z.number().nullable().optional(),
    strategy_name: z.string().nullable().optional(),
    fired_at: z.iso.datetime(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    user_public_id: z.string().nullable().optional(),
  })
  .strict()

export const StrategyProcessSchema = z
  .object({
    type: z.literal('strategy_process'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    name: z.string(),
    running: z.boolean(),
    enabled: z.boolean(),
    mode: z.enum(['thread', 'process']),
  })
  .strict()

export const SubscriptionsStatsSchema = z
  .object({
    per_topic: z.record(z.string(), z.number().int()),
    per_client: z.record(z.string(), z.array(z.string())),
  })
  .strict()

export const TopicMetricSnapshotSchema = z
  .object({
    active_subscribers: z.number().int(),
    received: z.number().int(),
    forwarded: z.number().int(),
    throttled: z.number().int(),
    dropped: z.number().int(),
    timeout: z.number().int(),
    errors: z.number().int(),
    invalid_messages: z.number().int(),
    last_message_ts: z.number(),
    throttle_ms: z.number().int().nullable().optional(),
    pattern: z.string().nullable().optional(),
  })
  .strict()

export const UnderlyingAssetDataSchema = z
  .object({
    type: z.literal('underlying_asset'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
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
    instrument_public_id: z.string(),
    native_symbol: z.string(),
    exchange: z.string(),
    asset_type: z.string(),
    relationship_type: z.string(),
    contract_family: z.string().nullable(),
  })
  .strict()

export const UserRoleSchema = z.enum(['viewer', 'operator', 'admin'])

export const ValidationErrorSchema = z
  .object({
    loc: z.array(z.union([z.string(), z.number().int()])),
    msg: z.string(),
    type: z.string(),
    input: z.unknown().optional(),
    ctx: z.record(z.string(), z.unknown()).optional(),
  })
  .strict()

export const WalletInfoSchema = z
  .object({
    type: z.literal('wallet_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    label: z.string(),
    description: z.string().nullable().optional(),
    is_paper: z.boolean(),
  })
  .strict()

export const WebSocketStatsSchema = z
  .object({
    active_connections: z.number().int(),
    topic_subscribers: z.record(z.string(), z.number().int()),
    client_count: z.number().int(),
  })
  .strict()

export const WsStatsConfigSchema = z
  .object({
    broker_xpub: z.string(),
    heartbeat_interval_ms: z.number().int(),
  })
  .strict()

export const ZmqBridgeStatsSchema = z
  .object({
    active_topics: z.number().int(),
    subscriber_tasks: z.number().int(),
    available_topics: z.array(z.string()),
  })
  .strict()

export const ZmqComponentsSchema = z
  .object({
    zmq_context: z.enum(['ok', 'error']),
    websocket_manager: z.enum(['ok', 'error']),
    active_connections: z.number().int(),
  })
  .strict()

export const ZmqConfigSchema = z
  .object({
    available_topics: z.array(z.string()),
  })
  .strict()

export const LoginBodySchema = z
  .object({
    username: z.string(),
    password: z.string(),
    remember_me: z.boolean().optional(),
  })
  .strict()

export const DeactivateUserBodySchema = z.object({}).strict()

export const ChangePasswordBodySchema = z
  .object({
    current_password: z.string(),
    new_password: z.string().min(8),
  })
  .strict()

export const AdminResetPasswordBodySchema = z
  .object({
    new_password: z.string().min(8),
  })
  .strict()

export const SettingUpdateBodySchema = z
  .object({
    value: z.string(),
    category: z.string().optional(),
    description: z.string().nullable().optional(),
  })
  .strict()

export const RemoveSettingBodySchema = z.object({}).strict()

export const CreateCredentialBodySchema = z
  .object({
    exchange: z.string().min(1).max(20),
    credential_type: z.enum(['api_key_secret', 'rsa_pem', 'oauth', 'paper']),
    credential_payload: z.record(z.string(), z.string()),
    label: z.string().max(128).nullable().optional(),
  })
  .strict()

export const RotateCredentialBodySchema = z
  .object({
    credential_payload: z.record(z.string(), z.string()),
    label: z.string().max(128).nullable().optional(),
  })
  .strict()

export const BracketCreateBodySchema = z
  .object({
    position_cycle_public_id: z.string(),
    sl_price: z.number().nullable().optional(),
    tp_price: z.number().nullable().optional(),
    idempotency_key: z.string().nullable().optional(),
  })
  .strict()

export const BracketCancelBodySchema = z
  .object({
    reason: z.string().nullable().optional(),
  })
  .strict()

export const CreateOrderBodySchema = z
  .object({
    instrument: z.string(),
    instrument_public_id: z.string(),
    exchange: z.string(),
    mode: z.enum(['live', 'paper']).optional(),
    side: z.enum(['buy', 'sell']),
    order_type: z.enum(['market', 'limit', 'stop', 'stop_limit']),
    quantity: z.number(),
    price: z.number().nullable().optional(),
    stop_price: z.number().nullable().optional(),
    time_in_force: z.string().optional(),
    post_only: z.boolean().optional(),
    leverage: z.number().int().nullable().optional(),
    reduce_only: z.boolean().optional(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    idempotency_key: z.string().nullable().optional(),
  })
  .strict()

export const CancelOrderBodySchema = z
  .object({
    reason: z.string().nullable().optional(),
  })
  .strict()

export const CreateScopeGrantBodySchema = z
  .object({
    operator_public_id: z.string().min(1).max(64),
    wallet_public_id: z.string().min(1).max(64),
    scope_kind: z.enum(['underlying', 'instrument']),
    underlying_public_id: z.string().max(64).nullable().optional(),
    instrument_public_id: z.string().max(64).nullable().optional(),
    note: z.string().max(512).nullable().optional(),
  })
  .strict()

export const HandoverScopeGrantBodySchema = z
  .object({
    from_grant_public_id: z.string().min(1).max(64),
    to_operator_public_id: z.string().min(1).max(64),
    reason: z.string().max(512).nullable().optional(),
  })
  .strict()

export const CreateWalletBodySchema = z
  .object({
    label: z.string().min(1).max(128),
    description: z.string().max(512).nullable().optional(),
    is_paper: z.boolean().optional(),
  })
  .strict()

export const ContinuousCandleListResponseSchema = z
  .object({
    type: z.literal('continuous_candle_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(ContinuousCandleDataSchema),
    count: z.number().int(),
  })
  .strict()

export const ContractListResponseSchema = z
  .object({
    type: z.literal('contract_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(ContractDataSchema),
    count: z.number().int(),
  })
  .strict()

export const CredentialListResponseSchema = z
  .object({
    type: z.literal('credential_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(CredentialSummarySchema),
    count: z.number().int(),
  })
  .strict()

export const CredentialResponseSchema = z
  .object({
    type: z.literal('credential_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: CredentialSummarySchema,
  })
  .strict()

export const ExecutionListResponseSchema = z
  .object({
    type: z.literal('execution_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(ExecutionDataSchema),
    count: z.number().int(),
  })
  .strict()

export const ExecutionPlanResponseSchema = z
  .object({
    type: z.literal('execution_plan_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ExecutionPlanDataSchema,
  })
  .strict()

export const FrontMonthResponseSchema = z
  .object({
    type: z.literal('front_month'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: FrontMonthDataSchema,
  })
  .strict()

export const GapDetectionStatsSchema = z
  .object({
    bridge: GapStatsSchema,
    rest_clients: z.record(z.string(), GapStatsSchema),
  })
  .strict()

export const JsonValueSchema = z.unknown()

export const OperatorListResponseSchema = z
  .object({
    type: z.literal('operator_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(OperatorInfoSchema),
    count: z.number().int(),
  })
  .strict()

export const OrderListResponseSchema = z
  .object({
    type: z.literal('order_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(OrderDataSchema),
    count: z.number().int(),
  })
  .strict()

export const PositionListResponseSchema = z
  .object({
    type: z.literal('position_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(PositionDataSchema),
    count: z.number().int(),
  })
  .strict()

export const ProcessSummaryDataSchema = z
  .object({
    type: z.literal('process_summary'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    feeds: ProcessCategoryCountSchema,
    strategies: ProcessCategoryCountSchema,
    executors: ProcessCategoryCountSchema,
    brokers: ProcessCategoryCountSchema,
  })
  .strict()

export const ProcessCreateDataSchema = z
  .object({
    type: z.literal('process_create'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    status: z.literal('created'),
    process: ProcessCreatedInfoSchema,
  })
  .strict()

export const ProcessStartResponseSchema = z
  .object({
    type: z.literal('process_start_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ProcessStartDataSchema,
  })
  .strict()

export const ProcessStopResponseSchema = z
  .object({
    type: z.literal('process_stop_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ProcessStopDataSchema,
  })
  .strict()

export const ContinuousSeriesPartialResponseSchema = z
  .object({
    type: z.literal('continuous_partial'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(ContinuousCandleDataSchema),
    count: z.number().int(),
    failed_roll: RollPointDetailSchema,
    message: z.string(),
  })
  .strict()

export const HandoverScopeGrantResultSchema = z
  .object({
    closed_grant: ScopeGrantInfoSchema,
    new_grant: ScopeGrantInfoSchema,
  })
  .strict()

export const ScopeGrantListResponseSchema = z
  .object({
    type: z.literal('scope_grant_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(ScopeGrantInfoSchema),
    count: z.number().int(),
  })
  .strict()

export const ScopeGrantResponseSchema = z
  .object({
    type: z.literal('scope_grant_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ScopeGrantInfoSchema,
  })
  .strict()

export const SettingListResponseSchema = z
  .object({
    type: z.literal('setting_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(SettingReadSchema),
    count: z.number().int(),
  })
  .strict()

export const SettingResponseSchema = z
  .object({
    type: z.literal('setting_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: SettingReadSchema,
  })
  .strict()

export const SignalListResponseSchema = z
  .object({
    type: z.literal('signal_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(SignalDataSchema),
    count: z.number().int(),
  })
  .strict()

export const StrategyListResponseSchema = z
  .object({
    type: z.literal('strategy_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(StrategyProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const UnderlyingAssetListResponseSchema = z
  .object({
    type: z.literal('underlying_asset_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(UnderlyingAssetDataSchema),
    count: z.number().int(),
  })
  .strict()

export const UnderlyingInstrumentListResponseSchema = z
  .object({
    type: z.literal('underlying_instrument_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(UnderlyingInstrumentDataSchema),
    count: z.number().int(),
  })
  .strict()

export const UserProfileSchema = z
  .object({
    type: z.literal('user_profile'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    username: z.string(),
    email: z.string().nullable().optional(),
    role: UserRoleSchema,
    is_active: z.boolean(),
    created_at: z.iso.datetime(),
    operator_public_ids: z.array(z.string()).optional(),
    primary_operator_public_id: z.string().nullable().optional(),
  })
  .strict()

export const CreateUserBodySchema = z
  .object({
    username: z.string().min(3).max(64),
    email: z.string().max(255).nullable().optional(),
    password: z.string().min(8),
    role: UserRoleSchema,
    is_active: z.boolean().optional(),
  })
  .strict()

export const UpdateUserBodySchema = z
  .object({
    email: z.string().max(255).nullable().optional(),
    role: UserRoleSchema.nullable().optional(),
    is_active: z.boolean().nullable().optional(),
  })
  .strict()

export const HTTPValidationErrorSchema = z
  .object({
    detail: z.array(ValidationErrorSchema).optional(),
  })
  .strict()

export const WalletListResponseSchema = z
  .object({
    type: z.literal('wallet_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(WalletInfoSchema),
    count: z.number().int(),
  })
  .strict()

export const WalletResponseSchema = z
  .object({
    type: z.literal('wallet_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: WalletInfoSchema,
  })
  .strict()

export const WsStatsDataSchema = z
  .object({
    type: z.literal('ws_stats'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    websocket: WebSocketStatsSchema,
    zmq_bridge: ZmqBridgeStatsSchema,
    connections: ConnectionStatsSchema,
    topics: z.record(z.string(), TopicMetricSnapshotSchema),
    subscriptions: SubscriptionsStatsSchema,
    config: WsStatsConfigSchema,
  })
  .strict()

export const ZmqHealthDataSchema = z
  .object({
    type: z.literal('zmq_health'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    status: z.enum(['healthy', 'warning', 'error']),
    components: ZmqComponentsSchema,
    config: ZmqConfigSchema,
    connections: ConnectionStatsSchema,
    message_stats: z.record(z.string(), TopicMetricSnapshotSchema),
    errors: z.array(z.string()),
  })
  .strict()

export const LoginRequestSchema = z
  .object({
    type: z.literal('login_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: LoginBodySchema,
  })
  .strict()

export const DeactivateUserRequestSchema = z
  .object({
    type: z.literal('deactivate_user_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: DeactivateUserBodySchema,
  })
  .strict()

export const ChangePasswordRequestSchema = z
  .object({
    type: z.literal('change_password_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ChangePasswordBodySchema,
  })
  .strict()

export const AdminResetPasswordRequestSchema = z
  .object({
    type: z.literal('admin_reset_password_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: AdminResetPasswordBodySchema,
  })
  .strict()

export const SettingUpdateSchema = z
  .object({
    type: z.literal('setting_update').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: SettingUpdateBodySchema,
  })
  .strict()

export const RemoveSettingRequestSchema = z
  .object({
    type: z.literal('remove_setting_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: RemoveSettingBodySchema,
  })
  .strict()

export const CreateCredentialCommandSchema = z
  .object({
    type: z.literal('create_credential_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: CreateCredentialBodySchema,
  })
  .strict()

export const RotateCredentialCommandSchema = z
  .object({
    type: z.literal('rotate_credential_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: RotateCredentialBodySchema,
  })
  .strict()

export const BracketCreateCommandSchema = z
  .object({
    type: z.literal('create_bracket_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: BracketCreateBodySchema,
  })
  .strict()

export const BracketCancelCommandSchema = z
  .object({
    type: z.literal('cancel_bracket_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: BracketCancelBodySchema,
  })
  .strict()

export const CreateOrderCommandSchema = z
  .object({
    type: z.literal('create_order_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: CreateOrderBodySchema,
  })
  .strict()

export const CancelOrderCommandSchema = z
  .object({
    type: z.literal('cancel_order_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: CancelOrderBodySchema,
  })
  .strict()

export const CreateScopeGrantCommandSchema = z
  .object({
    type: z.literal('create_scope_grant_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: CreateScopeGrantBodySchema,
  })
  .strict()

export const HandoverScopeGrantCommandSchema = z
  .object({
    type: z.literal('handover_scope_grant_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: HandoverScopeGrantBodySchema,
  })
  .strict()

export const CreateWalletCommandSchema = z
  .object({
    type: z.literal('create_wallet_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: CreateWalletBodySchema,
  })
  .strict()

export const HealthCheckDataSchema = z
  .object({
    type: z.literal('health_check'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    status: z.enum(['healthy', 'warning', 'error']),
    version: z.string(),
    connections: ConnectionStatsSchema,
    topics: HealthTopicsSchema,
    gap_detection: GapDetectionStatsSchema,
  })
  .strict()

export const JsonObjectSchema = z.record(z.string(), z.any())

export const ProcessSummaryResponseSchema = z
  .object({
    type: z.literal('process_summary_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ProcessSummaryDataSchema,
  })
  .strict()

export const ProcessCreateResponseSchema = z
  .object({
    type: z.literal('process_create_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ProcessCreateDataSchema,
  })
  .strict()

export const HandoverScopeGrantResponseSchema = z
  .object({
    type: z.literal('handover_scope_grant_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: HandoverScopeGrantResultSchema,
  })
  .strict()

export const LoginDataSchema = z
  .object({
    type: z.literal('login'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    message: z.string(),
    expires_in: z.number().int(),
    user: UserProfileSchema,
  })
  .strict()

export const RefreshDataSchema = z
  .object({
    type: z.literal('refresh'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    message: z.string(),
    ws_token: z.string(),
    ws_token_exp: z.iso.datetime(),
    csrf_token: z.string(),
    user: UserProfileSchema,
  })
  .strict()

export const UserListResponseSchema = z
  .object({
    type: z.literal('user_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(UserProfileSchema),
    count: z.number().int(),
  })
  .strict()

export const UserResponseSchema = z
  .object({
    type: z.literal('user_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: UserProfileSchema,
  })
  .strict()

export const CreateUserRequestSchema = z
  .object({
    type: z.literal('create_user_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: CreateUserBodySchema,
  })
  .strict()

export const UpdateUserRequestSchema = z
  .object({
    type: z.literal('update_user_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: UpdateUserBodySchema,
  })
  .strict()

export const WsStatsResponseSchema = z
  .object({
    type: z.literal('ws_stats_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: WsStatsDataSchema,
  })
  .strict()

export const ZmqHealthResponseSchema = z
  .object({
    type: z.literal('zmq_health_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ZmqHealthDataSchema,
  })
  .strict()

export const HealthCheckResponseSchema = z
  .object({
    type: z.literal('health_check_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: HealthCheckDataSchema,
  })
  .strict()

export const AvailableProcessSchema = z
  .object({
    type: z.literal('available_process'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    name: z.string(),
    class_path: z.string(),
    method: z.string(),
    description: z.string(),
    lifecycle: z.enum(['long_running', 'one_shot']),
    role: z.enum(['core', 'task', 'strategy', 'backtest']),
    tags: z.array(z.string()),
    parameters_schema: z.record(z.string(), z.any()).nullable().optional(),
  })
  .strict()

export const ConfiguredProcessSchema = z
  .object({
    type: z.literal('configured_process'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    name: z.string(),
    enabled: z.boolean(),
    running: z.boolean(),
    mode: z.enum(['thread', 'process']),
    class_path: z.string(),
    method: z.string(),
    parameters: z.record(z.string(), z.any()),
    note: z.string().nullable().optional(),
    lifecycle: z.enum(['long_running', 'one_shot']),
    role: z.enum(['core', 'task', 'strategy', 'backtest']),
    tags: z.array(z.string()),
    parameters_schema: z.record(z.string(), z.any()).nullable().optional(),
    is_one_shot: z.boolean(),
    active_public_id: z.string().nullable().optional(),
  })
  .strict()

export const ProcessRunSchema = z
  .object({
    type: z.literal('process_run'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    process_name: z.string(),
    status: z.enum(['running', 'succeeded', 'failed', 'cancelled']),
    role: z.enum(['core', 'task', 'strategy', 'backtest']),
    lifecycle: z.enum(['long_running', 'one_shot']),
    parameters: z.record(z.string(), z.any()).nullable().optional(),
    result: z.record(z.string(), z.any()).nullable().optional(),
    error: z.string().nullable().optional(),
    tags: z.array(z.string()),
    started_at: z.string(),
    completed_at: z.string().nullable().optional(),
  })
  .strict()

export const ProcessSchemaDataSchema = z
  .object({
    type: z.literal('process_schema'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    name: z.string(),
    description: z.string(),
    class_path: z.string(),
    method: z.string(),
    default_enabled: z.boolean(),
    default_mode: z.enum(['thread', 'process']),
    default_parameters: z.record(z.string(), z.any()),
    lifecycle: z.enum(['long_running', 'one_shot']),
  })
  .strict()

export const StrategyStatusPayloadSchema = z
  .object({
    strategy_name: z.string(),
    status: z.string(),
    details: z.record(z.string(), z.any()),
    signals_generated: z.number().int().nullable().optional(),
    trades_executed: z.number().int().nullable().optional(),
    last_signal: z.string().nullable().optional(),
    last_signal_time: z.string().nullable().optional(),
    pnl: z.number().nullable().optional(),
    pid: z.number().int().nullable().optional(),
    uptime: z.string().nullable().optional(),
  })
  .strict()

export const ProcessCreateBodySchema = z
  .object({
    name: z.string().min(3).max(64),
    template: z.string(),
    enabled: z.boolean().nullable().optional(),
    mode: z.enum(['thread', 'process']).nullable().optional(),
    parameters: z.record(z.string(), z.any()).nullable().optional(),
    note: z.string().max(512).nullable().optional(),
  })
  .strict()

export const ProcessStartBodySchema = z
  .object({
    mode: z.enum(['thread', 'process']).nullable().optional(),
    parameters: z.record(z.string(), z.any()).nullable().optional(),
  })
  .strict()

export const LoginResponseSchema = z
  .object({
    type: z.literal('login_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: LoginDataSchema,
  })
  .strict()

export const RefreshResponseSchema = z
  .object({
    type: z.literal('refresh_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: RefreshDataSchema,
  })
  .strict()

export const AvailableProcessesResponseSchema = z
  .object({
    type: z.literal('available_processes'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(AvailableProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const ConfiguredProcessesResponseSchema = z
  .object({
    type: z.literal('configured_processes'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(ConfiguredProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const ProcessRunsResponseSchema = z
  .object({
    type: z.literal('process_runs'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: z.array(ProcessRunSchema),
    count: z.number().int(),
  })
  .strict()

export const ProcessSchemaResponseSchema = z
  .object({
    type: z.literal('process_schema_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ProcessSchemaDataSchema,
  })
  .strict()

export const SystemStatusDataSchema = z
  .object({
    type: z.literal('system_status'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    trader: ProcessStatusSchema,
    backtests: z.record(z.string(), ProcessStatusSchema),
    strategies: z.array(StrategyStatusPayloadSchema),
  })
  .strict()

export const ProcessCreateRequestSchema = z
  .object({
    type: z.literal('process_create_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ProcessCreateBodySchema,
  })
  .strict()

export const ProcessStartRequestSchema = z
  .object({
    type: z.literal('process_start_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: ProcessStartBodySchema,
  })
  .strict()

export const SystemStatusResponseSchema = z
  .object({
    type: z.literal('system_status_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    payload: SystemStatusDataSchema,
  })
  .strict()

// Type exports
export type ConnectionStats = z.infer<typeof ConnectionStatsSchema>
export type ContinuousCandleData = z.infer<typeof ContinuousCandleDataSchema>
export type ContractData = z.infer<typeof ContractDataSchema>
export type CredentialSummary = z.infer<typeof CredentialSummarySchema>
export type ExchangeListResponse = z.infer<typeof ExchangeListResponseSchema>
export type ExecutionData = z.infer<typeof ExecutionDataSchema>
export type ExecutionPlanData = z.infer<typeof ExecutionPlanDataSchema>
export type FrontMonthData = z.infer<typeof FrontMonthDataSchema>
export type GapStats = z.infer<typeof GapStatsSchema>
export type HealthTopics = z.infer<typeof HealthTopicsSchema>
export type InstrumentListResponse = z.infer<typeof InstrumentListResponseSchema>
export type JsonPrimitive = z.infer<typeof JsonPrimitiveSchema>
export type MessageResponse = z.infer<typeof MessageResponseSchema>
export type OperatorInfo = z.infer<typeof OperatorInfoSchema>
export type OrderData = z.infer<typeof OrderDataSchema>
export type PositionData = z.infer<typeof PositionDataSchema>
export type ProcessCategoryCount = z.infer<typeof ProcessCategoryCountSchema>
export type ProcessCreatedInfo = z.infer<typeof ProcessCreatedInfoSchema>
export type ProcessStartData = z.infer<typeof ProcessStartDataSchema>
export type ProcessStatus = z.infer<typeof ProcessStatusSchema>
export type ProcessStopData = z.infer<typeof ProcessStopDataSchema>
export type RelationshipTypeEnum = z.infer<typeof RelationshipTypeEnumSchema>
export type RollPointDetail = z.infer<typeof RollPointDetailSchema>
export type ScopeGrantInfo = z.infer<typeof ScopeGrantInfoSchema>
export type SettingCategoriesResponse = z.infer<typeof SettingCategoriesResponseSchema>
export type SettingRead = z.infer<typeof SettingReadSchema>
export type SignalData = z.infer<typeof SignalDataSchema>
export type StrategyProcess = z.infer<typeof StrategyProcessSchema>
export type SubscriptionsStats = z.infer<typeof SubscriptionsStatsSchema>
export type TopicMetricSnapshot = z.infer<typeof TopicMetricSnapshotSchema>
export type UnderlyingAssetData = z.infer<typeof UnderlyingAssetDataSchema>
export type UnderlyingInstrumentData = z.infer<typeof UnderlyingInstrumentDataSchema>
export type UserRole = z.infer<typeof UserRoleSchema>
export type ValidationError = z.infer<typeof ValidationErrorSchema>
export type WalletInfo = z.infer<typeof WalletInfoSchema>
export type WebSocketStats = z.infer<typeof WebSocketStatsSchema>
export type WsStatsConfig = z.infer<typeof WsStatsConfigSchema>
export type ZmqBridgeStats = z.infer<typeof ZmqBridgeStatsSchema>
export type ZmqComponents = z.infer<typeof ZmqComponentsSchema>
export type ZmqConfig = z.infer<typeof ZmqConfigSchema>
export type LoginBody = z.infer<typeof LoginBodySchema>
export type DeactivateUserBody = z.infer<typeof DeactivateUserBodySchema>
export type ChangePasswordBody = z.infer<typeof ChangePasswordBodySchema>
export type AdminResetPasswordBody = z.infer<typeof AdminResetPasswordBodySchema>
export type SettingUpdateBody = z.infer<typeof SettingUpdateBodySchema>
export type RemoveSettingBody = z.infer<typeof RemoveSettingBodySchema>
export type CreateCredentialBody = z.infer<typeof CreateCredentialBodySchema>
export type RotateCredentialBody = z.infer<typeof RotateCredentialBodySchema>
export type BracketCreateBody = z.infer<typeof BracketCreateBodySchema>
export type BracketCancelBody = z.infer<typeof BracketCancelBodySchema>
export type CreateOrderBody = z.infer<typeof CreateOrderBodySchema>
export type CancelOrderBody = z.infer<typeof CancelOrderBodySchema>
export type CreateScopeGrantBody = z.infer<typeof CreateScopeGrantBodySchema>
export type HandoverScopeGrantBody = z.infer<typeof HandoverScopeGrantBodySchema>
export type CreateWalletBody = z.infer<typeof CreateWalletBodySchema>
export type ContinuousCandleListResponse = z.infer<typeof ContinuousCandleListResponseSchema>
export type ContractListResponse = z.infer<typeof ContractListResponseSchema>
export type CredentialListResponse = z.infer<typeof CredentialListResponseSchema>
export type CredentialResponse = z.infer<typeof CredentialResponseSchema>
export type ExecutionListResponse = z.infer<typeof ExecutionListResponseSchema>
export type ExecutionPlanResponse = z.infer<typeof ExecutionPlanResponseSchema>
export type FrontMonthResponse = z.infer<typeof FrontMonthResponseSchema>
export type GapDetectionStats = z.infer<typeof GapDetectionStatsSchema>
export type JsonValue = z.infer<typeof JsonValueSchema>
export type OperatorListResponse = z.infer<typeof OperatorListResponseSchema>
export type OrderListResponse = z.infer<typeof OrderListResponseSchema>
export type PositionListResponse = z.infer<typeof PositionListResponseSchema>
export type ProcessSummaryData = z.infer<typeof ProcessSummaryDataSchema>
export type ProcessCreateData = z.infer<typeof ProcessCreateDataSchema>
export type ProcessStartResponse = z.infer<typeof ProcessStartResponseSchema>
export type ProcessStopResponse = z.infer<typeof ProcessStopResponseSchema>
export type ContinuousSeriesPartialResponse = z.infer<typeof ContinuousSeriesPartialResponseSchema>
export type HandoverScopeGrantResult = z.infer<typeof HandoverScopeGrantResultSchema>
export type ScopeGrantListResponse = z.infer<typeof ScopeGrantListResponseSchema>
export type ScopeGrantResponse = z.infer<typeof ScopeGrantResponseSchema>
export type SettingListResponse = z.infer<typeof SettingListResponseSchema>
export type SettingResponse = z.infer<typeof SettingResponseSchema>
export type SignalListResponse = z.infer<typeof SignalListResponseSchema>
export type StrategyListResponse = z.infer<typeof StrategyListResponseSchema>
export type UnderlyingAssetListResponse = z.infer<typeof UnderlyingAssetListResponseSchema>
export type UnderlyingInstrumentListResponse = z.infer<
  typeof UnderlyingInstrumentListResponseSchema
>
export type UserProfile = z.infer<typeof UserProfileSchema>
export type CreateUserBody = z.infer<typeof CreateUserBodySchema>
export type UpdateUserBody = z.infer<typeof UpdateUserBodySchema>
export type HTTPValidationError = z.infer<typeof HTTPValidationErrorSchema>
export type WalletListResponse = z.infer<typeof WalletListResponseSchema>
export type WalletResponse = z.infer<typeof WalletResponseSchema>
export type WsStatsData = z.infer<typeof WsStatsDataSchema>
export type ZmqHealthData = z.infer<typeof ZmqHealthDataSchema>
export type LoginRequest = z.infer<typeof LoginRequestSchema>
export type DeactivateUserRequest = z.infer<typeof DeactivateUserRequestSchema>
export type ChangePasswordRequest = z.infer<typeof ChangePasswordRequestSchema>
export type AdminResetPasswordRequest = z.infer<typeof AdminResetPasswordRequestSchema>
export type SettingUpdate = z.infer<typeof SettingUpdateSchema>
export type RemoveSettingRequest = z.infer<typeof RemoveSettingRequestSchema>
export type CreateCredentialCommand = z.infer<typeof CreateCredentialCommandSchema>
export type RotateCredentialCommand = z.infer<typeof RotateCredentialCommandSchema>
export type BracketCreateCommand = z.infer<typeof BracketCreateCommandSchema>
export type BracketCancelCommand = z.infer<typeof BracketCancelCommandSchema>
export type CreateOrderCommand = z.infer<typeof CreateOrderCommandSchema>
export type CancelOrderCommand = z.infer<typeof CancelOrderCommandSchema>
export type CreateScopeGrantCommand = z.infer<typeof CreateScopeGrantCommandSchema>
export type HandoverScopeGrantCommand = z.infer<typeof HandoverScopeGrantCommandSchema>
export type CreateWalletCommand = z.infer<typeof CreateWalletCommandSchema>
export type HealthCheckData = z.infer<typeof HealthCheckDataSchema>
export type JsonObject = z.infer<typeof JsonObjectSchema>
export type ProcessSummaryResponse = z.infer<typeof ProcessSummaryResponseSchema>
export type ProcessCreateResponse = z.infer<typeof ProcessCreateResponseSchema>
export type HandoverScopeGrantResponse = z.infer<typeof HandoverScopeGrantResponseSchema>
export type LoginData = z.infer<typeof LoginDataSchema>
export type RefreshData = z.infer<typeof RefreshDataSchema>
export type UserListResponse = z.infer<typeof UserListResponseSchema>
export type UserResponse = z.infer<typeof UserResponseSchema>
export type CreateUserRequest = z.infer<typeof CreateUserRequestSchema>
export type UpdateUserRequest = z.infer<typeof UpdateUserRequestSchema>
export type WsStatsResponse = z.infer<typeof WsStatsResponseSchema>
export type ZmqHealthResponse = z.infer<typeof ZmqHealthResponseSchema>
export type HealthCheckResponse = z.infer<typeof HealthCheckResponseSchema>
export type AvailableProcess = z.infer<typeof AvailableProcessSchema>
export type ConfiguredProcess = z.infer<typeof ConfiguredProcessSchema>
export type ProcessRun = z.infer<typeof ProcessRunSchema>
export type ProcessSchemaData = z.infer<typeof ProcessSchemaDataSchema>
export type StrategyStatusPayload = z.infer<typeof StrategyStatusPayloadSchema>
export type ProcessCreateBody = z.infer<typeof ProcessCreateBodySchema>
export type ProcessStartBody = z.infer<typeof ProcessStartBodySchema>
export type LoginResponse = z.infer<typeof LoginResponseSchema>
export type RefreshResponse = z.infer<typeof RefreshResponseSchema>
export type AvailableProcessesResponse = z.infer<typeof AvailableProcessesResponseSchema>
export type ConfiguredProcessesResponse = z.infer<typeof ConfiguredProcessesResponseSchema>
export type ProcessRunsResponse = z.infer<typeof ProcessRunsResponseSchema>
export type ProcessSchemaResponse = z.infer<typeof ProcessSchemaResponseSchema>
export type SystemStatusData = z.infer<typeof SystemStatusDataSchema>
export type ProcessCreateRequest = z.infer<typeof ProcessCreateRequestSchema>
export type ProcessStartRequest = z.infer<typeof ProcessStartRequestSchema>
export type SystemStatusResponse = z.infer<typeof SystemStatusResponseSchema>
