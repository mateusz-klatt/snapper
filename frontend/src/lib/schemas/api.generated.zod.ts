/**
 * Generated Zod schemas for REST API validation.
 * DO NOT EDIT - regenerate with: make ui-gen-api-zod
 */

import { z } from 'zod'

export const AsyncioMetricsSchema = z
  .object({
    active_tasks: z.number().int(),
    pending_tasks: z.number().int(),
  })
  .strict()

export const BacktestComparisonDataSchema = z
  .object({
    type: z.literal('backtest_comparison'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    wallet_public_id: z.string(),
    run_a_public_id: z.string(),
    run_b_public_id: z.string(),
    config_hash: z.string().nullable().optional(),
    pairing_mode: z.string(),
    anchor_run_public_id: z.string().nullable().optional(),
  })
  .strict()

export const BacktestEquityPointInlineSchema = z
  .object({
    point_time: z.iso.datetime(),
    equity: z.number(),
    cash: z.number(),
    position_value: z.number(),
    drawdown: z.number(),
  })
  .strict()

export const BacktestEventDataSchema = z
  .object({
    type: z.literal('backtest_event'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    run_public_id: z.string(),
    event_type: z.string(),
    detail: z.record(z.string(), z.unknown()).optional(),
  })
  .strict()

export const BacktestSignalDataSchema = z
  .object({
    type: z.literal('backtest_signal'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    run_public_id: z.string(),
    signal_time: z.iso.datetime(),
    signal_type: z.string(),
    instrument: z.string(),
    price: z.number(),
    indicators: z.record(z.string(), z.unknown()).optional(),
  })
  .strict()

export const BacktestTradeDataSchema = z
  .object({
    type: z.literal('backtest_trade'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    run_public_id: z.string(),
    executed_at: z.iso.datetime(),
    instrument: z.string(),
    side: z.string(),
    quantity: z.number(),
    price: z.number(),
    fee: z.number(),
    pnl: z.number().nullable().optional(),
    position_after: z.number(),
    signal_public_id: z.string().nullable().optional(),
  })
  .strict()

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

export const CpuMetricsSchema = z
  .object({
    process_percent: z.number(),
    user_time_seconds: z.number(),
    system_time_seconds: z.number(),
    cgroup_quota_microseconds: z.number().int().nullable(),
    cgroup_throttled_count: z.number().int().nullable(),
  })
  .strict()

export const CredentialSummarySchema = z
  .object({
    type: z.literal('credential_summary'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    wallet_public_id: z.string(),
    exchange: z.string(),
    credential_type: z.string(),
    label: z.string().nullable().optional(),
  })
  .strict()

export const DbInternalMetricsSchema = z
  .object({
    aiosqlite_live_connections: z.number().int(),
    pool_size: z.number().int().nullable(),
    pool_checked_out: z.number().int().nullable(),
  })
  .strict()

export const DeviceAlertPrefBodySchema = z
  .object({
    alert_type: z.enum([
      'order_fill_full',
      'order_rejected',
      'position_stop_loss_fired',
      'margin_warning',
      'critical_system_error',
    ]),
    operator_public_id: z.string().nullable().optional(),
    wallet_public_id: z.string().nullable().optional(),
    enabled: z.boolean(),
    min_priority: z.enum(['low', 'medium', 'high']),
    quiet_hours_start_min: z.number().int().nullable().optional(),
    quiet_hours_end_min: z.number().int().nullable().optional(),
    mute_until: z.iso.datetime().nullable().optional(),
    timezone: z.string().max(64),
  })
  .strict()

export const DeviceAlertPrefInfoSchema = z
  .object({
    type: z.literal('device_alert_pref_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    device_public_id: z.string(),
    alert_type: z.string(),
    operator_public_id: z.string().nullable().optional(),
    wallet_public_id: z.string().nullable().optional(),
    enabled: z.boolean(),
    min_priority: z.string(),
    quiet_hours_start_min: z.number().int().nullable().optional(),
    quiet_hours_end_min: z.number().int().nullable().optional(),
    mute_until: z.iso.datetime().nullable().optional(),
    timezone: z.string(),
  })
  .strict()

export const EquityOverlayPointSchema = z
  .object({
    point_time: z.iso.datetime(),
    equity_a: z.number().nullable().optional(),
    equity_b: z.number().nullable().optional(),
  })
  .strict()

export const ExchangeListResponseSchema = z
  .object({
    type: z.literal('exchange_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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

export const FeatureFlagsPayloadSchema = z
  .object({
    ai_integration_enabled: z.boolean(),
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

export const GapStatsSchema = z
  .object({
    gaps_detected: z.number().int(),
    session_resets: z.number().int(),
    duplicates: z.number().int(),
    mid_stream_joins: z.number().int(),
    rejected_unstamped: z.number().int(),
  })
  .strict()

export const GcMetricsSchema = z
  .object({
    collections_gen0: z.number().int(),
    collections_gen1: z.number().int(),
    collections_gen2: z.number().int(),
    uncollectable: z.number().int(),
    current_objects: z.number().int(),
  })
  .strict()

export const HealthTopicsSchema = z
  .object({
    active: z.number().int(),
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

export const InstrumentListResponseSchema = z
  .object({
    type: z.literal('instrument_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(z.string()),
    count: z.number().int(),
  })
  .strict()

export const JsonPrimitiveSchema = z.unknown()

export const LimitsMetricsSchema = z
  .object({
    rlimit_nproc: z.number().int(),
    rlimit_nofile: z.number().int(),
    rlimit_as_bytes: z.number().int(),
  })
  .strict()

export const MemoryMetricsSchema = z
  .object({
    rss_bytes: z.number().int(),
    rss_peak_bytes: z.number().int(),
    vms_bytes: z.number().int(),
    python_traced_bytes: z.number().int().nullable(),
    native_bytes: z.number().int().nullable(),
    cgroup_limit_bytes: z.number().int().nullable(),
    cgroup_current_bytes: z.number().int().nullable(),
    saturation_pct: z.number().nullable(),
  })
  .strict()

export const MessageResponseSchema = z
  .object({
    type: z.literal('message'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.string(),
  })
  .strict()

export const MetricDiffRowSchema = z
  .object({
    name: z.string(),
    run_a: z.number().nullable().optional(),
    run_b: z.number().nullable().optional(),
    delta: z.number().nullable().optional(),
    pct: z.number().nullable().optional(),
  })
  .strict()

export const NotificationDeviceInfoSchema = z
  .object({
    type: z.literal('notification_device_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    user_public_id: z.string(),
    device_token: z.string(),
    device_id: z.string(),
    platform: z.string(),
    env: z.string(),
    app_version: z.string().nullable().optional(),
    previews_mode: z.string(),
    registered_at: z.iso.datetime(),
    last_seen_at: z.iso.datetime().nullable().optional(),
  })
  .strict()

export const NotificationMetricsDataSchema = z
  .object({
    type: z.literal('notification_metrics'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    delivery_success_total: z.number().int(),
    delivery_failed_total: z.number().int(),
    delivery_410_unregistered_total: z.number().int(),
    delivery_cancelled_scope_total: z.number().int(),
    outbox_queued_depth: z.number().int(),
  })
  .strict()

export const OperatorInfoSchema = z
  .object({
    type: z.literal('operator_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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

export const OrphanSweepResultDataSchema = z
  .object({
    type: z.literal('orphan_sweep_result'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    closed_count: z.number().int(),
    closed_cycle_ids: z.array(z.string()),
  })
  .strict()

export const PendingReviewSummaryItemSchema = z
  .object({
    review_public_id: z.string(),
    selected_delegate_public_id: z.string(),
    wallet_public_id: z.string(),
    dispatch_version: z.number().int(),
    status: z.string(),
    deadline: z.iso.datetime(),
    fanout_after: z.iso.datetime(),
  })
  .strict()

export const PositionCycleDataSchema = z
  .object({
    type: z.literal('position_cycle'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    cycle_public_id: z.string(),
    shard_key: z.string(),
    instrument_public_id: z.string(),
    exchange: z.string(),
    mode: z.string(),
    wallet_public_id: z.string(),
    operator_public_id: z.string().nullable(),
    direction: z.string(),
    max_qty: z.number(),
    opened_at: z.string(),
    age_hours: z.number(),
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

export const ProcessMetricsSchema = z
  .object({
    pid: z.number().int(),
    uptime_seconds: z.number(),
    status: z.string(),
    num_threads: z.number().int(),
    num_fds: z.number().int(),
    num_connections: z.number().int(),
  })
  .strict()

export const ProcessStartDataSchema = z
  .object({
    type: z.literal('process_start'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    status: z.enum(['success', 'not_running', 'error']),
    name: z.string(),
    message: z.string().nullable().optional(),
  })
  .strict()

export const PushBetaConfigReadSchema = z
  .object({
    type: z.literal('push_beta_config_read'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    enabled: z.boolean(),
    user_public_ids: z.array(z.string()),
  })
  .strict()

export const RegisterDeviceBodySchema = z
  .object({
    device_token: z.string().min(64).max(64),
    device_id: z.string().min(1).max(64),
    env: z.enum(['sandbox', 'prod']),
    app_version: z.string().max(32).nullable().optional(),
    previews_mode: z.enum(['private', 'public']),
  })
  .strict()

export const RelationshipTypeEnumSchema = z.enum(['exact', 'derivative', 'proxy'])

export const RestRateExchangeStatsSchema = z
  .object({
    rps_1s: z.number(),
    rps_10s: z.number(),
    rps_60s: z.number(),
    limit_rps: z.number().nullable().optional(),
    utilization: z.number().nullable().optional(),
  })
  .strict()

export const RetentionPolicyResultSchema = z
  .object({
    table: z.string(),
    retain_days: z.number().int(),
    backlog_lookback_days: z.number().int(),
    day_start: z.string().nullable(),
    day_end: z.string().nullable(),
    archived_rows: z.number().int(),
    purged_rows: z.number().int(),
    files_written: z.number().int(),
    error: z.string().nullable(),
  })
  .strict()

export const RollPointDetailSchema = z
  .object({
    from_contract: z.string(),
    to_contract: z.string(),
    roll_at: z.string(),
  })
  .strict()

export const SaturationMetricsSchema = z
  .object({
    threads_pct: z.number().nullable(),
    fds_pct: z.number().nullable(),
  })
  .strict()

export const ScopeGrantInfoSchema = z
  .object({
    type: z.literal('scope_grant_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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

export const SignalDiffEntrySchema = z
  .object({
    instrument: z.string(),
    signal_time: z.iso.datetime(),
    signal_type: z.string(),
    leg: z.enum(['a', 'b', 'common']),
  })
  .strict()

export const StrategyProcessSchema = z
  .object({
    type: z.literal('strategy_process'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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

export const TableStatsItemSchema = z
  .object({
    table: z.string(),
    table_kind: z.enum(['event', 'state']),
    total: z.number().int().nullable(),
    current: z.number().int().nullable(),
    closed: z.number().int().nullable(),
    archivable: z.number().int().nullable(),
    is_stale: z.boolean(),
    last_sampled_at: z.iso.datetime(),
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

export const TracemallocStateSchema = z
  .object({
    active: z.boolean(),
    requested_duration_seconds: z.number().nullable(),
  })
  .strict()

export const TradeDiffEntrySchema = z
  .object({
    instrument: z.string(),
    executed_at: z.iso.datetime(),
    side: z.string(),
    quantity: z.number(),
    price: z.number(),
    leg: z.enum(['a', 'b', 'common']),
    pnl_a: z.number().nullable().optional(),
    pnl_b: z.number().nullable().optional(),
    pnl_delta: z.number().nullable().optional(),
  })
  .strict()

export const TrailingStopStateDataSchema = z
  .object({
    type: z.literal('trailing_stop_state'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    plan_public_id: z.string(),
    status: z.string(),
    trailing_pct: z.number(),
    min_lock_pct: z.number(),
    entry_price: z.number(),
    peak_price: z.number(),
    current_stop: z.number(),
    side: z.string(),
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

export const UserAlertDefaultBodySchema = z
  .object({
    alert_type: z.enum([
      'order_fill_full',
      'order_rejected',
      'position_stop_loss_fired',
      'margin_warning',
      'critical_system_error',
    ]),
    enabled: z.boolean(),
    min_priority: z.enum(['low', 'medium', 'high']),
  })
  .strict()

export const UserAlertDefaultInfoSchema = z
  .object({
    type: z.literal('user_alert_default_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    user_public_id: z.string(),
    alert_type: z.string(),
    enabled: z.boolean(),
    min_priority: z.string(),
  })
  .strict()

export const UserRoleSchema = z.enum(['ai_delegate', 'viewer', 'operator', 'admin'])

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
    topic: z.string().nullable().optional(),
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

export const WsTokenDataSchema = z
  .object({
    type: z.literal('ws_token'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    message: z.string(),
    ws_token: z.string(),
    ws_token_exp: z.iso.datetime(),
    expires_in: z.number().int(),
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

export const RefreshTokenPayloadSchema = z
  .object({
    active_wallet_public_id: z.string().nullable().optional(),
    clear_active_wallet: z.boolean().optional(),
  })
  .strict()

export const DeactivateUserBodySchema = z
  .object({
    reason: z.string().nullable().optional(),
  })
  .strict()

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

export const PushBetaUsersBodySchema = z
  .object({
    enabled: z.boolean(),
    user_public_ids: z.array(z.string()).optional(),
  })
  .strict()

export const RemoveSettingBodySchema = z.object({}).strict()

export const DelegateDeactivateBodySchema = z
  .object({
    reason: z.string().max(120).nullable().optional(),
  })
  .strict()

export const AiReviewDecisionRequestSchema = z
  .object({
    decision: z.string(),
    rationale: z.string().nullable().optional(),
  })
  .strict()

export const BacktestCompareBodySchema = z
  .object({
    mode: z.enum(['manual', 'auto']),
    run_a_public_id: z.string().nullable().optional(),
    run_b_public_id: z.string().nullable().optional(),
    config_hash: z.string().nullable().optional(),
    anchor_run_public_id: z.string().nullable().optional(),
  })
  .strict()

export const BacktestCancelBodySchema = z
  .object({
    reason: z.string().optional(),
  })
  .strict()

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
    ai_review_public_id: z.string().nullable().optional(),
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

export const RevokeScopeGrantBodySchema = z
  .object({
    reason: z.string().max(512).nullable().optional(),
  })
  .strict()

export const TrailingStopCreateBodySchema = z
  .object({
    position_cycle_public_id: z.string(),
    trailing_pct: z.number(),
    min_lock_pct: z.number().optional(),
    idempotency_key: z.string().nullable().optional(),
  })
  .strict()

export const TrailingStopCancelBodySchema = z
  .object({
    reason: z.string().nullable().optional(),
  })
  .strict()

export const CreateWalletBodySchema = z
  .object({
    label: z.string().min(1).max(128),
    description: z.string().max(512).nullable().optional(),
    is_paper: z.boolean().optional(),
  })
  .strict()

export const BacktestComparisonListResponseSchema = z
  .object({
    type: z.literal('backtest_comparison_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(BacktestComparisonDataSchema),
    count: z.number().int(),
  })
  .strict()

export const BacktestComparisonResponseSchema = z
  .object({
    type: z.literal('backtest_comparison_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: BacktestComparisonDataSchema,
  })
  .strict()

export const BacktestEquityPointListResponseSchema = z
  .object({
    type: z.literal('backtest_equity_point_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(BacktestEquityPointInlineSchema),
    count: z.number().int(),
  })
  .strict()

export const BacktestEventListResponseSchema = z
  .object({
    type: z.literal('backtest_event_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(BacktestEventDataSchema),
    count: z.number().int(),
  })
  .strict()

export const BacktestSignalListResponseSchema = z
  .object({
    type: z.literal('backtest_signal_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(BacktestSignalDataSchema),
    count: z.number().int(),
  })
  .strict()

export const BacktestTradeListResponseSchema = z
  .object({
    type: z.literal('backtest_trade_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(BacktestTradeDataSchema),
    count: z.number().int(),
  })
  .strict()

export const ContinuousCandleListResponseSchema = z
  .object({
    type: z.literal('continuous_candle_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: CredentialSummarySchema,
  })
  .strict()

export const UpdateDevicePrefCommandSchema = z
  .object({
    type: z.literal('update_device_pref_command'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: DeviceAlertPrefBodySchema,
  })
  .strict()

export const DeviceAlertPrefListResponseSchema = z
  .object({
    type: z.literal('device_alert_pref_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(DeviceAlertPrefInfoSchema),
    count: z.number().int(),
  })
  .strict()

export const DeviceAlertPrefResponseSchema = z
  .object({
    type: z.literal('device_alert_pref_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: DeviceAlertPrefInfoSchema,
  })
  .strict()

export const ExecutionListResponseSchema = z
  .object({
    type: z.literal('execution_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: ExecutionPlanDataSchema,
  })
  .strict()

export const FeatureFlagsResponseSchema = z
  .object({
    type: z.literal('feature_flags_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: FeatureFlagsPayloadSchema,
  })
  .strict()

export const FrontMonthResponseSchema = z
  .object({
    type: z.literal('front_month'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: FrontMonthDataSchema,
  })
  .strict()

export const GapDetectionStatsSchema = z
  .object({
    bridge: GapStatsSchema,
    rest_clients: z.record(z.string(), GapStatsSchema),
  })
  .strict()

export const InstrumentDetailListResponseSchema = z
  .object({
    type: z.literal('instrument_detail_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(InstrumentDetailDataSchema),
    count: z.number().int(),
  })
  .strict()

export const JsonValueSchema = z.unknown()

export const NotificationDeviceListResponseSchema = z
  .object({
    type: z.literal('notification_device_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(NotificationDeviceInfoSchema),
    count: z.number().int(),
  })
  .strict()

export const NotificationDeviceResponseSchema = z
  .object({
    type: z.literal('notification_device_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: NotificationDeviceInfoSchema,
  })
  .strict()

export const NotificationMetricsResponseSchema = z
  .object({
    type: z.literal('notification_metrics_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: NotificationMetricsDataSchema,
  })
  .strict()

export const OperatorListResponseSchema = z
  .object({
    type: z.literal('operator_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: z.array(OrderDataSchema),
    count: z.number().int(),
  })
  .strict()

export const OrphanSweepResponseSchema = z
  .object({
    type: z.literal('orphan_sweep_result'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: OrphanSweepResultDataSchema,
  })
  .strict()

export const PendingReviewListResponseSchema = z
  .object({
    items: z.array(PendingReviewSummaryItemSchema),
    count: z.number().int(),
  })
  .strict()

export const PositionCycleListResponseSchema = z
  .object({
    type: z.literal('position_cycles'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(PositionCycleDataSchema),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: ProcessStopDataSchema,
  })
  .strict()

export const PushBetaConfigResponseSchema = z
  .object({
    type: z.literal('push_beta_config_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: PushBetaConfigReadSchema,
  })
  .strict()

export const RegisterDeviceCommandSchema = z
  .object({
    type: z.literal('register_device_command'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: RegisterDeviceBodySchema,
  })
  .strict()

export const RestRateDataSchema = z
  .object({
    type: z.literal('rest_rate'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    exchanges: z.record(z.string(), RestRateExchangeStatsSchema),
  })
  .strict()

export const RetentionRunDataSchema = z
  .object({
    type: z.literal('retention_run'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    run_started_at: z.iso.datetime(),
    run_completed_at: z.iso.datetime(),
    dry_run: z.boolean(),
    results: z.array(RetentionPolicyResultSchema),
  })
  .strict()

export const ContinuousSeriesPartialResponseSchema = z
  .object({
    type: z.literal('continuous_partial'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(ContinuousCandleDataSchema),
    count: z.number().int(),
    failed_roll: RollPointDetailSchema,
    message: z.string(),
  })
  .strict()

export const SystemMetricsDataSchema = z
  .object({
    type: z.literal('system_metrics'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    bus_time: z.iso.datetime(),
    process: ProcessMetricsSchema,
    cpu: CpuMetricsSchema,
    memory: MemoryMetricsSchema,
    asyncio: AsyncioMetricsSchema,
    gc: GcMetricsSchema,
    limits: LimitsMetricsSchema,
    saturation: SaturationMetricsSchema,
    db_internal: DbInternalMetricsSchema,
    tracemalloc_active: z.boolean(),
    cgroup_version: z.enum(['v1', 'v2']).nullable(),
  })
  .strict()

export const SystemMetricsHistoryItemSchema = z
  .object({
    type: z.literal('system_metrics_history_item'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    bus_time: z.iso.datetime(),
    process: ProcessMetricsSchema,
    cpu: CpuMetricsSchema,
    memory: MemoryMetricsSchema,
    asyncio: AsyncioMetricsSchema,
    gc: GcMetricsSchema,
    limits: LimitsMetricsSchema,
    saturation: SaturationMetricsSchema,
    db_internal: DbInternalMetricsSchema,
    tracemalloc_active: z.boolean(),
    cgroup_version: z.enum(['v1', 'v2']).nullable(),
  })
  .strict()

export const HandoverScopeGrantResultSchema = z
  .object({
    closed_grant: ScopeGrantInfoSchema,
    new_grant: ScopeGrantInfoSchema,
  })
  .strict()

export const RevokeScopeGrantResponseSchema = z
  .object({
    type: z.literal('revoke_scope_grant_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: ScopeGrantInfoSchema,
  })
  .strict()

export const ScopeGrantListResponseSchema = z
  .object({
    type: z.literal('scope_grant_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: z.array(StrategyProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const DbStatsDataSchema = z
  .object({
    type: z.literal('db_stats'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    snapshot_started_at: z.iso.datetime(),
    snapshot_completed_at: z.iso.datetime(),
    interval_seconds: z.number().int(),
    tables: z.array(TableStatsItemSchema),
  })
  .strict()

export const TracemallocStateResponseSchema = z
  .object({
    type: z.literal('tracemalloc_state_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: TracemallocStateSchema,
  })
  .strict()

export const TrailingStopStateResponseSchema = z
  .object({
    type: z.literal('trailing_stop_state'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: TrailingStopStateDataSchema,
  })
  .strict()

export const UnderlyingAssetListResponseSchema = z
  .object({
    type: z.literal('underlying_asset_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: z.array(UnderlyingInstrumentDataSchema),
    count: z.number().int(),
  })
  .strict()

export const UpdateUserAlertDefaultCommandSchema = z
  .object({
    type: z.literal('update_user_alert_default_command'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: UserAlertDefaultBodySchema,
  })
  .strict()

export const UserAlertDefaultListResponseSchema = z
  .object({
    type: z.literal('user_alert_default_list_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(UserAlertDefaultInfoSchema),
    count: z.number().int(),
  })
  .strict()

export const UserAlertDefaultResponseSchema = z
  .object({
    type: z.literal('user_alert_default_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: UserAlertDefaultInfoSchema,
  })
  .strict()

export const UserProfileSchema = z
  .object({
    type: z.literal('user_profile'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    username: z.string(),
    email: z.string().nullable().optional(),
    role: UserRoleSchema,
    is_active: z.boolean(),
    created_at: z.iso.datetime(),
    operator_public_ids: z.array(z.string()).optional(),
    primary_operator_public_id: z.string().nullable().optional(),
    active_wallet_public_id: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: WalletInfoSchema,
  })
  .strict()

export const WsTokenResponseSchema = z
  .object({
    type: z.literal('ws_token_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: WsTokenDataSchema,
  })
  .strict()

export const WsStatsDataSchema = z
  .object({
    type: z.literal('ws_stats'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: LoginBodySchema,
  })
  .strict()

export const RefreshTokenRequestSchema = z
  .object({
    type: z.literal('refresh_token_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: RefreshTokenPayloadSchema,
  })
  .strict()

export const DeactivateUserRequestSchema = z
  .object({
    type: z.literal('deactivate_user_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: SettingUpdateBodySchema,
  })
  .strict()

export const UpdatePushBetaUsersCommandSchema = z
  .object({
    type: z.literal('update_push_beta_users_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: PushBetaUsersBodySchema,
  })
  .strict()

export const RemoveSettingRequestSchema = z
  .object({
    type: z.literal('remove_setting_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: RemoveSettingBodySchema,
  })
  .strict()

export const DelegateDeactivateRequestSchema = z
  .object({
    type: z.literal('delegate_deactivate_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: DelegateDeactivateBodySchema,
  })
  .strict()

export const BacktestCompareRequestSchema = z
  .object({
    type: z.literal('backtest_compare_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: BacktestCompareBodySchema,
  })
  .strict()

export const BacktestCancelCommandSchema = z
  .object({
    type: z.literal('backtest_cancel_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: BacktestCancelBodySchema,
  })
  .strict()

export const CreateCredentialCommandSchema = z
  .object({
    type: z.literal('create_credential_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: HandoverScopeGrantBodySchema,
  })
  .strict()

export const RevokeScopeGrantCommandSchema = z
  .object({
    type: z.literal('revoke_scope_grant_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: RevokeScopeGrantBodySchema,
  })
  .strict()

export const TrailingStopCreateCommandSchema = z
  .object({
    type: z.literal('create_trailing_stop_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: TrailingStopCreateBodySchema,
  })
  .strict()

export const TrailingStopCancelCommandSchema = z
  .object({
    type: z.literal('cancel_trailing_stop_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: TrailingStopCancelBodySchema,
  })
  .strict()

export const CreateWalletCommandSchema = z
  .object({
    type: z.literal('create_wallet_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: ProcessCreateDataSchema,
  })
  .strict()

export const RestRateResponseSchema = z
  .object({
    type: z.literal('rest_rate_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: RestRateDataSchema,
  })
  .strict()

export const RetentionRunResponseSchema = z
  .object({
    type: z.literal('retention_run_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: RetentionRunDataSchema,
  })
  .strict()

export const SystemMetricsResponseSchema = z
  .object({
    type: z.literal('system_metrics_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: SystemMetricsDataSchema,
  })
  .strict()

export const SystemMetricsHistoryResponseSchema = z
  .object({
    type: z.literal('system_metrics_history_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(SystemMetricsHistoryItemSchema),
    count: z.number().int(),
  })
  .strict()

export const HandoverScopeGrantResponseSchema = z
  .object({
    type: z.literal('handover_scope_grant_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: HandoverScopeGrantResultSchema,
  })
  .strict()

export const DbStatsResponseSchema = z
  .object({
    type: z.literal('db_stats_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: DbStatsDataSchema,
  })
  .strict()

export const LoginDataSchema = z
  .object({
    type: z.literal('login'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    message: z.string(),
    expires_in: z.number().int(),
    user: UserProfileSchema,
    access_token: z.string().nullable().optional(),
    refresh_token: z.string().nullable().optional(),
  })
  .strict()

export const RefreshDataSchema = z
  .object({
    type: z.literal('refresh'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    message: z.string(),
    ws_token: z.string(),
    ws_token_exp: z.iso.datetime(),
    csrf_token: z.string(),
    user: UserProfileSchema,
    access_token: z.string().nullable().optional(),
    refresh_token: z.string().nullable().optional(),
  })
  .strict()

export const UserListResponseSchema = z
  .object({
    type: z.literal('user_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: HealthCheckDataSchema,
  })
  .strict()

export const AiReviewDecisionResponseSchema = z
  .object({
    success: z.boolean(),
    error_code: z.string().nullable(),
    message: z.string(),
    details: z.record(z.string(), z.any()),
  })
  .strict()

export const AlertEventInfoSchema = z
  .object({
    type: z.literal('alert_event_info'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    user_public_id: z.string(),
    operator_public_id: z.string().nullable().optional(),
    wallet_public_id: z.string().nullable().optional(),
    alert_type: z.string(),
    priority: z.string(),
    is_safety_critical: z.boolean(),
    title: z.string(),
    body: z.string(),
    payload: z.record(z.string(), z.any()).nullable().optional(),
    dedup_key: z.string().nullable().optional(),
    thread_key: z.string().nullable().optional(),
    source_topic: z.string().nullable().optional(),
  })
  .strict()

export const AvailableProcessSchema = z
  .object({
    type: z.literal('available_process'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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

export const BacktestResultInlineSchema = z
  .object({
    total_trades: z.number().int(),
    winning_trades: z.number().int(),
    losing_trades: z.number().int(),
    total_pnl: z.number(),
    max_drawdown: z.number(),
    sharpe_ratio: z.number().nullable().optional(),
    win_rate: z.number().nullable().optional(),
    profit_factor: z.number().nullable().optional(),
    final_equity: z.number(),
    max_equity: z.number(),
    sortino_ratio: z.number().nullable().optional(),
    cagr: z.number().nullable().optional(),
    calmar_ratio: z.number().nullable().optional(),
    expectancy: z.number().nullable().optional(),
    avg_trade_pnl: z.number().nullable().optional(),
    max_drawdown_duration_seconds: z.number().nullable().optional(),
    exposure_ratio: z.number().nullable().optional(),
    turnover_ratio: z.number().nullable().optional(),
    extra_metrics: z.record(z.string(), z.any()).optional(),
  })
  .strict()

export const BacktestRunDataSchema = z
  .object({
    type: z.literal('backtest_run'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    wallet_public_id: z.string(),
    strategy_name: z.string(),
    strategy_params: z.record(z.string(), z.any()),
    instrument_public_id: z.string(),
    exchange: z.string(),
    timeframe: z.string(),
    start_date: z.iso.datetime(),
    end_date: z.iso.datetime(),
    initial_cash: z.number(),
    status: z.string(),
    execution_mode: z.string(),
    fill_model: z.string(),
    slippage_bps: z.number(),
    commission_bps: z.number(),
    config_hash: z.string().nullable().optional(),
    target_execution_exchange: z.string().nullable().optional(),
    started_at: z.iso.datetime().nullable().optional(),
    completed_at: z.iso.datetime().nullable().optional(),
    error: z.string().nullable().optional(),
  })
  .strict()

export const ConfiguredProcessSchema = z
  .object({
    type: z.literal('configured_process'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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

export const DelegateCapsBodySchema = z
  .object({
    max_order_quantity_per_instrument: z.record(z.string(), z.any()).nullable().optional(),
    max_open_orders: z.number().int().nullable().optional(),
    max_daily_notional_usd: z.number().nullable().optional(),
    max_cancels_per_minute: z.number().int().nullable().optional(),
  })
  .strict()

export const ProcessRunSchema = z
  .object({
    type: z.literal('process_run'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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

export const BacktestCreateBodySchema = z
  .object({
    strategy_class: z.string(),
    instrument_public_id: z.string(),
    exchange: z.string(),
    timeframe: z.string().optional(),
    start_date: z.iso.datetime(),
    end_date: z.iso.datetime(),
    initial_cash: z.number().optional(),
    strategy_params: z.record(z.string(), z.any()).optional(),
    execution_mode: z.string().optional(),
    fill_model: z.string().optional(),
    slippage_bps: z.number().optional(),
    commission_bps: z.number().optional(),
    target_execution_exchange: z
      .enum(['paper', 'kraken', 'kraken_futures', 'walutomat'])
      .nullable()
      .optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: RefreshDataSchema,
  })
  .strict()

export const AlertEventResponseSchema = z
  .object({
    type: z.literal('alert_event_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: AlertEventInfoSchema,
  })
  .strict()

export const AlertHistoryResponseSchema = z
  .object({
    type: z.literal('alert_history_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(AlertEventInfoSchema),
    count: z.number().int(),
    next_cursor: z.string().nullable().optional(),
  })
  .strict()

export const AvailableProcessesResponseSchema = z
  .object({
    type: z.literal('available_processes'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(AvailableProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const BacktestRunDetailDataSchema = z
  .object({
    type: z.literal('backtest_run'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    wallet_public_id: z.string(),
    strategy_name: z.string(),
    strategy_params: z.record(z.string(), z.any()),
    instrument_public_id: z.string(),
    exchange: z.string(),
    timeframe: z.string(),
    start_date: z.iso.datetime(),
    end_date: z.iso.datetime(),
    initial_cash: z.number(),
    status: z.string(),
    execution_mode: z.string(),
    fill_model: z.string(),
    slippage_bps: z.number(),
    commission_bps: z.number(),
    config_hash: z.string().nullable().optional(),
    target_execution_exchange: z.string().nullable().optional(),
    started_at: z.iso.datetime().nullable().optional(),
    completed_at: z.iso.datetime().nullable().optional(),
    error: z.string().nullable().optional(),
    result: BacktestResultInlineSchema.nullable().optional(),
  })
  .strict()

export const BacktestComparisonDetailResponseDataSchema = z
  .object({
    type: z.literal('backtest_comparison_detail'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    comparison: BacktestComparisonDataSchema,
    run_a: BacktestRunDataSchema,
    run_b: BacktestRunDataSchema,
    metrics_diff: z.array(MetricDiffRowSchema),
    equity_overlay: z.array(EquityOverlayPointSchema),
    trades_diff: z.array(TradeDiffEntrySchema),
    signals_diff: z.array(SignalDiffEntrySchema),
  })
  .strict()

export const BacktestRunListResponseSchema = z
  .object({
    type: z.literal('backtest_run_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(BacktestRunDataSchema),
    count: z.number().int(),
  })
  .strict()

export const BacktestRunResponseSchema = z
  .object({
    type: z.literal('backtest_run_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: BacktestRunDataSchema,
  })
  .strict()

export const ConfiguredProcessesResponseSchema = z
  .object({
    type: z.literal('configured_processes'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(ConfiguredProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const DelegateReadSchema = z
  .object({
    public_id: z.string(),
    username: z.string(),
    label: z.string(),
    created_by_user_public_id: z.string(),
    created_at: z.iso.datetime(),
    is_active: z.boolean(),
    caps: DelegateCapsBodySchema,
  })
  .strict()

export const DelegateCreateBodySchema = z
  .object({
    label: z.string().min(1).max(48),
    caps: DelegateCapsBodySchema.optional(),
    operator_public_id: z.string().nullable().optional(),
  })
  .strict()

export const DelegateCapsUpdateBodySchema = z
  .object({
    caps: DelegateCapsBodySchema,
  })
  .strict()

export const ProcessRunsResponseSchema = z
  .object({
    type: z.literal('process_runs'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    trader: ProcessStatusSchema,
    backtests: z.record(z.string(), ProcessStatusSchema),
    strategies: z.array(StrategyStatusPayloadSchema),
  })
  .strict()

export const BacktestCreateCommandSchema = z
  .object({
    type: z.literal('backtest_create_command').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: BacktestCreateBodySchema,
  })
  .strict()

export const ProcessCreateRequestSchema = z
  .object({
    type: z.literal('process_create_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
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
    topic: z.string().nullable().optional(),
    payload: ProcessStartBodySchema,
  })
  .strict()

export const BacktestRunDetailResponseSchema = z
  .object({
    type: z.literal('backtest_run_detail_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: BacktestRunDetailDataSchema,
  })
  .strict()

export const BacktestComparisonDetailResponseSchema = z
  .object({
    type: z.literal('backtest_comparison_detail_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: BacktestComparisonDetailResponseDataSchema,
  })
  .strict()

export const DelegateCreatedPayloadSchema = z
  .object({
    delegate: DelegateReadSchema,
    access_token: z.string(),
    expires_in: z.number().int(),
  })
  .strict()

export const DelegateListResponseSchema = z
  .object({
    type: z.literal('delegate_list'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: z.array(DelegateReadSchema),
    count: z.number().int(),
  })
  .strict()

export const DelegateResponseSchema = z
  .object({
    type: z.literal('delegate_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: DelegateReadSchema,
  })
  .strict()

export const DelegateCreateRequestSchema = z
  .object({
    type: z.literal('delegate_create_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: DelegateCreateBodySchema,
  })
  .strict()

export const DelegateCapsUpdateRequestSchema = z
  .object({
    type: z.literal('delegate_caps_update_request').optional(),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: DelegateCapsUpdateBodySchema,
  })
  .strict()

export const SystemStatusResponseSchema = z
  .object({
    type: z.literal('system_status_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: SystemStatusDataSchema,
  })
  .strict()

export const DelegateCreatedResponseSchema = z
  .object({
    type: z.literal('delegate_created_response'),
    sequence_id: z.number().int(),
    public_id: z.string(),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    topic: z.string().nullable().optional(),
    payload: DelegateCreatedPayloadSchema,
  })
  .strict()

// Type exports
export type AsyncioMetrics = z.infer<typeof AsyncioMetricsSchema>
export type BacktestComparisonData = z.infer<typeof BacktestComparisonDataSchema>
export type BacktestEquityPointInline = z.infer<typeof BacktestEquityPointInlineSchema>
export type BacktestEventData = z.infer<typeof BacktestEventDataSchema>
export type BacktestSignalData = z.infer<typeof BacktestSignalDataSchema>
export type BacktestTradeData = z.infer<typeof BacktestTradeDataSchema>
export type ConnectionStats = z.infer<typeof ConnectionStatsSchema>
export type ContinuousCandleData = z.infer<typeof ContinuousCandleDataSchema>
export type ContractData = z.infer<typeof ContractDataSchema>
export type CpuMetrics = z.infer<typeof CpuMetricsSchema>
export type CredentialSummary = z.infer<typeof CredentialSummarySchema>
export type DbInternalMetrics = z.infer<typeof DbInternalMetricsSchema>
export type DeviceAlertPrefBody = z.infer<typeof DeviceAlertPrefBodySchema>
export type DeviceAlertPrefInfo = z.infer<typeof DeviceAlertPrefInfoSchema>
export type EquityOverlayPoint = z.infer<typeof EquityOverlayPointSchema>
export type ExchangeListResponse = z.infer<typeof ExchangeListResponseSchema>
export type ExecutionData = z.infer<typeof ExecutionDataSchema>
export type ExecutionPlanData = z.infer<typeof ExecutionPlanDataSchema>
export type FeatureFlagsPayload = z.infer<typeof FeatureFlagsPayloadSchema>
export type FrontMonthData = z.infer<typeof FrontMonthDataSchema>
export type GapStats = z.infer<typeof GapStatsSchema>
export type GcMetrics = z.infer<typeof GcMetricsSchema>
export type HealthTopics = z.infer<typeof HealthTopicsSchema>
export type InstrumentDetailData = z.infer<typeof InstrumentDetailDataSchema>
export type InstrumentListResponse = z.infer<typeof InstrumentListResponseSchema>
export type JsonPrimitive = z.infer<typeof JsonPrimitiveSchema>
export type LimitsMetrics = z.infer<typeof LimitsMetricsSchema>
export type MemoryMetrics = z.infer<typeof MemoryMetricsSchema>
export type MessageResponse = z.infer<typeof MessageResponseSchema>
export type MetricDiffRow = z.infer<typeof MetricDiffRowSchema>
export type NotificationDeviceInfo = z.infer<typeof NotificationDeviceInfoSchema>
export type NotificationMetricsData = z.infer<typeof NotificationMetricsDataSchema>
export type OperatorInfo = z.infer<typeof OperatorInfoSchema>
export type OrderData = z.infer<typeof OrderDataSchema>
export type OrphanSweepResultData = z.infer<typeof OrphanSweepResultDataSchema>
export type PendingReviewSummaryItem = z.infer<typeof PendingReviewSummaryItemSchema>
export type PositionCycleData = z.infer<typeof PositionCycleDataSchema>
export type PositionData = z.infer<typeof PositionDataSchema>
export type ProcessCategoryCount = z.infer<typeof ProcessCategoryCountSchema>
export type ProcessCreatedInfo = z.infer<typeof ProcessCreatedInfoSchema>
export type ProcessMetrics = z.infer<typeof ProcessMetricsSchema>
export type ProcessStartData = z.infer<typeof ProcessStartDataSchema>
export type ProcessStatus = z.infer<typeof ProcessStatusSchema>
export type ProcessStopData = z.infer<typeof ProcessStopDataSchema>
export type PushBetaConfigRead = z.infer<typeof PushBetaConfigReadSchema>
export type RegisterDeviceBody = z.infer<typeof RegisterDeviceBodySchema>
export type RelationshipTypeEnum = z.infer<typeof RelationshipTypeEnumSchema>
export type RestRateExchangeStats = z.infer<typeof RestRateExchangeStatsSchema>
export type RetentionPolicyResult = z.infer<typeof RetentionPolicyResultSchema>
export type RollPointDetail = z.infer<typeof RollPointDetailSchema>
export type SaturationMetrics = z.infer<typeof SaturationMetricsSchema>
export type ScopeGrantInfo = z.infer<typeof ScopeGrantInfoSchema>
export type SettingCategoriesResponse = z.infer<typeof SettingCategoriesResponseSchema>
export type SettingRead = z.infer<typeof SettingReadSchema>
export type SignalData = z.infer<typeof SignalDataSchema>
export type SignalDiffEntry = z.infer<typeof SignalDiffEntrySchema>
export type StrategyProcess = z.infer<typeof StrategyProcessSchema>
export type SubscriptionsStats = z.infer<typeof SubscriptionsStatsSchema>
export type TableStatsItem = z.infer<typeof TableStatsItemSchema>
export type TopicMetricSnapshot = z.infer<typeof TopicMetricSnapshotSchema>
export type TracemallocState = z.infer<typeof TracemallocStateSchema>
export type TradeDiffEntry = z.infer<typeof TradeDiffEntrySchema>
export type TrailingStopStateData = z.infer<typeof TrailingStopStateDataSchema>
export type UnderlyingAssetData = z.infer<typeof UnderlyingAssetDataSchema>
export type UnderlyingInstrumentData = z.infer<typeof UnderlyingInstrumentDataSchema>
export type UserAlertDefaultBody = z.infer<typeof UserAlertDefaultBodySchema>
export type UserAlertDefaultInfo = z.infer<typeof UserAlertDefaultInfoSchema>
export type UserRole = z.infer<typeof UserRoleSchema>
export type ValidationError = z.infer<typeof ValidationErrorSchema>
export type WalletInfo = z.infer<typeof WalletInfoSchema>
export type WebSocketStats = z.infer<typeof WebSocketStatsSchema>
export type WsStatsConfig = z.infer<typeof WsStatsConfigSchema>
export type WsTokenData = z.infer<typeof WsTokenDataSchema>
export type ZmqBridgeStats = z.infer<typeof ZmqBridgeStatsSchema>
export type ZmqComponents = z.infer<typeof ZmqComponentsSchema>
export type ZmqConfig = z.infer<typeof ZmqConfigSchema>
export type LoginBody = z.infer<typeof LoginBodySchema>
export type RefreshTokenPayload = z.infer<typeof RefreshTokenPayloadSchema>
export type DeactivateUserBody = z.infer<typeof DeactivateUserBodySchema>
export type ChangePasswordBody = z.infer<typeof ChangePasswordBodySchema>
export type AdminResetPasswordBody = z.infer<typeof AdminResetPasswordBodySchema>
export type SettingUpdateBody = z.infer<typeof SettingUpdateBodySchema>
export type PushBetaUsersBody = z.infer<typeof PushBetaUsersBodySchema>
export type RemoveSettingBody = z.infer<typeof RemoveSettingBodySchema>
export type DelegateDeactivateBody = z.infer<typeof DelegateDeactivateBodySchema>
export type AiReviewDecisionRequest = z.infer<typeof AiReviewDecisionRequestSchema>
export type BacktestCompareBody = z.infer<typeof BacktestCompareBodySchema>
export type BacktestCancelBody = z.infer<typeof BacktestCancelBodySchema>
export type CreateCredentialBody = z.infer<typeof CreateCredentialBodySchema>
export type RotateCredentialBody = z.infer<typeof RotateCredentialBodySchema>
export type BracketCreateBody = z.infer<typeof BracketCreateBodySchema>
export type BracketCancelBody = z.infer<typeof BracketCancelBodySchema>
export type CreateOrderBody = z.infer<typeof CreateOrderBodySchema>
export type CancelOrderBody = z.infer<typeof CancelOrderBodySchema>
export type CreateScopeGrantBody = z.infer<typeof CreateScopeGrantBodySchema>
export type HandoverScopeGrantBody = z.infer<typeof HandoverScopeGrantBodySchema>
export type RevokeScopeGrantBody = z.infer<typeof RevokeScopeGrantBodySchema>
export type TrailingStopCreateBody = z.infer<typeof TrailingStopCreateBodySchema>
export type TrailingStopCancelBody = z.infer<typeof TrailingStopCancelBodySchema>
export type CreateWalletBody = z.infer<typeof CreateWalletBodySchema>
export type BacktestComparisonListResponse = z.infer<typeof BacktestComparisonListResponseSchema>
export type BacktestComparisonResponse = z.infer<typeof BacktestComparisonResponseSchema>
export type BacktestEquityPointListResponse = z.infer<typeof BacktestEquityPointListResponseSchema>
export type BacktestEventListResponse = z.infer<typeof BacktestEventListResponseSchema>
export type BacktestSignalListResponse = z.infer<typeof BacktestSignalListResponseSchema>
export type BacktestTradeListResponse = z.infer<typeof BacktestTradeListResponseSchema>
export type ContinuousCandleListResponse = z.infer<typeof ContinuousCandleListResponseSchema>
export type ContractListResponse = z.infer<typeof ContractListResponseSchema>
export type CredentialListResponse = z.infer<typeof CredentialListResponseSchema>
export type CredentialResponse = z.infer<typeof CredentialResponseSchema>
export type UpdateDevicePrefCommand = z.infer<typeof UpdateDevicePrefCommandSchema>
export type DeviceAlertPrefListResponse = z.infer<typeof DeviceAlertPrefListResponseSchema>
export type DeviceAlertPrefResponse = z.infer<typeof DeviceAlertPrefResponseSchema>
export type ExecutionListResponse = z.infer<typeof ExecutionListResponseSchema>
export type ExecutionPlanResponse = z.infer<typeof ExecutionPlanResponseSchema>
export type FeatureFlagsResponse = z.infer<typeof FeatureFlagsResponseSchema>
export type FrontMonthResponse = z.infer<typeof FrontMonthResponseSchema>
export type GapDetectionStats = z.infer<typeof GapDetectionStatsSchema>
export type InstrumentDetailListResponse = z.infer<typeof InstrumentDetailListResponseSchema>
export type JsonValue = z.infer<typeof JsonValueSchema>
export type NotificationDeviceListResponse = z.infer<typeof NotificationDeviceListResponseSchema>
export type NotificationDeviceResponse = z.infer<typeof NotificationDeviceResponseSchema>
export type NotificationMetricsResponse = z.infer<typeof NotificationMetricsResponseSchema>
export type OperatorListResponse = z.infer<typeof OperatorListResponseSchema>
export type OrderListResponse = z.infer<typeof OrderListResponseSchema>
export type OrphanSweepResponse = z.infer<typeof OrphanSweepResponseSchema>
export type PendingReviewListResponse = z.infer<typeof PendingReviewListResponseSchema>
export type PositionCycleListResponse = z.infer<typeof PositionCycleListResponseSchema>
export type PositionListResponse = z.infer<typeof PositionListResponseSchema>
export type ProcessSummaryData = z.infer<typeof ProcessSummaryDataSchema>
export type ProcessCreateData = z.infer<typeof ProcessCreateDataSchema>
export type ProcessStartResponse = z.infer<typeof ProcessStartResponseSchema>
export type ProcessStopResponse = z.infer<typeof ProcessStopResponseSchema>
export type PushBetaConfigResponse = z.infer<typeof PushBetaConfigResponseSchema>
export type RegisterDeviceCommand = z.infer<typeof RegisterDeviceCommandSchema>
export type RestRateData = z.infer<typeof RestRateDataSchema>
export type RetentionRunData = z.infer<typeof RetentionRunDataSchema>
export type ContinuousSeriesPartialResponse = z.infer<typeof ContinuousSeriesPartialResponseSchema>
export type SystemMetricsData = z.infer<typeof SystemMetricsDataSchema>
export type SystemMetricsHistoryItem = z.infer<typeof SystemMetricsHistoryItemSchema>
export type HandoverScopeGrantResult = z.infer<typeof HandoverScopeGrantResultSchema>
export type RevokeScopeGrantResponse = z.infer<typeof RevokeScopeGrantResponseSchema>
export type ScopeGrantListResponse = z.infer<typeof ScopeGrantListResponseSchema>
export type ScopeGrantResponse = z.infer<typeof ScopeGrantResponseSchema>
export type SettingListResponse = z.infer<typeof SettingListResponseSchema>
export type SettingResponse = z.infer<typeof SettingResponseSchema>
export type SignalListResponse = z.infer<typeof SignalListResponseSchema>
export type StrategyListResponse = z.infer<typeof StrategyListResponseSchema>
export type DbStatsData = z.infer<typeof DbStatsDataSchema>
export type TracemallocStateResponse = z.infer<typeof TracemallocStateResponseSchema>
export type TrailingStopStateResponse = z.infer<typeof TrailingStopStateResponseSchema>
export type UnderlyingAssetListResponse = z.infer<typeof UnderlyingAssetListResponseSchema>
export type UnderlyingInstrumentListResponse = z.infer<
  typeof UnderlyingInstrumentListResponseSchema
>
export type UpdateUserAlertDefaultCommand = z.infer<typeof UpdateUserAlertDefaultCommandSchema>
export type UserAlertDefaultListResponse = z.infer<typeof UserAlertDefaultListResponseSchema>
export type UserAlertDefaultResponse = z.infer<typeof UserAlertDefaultResponseSchema>
export type UserProfile = z.infer<typeof UserProfileSchema>
export type CreateUserBody = z.infer<typeof CreateUserBodySchema>
export type UpdateUserBody = z.infer<typeof UpdateUserBodySchema>
export type HTTPValidationError = z.infer<typeof HTTPValidationErrorSchema>
export type WalletListResponse = z.infer<typeof WalletListResponseSchema>
export type WalletResponse = z.infer<typeof WalletResponseSchema>
export type WsTokenResponse = z.infer<typeof WsTokenResponseSchema>
export type WsStatsData = z.infer<typeof WsStatsDataSchema>
export type ZmqHealthData = z.infer<typeof ZmqHealthDataSchema>
export type LoginRequest = z.infer<typeof LoginRequestSchema>
export type RefreshTokenRequest = z.infer<typeof RefreshTokenRequestSchema>
export type DeactivateUserRequest = z.infer<typeof DeactivateUserRequestSchema>
export type ChangePasswordRequest = z.infer<typeof ChangePasswordRequestSchema>
export type AdminResetPasswordRequest = z.infer<typeof AdminResetPasswordRequestSchema>
export type SettingUpdate = z.infer<typeof SettingUpdateSchema>
export type UpdatePushBetaUsersCommand = z.infer<typeof UpdatePushBetaUsersCommandSchema>
export type RemoveSettingRequest = z.infer<typeof RemoveSettingRequestSchema>
export type DelegateDeactivateRequest = z.infer<typeof DelegateDeactivateRequestSchema>
export type BacktestCompareRequest = z.infer<typeof BacktestCompareRequestSchema>
export type BacktestCancelCommand = z.infer<typeof BacktestCancelCommandSchema>
export type CreateCredentialCommand = z.infer<typeof CreateCredentialCommandSchema>
export type RotateCredentialCommand = z.infer<typeof RotateCredentialCommandSchema>
export type BracketCreateCommand = z.infer<typeof BracketCreateCommandSchema>
export type BracketCancelCommand = z.infer<typeof BracketCancelCommandSchema>
export type CreateOrderCommand = z.infer<typeof CreateOrderCommandSchema>
export type CancelOrderCommand = z.infer<typeof CancelOrderCommandSchema>
export type CreateScopeGrantCommand = z.infer<typeof CreateScopeGrantCommandSchema>
export type HandoverScopeGrantCommand = z.infer<typeof HandoverScopeGrantCommandSchema>
export type RevokeScopeGrantCommand = z.infer<typeof RevokeScopeGrantCommandSchema>
export type TrailingStopCreateCommand = z.infer<typeof TrailingStopCreateCommandSchema>
export type TrailingStopCancelCommand = z.infer<typeof TrailingStopCancelCommandSchema>
export type CreateWalletCommand = z.infer<typeof CreateWalletCommandSchema>
export type HealthCheckData = z.infer<typeof HealthCheckDataSchema>
export type JsonObject = z.infer<typeof JsonObjectSchema>
export type ProcessSummaryResponse = z.infer<typeof ProcessSummaryResponseSchema>
export type ProcessCreateResponse = z.infer<typeof ProcessCreateResponseSchema>
export type RestRateResponse = z.infer<typeof RestRateResponseSchema>
export type RetentionRunResponse = z.infer<typeof RetentionRunResponseSchema>
export type SystemMetricsResponse = z.infer<typeof SystemMetricsResponseSchema>
export type SystemMetricsHistoryResponse = z.infer<typeof SystemMetricsHistoryResponseSchema>
export type HandoverScopeGrantResponse = z.infer<typeof HandoverScopeGrantResponseSchema>
export type DbStatsResponse = z.infer<typeof DbStatsResponseSchema>
export type LoginData = z.infer<typeof LoginDataSchema>
export type RefreshData = z.infer<typeof RefreshDataSchema>
export type UserListResponse = z.infer<typeof UserListResponseSchema>
export type UserResponse = z.infer<typeof UserResponseSchema>
export type CreateUserRequest = z.infer<typeof CreateUserRequestSchema>
export type UpdateUserRequest = z.infer<typeof UpdateUserRequestSchema>
export type WsStatsResponse = z.infer<typeof WsStatsResponseSchema>
export type ZmqHealthResponse = z.infer<typeof ZmqHealthResponseSchema>
export type HealthCheckResponse = z.infer<typeof HealthCheckResponseSchema>
export type AiReviewDecisionResponse = z.infer<typeof AiReviewDecisionResponseSchema>
export type AlertEventInfo = z.infer<typeof AlertEventInfoSchema>
export type AvailableProcess = z.infer<typeof AvailableProcessSchema>
export type BacktestResultInline = z.infer<typeof BacktestResultInlineSchema>
export type BacktestRunData = z.infer<typeof BacktestRunDataSchema>
export type ConfiguredProcess = z.infer<typeof ConfiguredProcessSchema>
export type DelegateCapsBody = z.infer<typeof DelegateCapsBodySchema>
export type ProcessRun = z.infer<typeof ProcessRunSchema>
export type ProcessSchemaData = z.infer<typeof ProcessSchemaDataSchema>
export type StrategyStatusPayload = z.infer<typeof StrategyStatusPayloadSchema>
export type BacktestCreateBody = z.infer<typeof BacktestCreateBodySchema>
export type ProcessCreateBody = z.infer<typeof ProcessCreateBodySchema>
export type ProcessStartBody = z.infer<typeof ProcessStartBodySchema>
export type LoginResponse = z.infer<typeof LoginResponseSchema>
export type RefreshResponse = z.infer<typeof RefreshResponseSchema>
export type AlertEventResponse = z.infer<typeof AlertEventResponseSchema>
export type AlertHistoryResponse = z.infer<typeof AlertHistoryResponseSchema>
export type AvailableProcessesResponse = z.infer<typeof AvailableProcessesResponseSchema>
export type BacktestRunDetailData = z.infer<typeof BacktestRunDetailDataSchema>
export type BacktestComparisonDetailResponseData = z.infer<
  typeof BacktestComparisonDetailResponseDataSchema
>
export type BacktestRunListResponse = z.infer<typeof BacktestRunListResponseSchema>
export type BacktestRunResponse = z.infer<typeof BacktestRunResponseSchema>
export type ConfiguredProcessesResponse = z.infer<typeof ConfiguredProcessesResponseSchema>
export type DelegateRead = z.infer<typeof DelegateReadSchema>
export type DelegateCreateBody = z.infer<typeof DelegateCreateBodySchema>
export type DelegateCapsUpdateBody = z.infer<typeof DelegateCapsUpdateBodySchema>
export type ProcessRunsResponse = z.infer<typeof ProcessRunsResponseSchema>
export type ProcessSchemaResponse = z.infer<typeof ProcessSchemaResponseSchema>
export type SystemStatusData = z.infer<typeof SystemStatusDataSchema>
export type BacktestCreateCommand = z.infer<typeof BacktestCreateCommandSchema>
export type ProcessCreateRequest = z.infer<typeof ProcessCreateRequestSchema>
export type ProcessStartRequest = z.infer<typeof ProcessStartRequestSchema>
export type BacktestRunDetailResponse = z.infer<typeof BacktestRunDetailResponseSchema>
export type BacktestComparisonDetailResponse = z.infer<
  typeof BacktestComparisonDetailResponseSchema
>
export type DelegateCreatedPayload = z.infer<typeof DelegateCreatedPayloadSchema>
export type DelegateListResponse = z.infer<typeof DelegateListResponseSchema>
export type DelegateResponse = z.infer<typeof DelegateResponseSchema>
export type DelegateCreateRequest = z.infer<typeof DelegateCreateRequestSchema>
export type DelegateCapsUpdateRequest = z.infer<typeof DelegateCapsUpdateRequestSchema>
export type SystemStatusResponse = z.infer<typeof SystemStatusResponseSchema>
export type DelegateCreatedResponse = z.infer<typeof DelegateCreatedResponseSchema>
