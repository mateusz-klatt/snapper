/**
 * Generated Zod schemas for REST API validation.
 * DO NOT EDIT - regenerate with: make ui-gen-api-zod
 */

import { z } from 'zod'

export const AdminResetPasswordRequestSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('admin_reset_password_request'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    new_password: z.string().min(8),
  })
  .strict()

export const AvailableProcessSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('available_process'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    name: z.string(),
    class_path: z.string(),
    method: z.string(),
    description: z.string(),
    lifecycle: z.enum(['long_running', 'one_shot']),
    role: z.enum(['core', 'task', 'strategy', 'backtest']),
    tags: z.array(z.string()).optional(),
    parameters_schema: z.record(z.string(), z.unknown()).nullable().optional(),
  })
  .strict()

export const ChangePasswordRequestSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('change_password_request'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    current_password: z.string(),
    new_password: z.string().min(8),
  })
  .strict()

export const ConfiguredProcessSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('configured_process'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    name: z.string(),
    enabled: z.boolean(),
    running: z.boolean(),
    mode: z.enum(['thread', 'process']),
    class_path: z.string(),
    method: z.string(),
    args: z.array(z.unknown()).optional(),
    kwargs: z.record(z.string(), z.unknown()).optional(),
    note: z.string().nullable().optional(),
    lifecycle: z.enum(['long_running', 'one_shot']),
    role: z.enum(['core', 'task', 'strategy', 'backtest']),
    tags: z.array(z.string()).optional(),
    parameters_schema: z.record(z.string(), z.unknown()).nullable().optional(),
    is_one_shot: z.boolean(),
    active_public_id: z.string().nullable().optional(),
  })
  .strict()

export const ConnectionStatsSchemaSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('connection_stats'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    active_connections: z.number().int(),
    zmq_subscribers: z.number().int(),
    subscriber_tasks: z.number().int(),
    active_topics: z.number().int(),
    active_clients: z.number().int(),
  })
  .strict()

export const ExchangeListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('exchange_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(z.string()),
    count: z.number().int(),
  })
  .strict()

export const ExecutionDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('execution'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    trade_id: z.string().nullable().optional(),
    exchange_order_id: z.string().nullable().optional(),
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

export const GapStatsSchemaSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('gap_stats'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    gaps_detected: z.number().int(),
    session_resets: z.number().int(),
    duplicates: z.number().int(),
    mid_stream_joins: z.number().int(),
    rejected_unstamped: z.number().int(),
  })
  .strict()

export const HealthTopicsSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('health_topics'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    active: z.number().int(),
  })
  .strict()

export const InstrumentListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('instrument_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(z.string()),
    count: z.number().int(),
  })
  .strict()

export const LoginRequestSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('login_request'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    username: z.string(),
    password: z.string(),
    remember_me: z.boolean(),
  })
  .strict()

export const MessageResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('message'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.string(),
  })
  .strict()

export const OrderDataSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('order'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    exchange_order_id: z.string().nullable().optional(),
    client_order_id: z.string(),
    instrument: z.string(),
    exchange: z.enum(['paper', 'kraken', 'zonda', 'walutomat']),
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
    created_at: z.iso.datetime().optional(),
    updated_at: z.iso.datetime().nullable().optional(),
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

export const ProcessCategoryCountSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_category_count'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    running: z.number().int(),
    total: z.number().int(),
  })
  .strict()

export const ProcessCreateRequestSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_create_request'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    name: z.string().min(3).max(64),
    template: z.string(),
    enabled: z.boolean().nullable().optional(),
    mode: z.enum(['thread', 'process']).nullable().optional(),
    args: z.array(z.unknown()).nullable().optional(),
    kwargs: z.record(z.string(), z.unknown()).nullable().optional(),
    note: z.string().max(512).nullable().optional(),
  })
  .strict()

export const ProcessCreatedInfoSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_created_info'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    name: z.string(),
    template: z.string(),
  })
  .strict()

export const ProcessRunSchema = z
  .object({
    public_id: z.string(),
    type: z.literal('process_run'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    process_name: z.string(),
    status: z.enum(['running', 'succeeded', 'failed', 'cancelled']),
    role: z.enum(['core', 'task', 'strategy', 'backtest']),
    lifecycle: z.enum(['long_running', 'one_shot']),
    parameters: z.record(z.string(), z.unknown()).nullable().optional(),
    result: z.record(z.string(), z.unknown()).nullable().optional(),
    error: z.string().nullable().optional(),
    tags: z.array(z.string()).optional(),
    started_at: z.string(),
    completed_at: z.string().nullable().optional(),
  })
  .strict()

export const ProcessSchemaResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_schema'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    name: z.string(),
    description: z.string(),
    class_path: z.string(),
    method: z.string(),
    default_enabled: z.boolean(),
    default_mode: z.enum(['thread', 'process']),
    default_args: z.array(z.unknown()).optional(),
    default_kwargs: z.record(z.string(), z.unknown()).optional(),
    lifecycle: z.enum(['long_running', 'one_shot']),
  })
  .strict()

export const ProcessStartRequestSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_start_request'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    mode: z.enum(['thread', 'process']).nullable().optional(),
    args: z.array(z.unknown()).nullable().optional(),
    kwargs: z.record(z.string(), z.unknown()).nullable().optional(),
    autostart: z.boolean().nullable().optional(),
  })
  .strict()

export const ProcessStartResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_start_response'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    status: z.enum(['success', 'already_running', 'error']),
    name: z.string(),
    process_public_id: z.string().nullable().optional(),
    message: z.string().nullable().optional(),
  })
  .strict()

export const ProcessStatusSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_status'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    status: z.enum(['not_running', 'running', 'stopped', 'completed', 'error']),
    pid: z.number().int().nullable().optional(),
    started_at: z.string().nullable().optional(),
    command: z.string().nullable().optional(),
    exit_code: z.number().int().nullable().optional(),
    error: z.string().nullable().optional(),
  })
  .strict()

export const ProcessStopResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_stop_response'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    status: z.enum(['success', 'not_running', 'error']),
    name: z.string(),
    message: z.string().nullable().optional(),
  })
  .strict()

export const SettingCategoriesResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('setting_categories'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(z.string()),
    count: z.number().int(),
  })
  .strict()

export const SettingReadSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('setting_read'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    key: z.string(),
    value: z.string(),
    category: z.string(),
    description: z.string().nullable().optional(),
    updated_at: z.iso.datetime(),
    updated_by: z.string().nullable().optional(),
  })
  .strict()

export const SettingUpdateSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('setting_update'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    value: z.string(),
    category: z.string(),
    description: z.string().nullable().optional(),
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
    price: z.number().nullable().optional(),
    strategy_name: z.string().nullable().optional(),
    fired_at: z.iso.datetime().optional(),
  })
  .strict()

export const StrategyProcessSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('strategy_process'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    name: z.string(),
    running: z.boolean(),
    enabled: z.boolean(),
    mode: z.enum(['thread', 'process']),
  })
  .strict()

export const StrategyStatusPayloadSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('strategy_status'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    strategy_name: z.string(),
    status: z.string(),
    details: z.record(z.string(), z.unknown()).optional(),
    signals_generated: z.number().int().nullable().optional(),
    trades_executed: z.number().int().nullable().optional(),
    last_signal: z.string().nullable().optional(),
    last_signal_time: z.string().nullable().optional(),
    pnl: z.number().nullable().optional(),
    pid: z.number().int().nullable().optional(),
    uptime: z.string().nullable().optional(),
  })
  .strict()

export const SubscriptionsStatsSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('subscriptions_stats'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    per_topic: z.record(z.string(), z.number().int()),
    per_client: z.record(z.string(), z.array(z.string())),
  })
  .strict()

export const TopicMetricSnapshotSchemaSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('topic_metric_snapshot'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
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

export const WebSocketStatsSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('websocket_stats'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    active_connections: z.number().int(),
    topic_subscribers: z.record(z.string(), z.number().int()),
    client_count: z.number().int(),
  })
  .strict()

export const WsStatsConfigSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('ws_stats_config'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    broker_xpub: z.string(),
    heartbeat_interval_ms: z.number().int(),
  })
  .strict()

export const ZmqBridgeStatsSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('zmq_bridge_stats'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    active_topics: z.number().int(),
    subscriber_tasks: z.number().int(),
    available_topics: z.array(z.string()),
  })
  .strict()

export const ZmqComponentsSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('zmq_components'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    zmq_context: z.enum(['ok', 'error']),
    websocket_manager: z.enum(['ok', 'error']),
    active_connections: z.number().int(),
  })
  .strict()

export const ZmqConfigSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('zmq_config'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    available_topics: z.array(z.string()),
  })
  .strict()

export const AvailableProcessesResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('available_processes'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(AvailableProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const ConfiguredProcessesResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('configured_processes'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(ConfiguredProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const ExecutionListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('execution_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(ExecutionDataSchema),
    count: z.number().int(),
  })
  .strict()

export const GapDetectionStatsSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('gap_detection_stats'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    bridge: GapStatsSchemaSchema,
    rest_clients: z.record(z.string(), GapStatsSchemaSchema).optional(),
  })
  .strict()

export const OrderListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('order_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(OrderDataSchema),
    count: z.number().int(),
  })
  .strict()

export const PositionListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('position_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(PositionDataSchema),
    count: z.number().int(),
  })
  .strict()

export const ProcessSummaryResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_summary'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    feeds: ProcessCategoryCountSchema,
    strategies: ProcessCategoryCountSchema,
    executors: ProcessCategoryCountSchema,
    brokers: ProcessCategoryCountSchema,
  })
  .strict()

export const ProcessCreateResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_create_response'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    status: z.literal('created'),
    process: ProcessCreatedInfoSchema,
  })
  .strict()

export const ProcessRunsResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('process_runs'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(ProcessRunSchema),
    count: z.number().int(),
  })
  .strict()

export const SettingListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('setting_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(SettingReadSchema),
    count: z.number().int(),
  })
  .strict()

export const SettingResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('setting_response'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: SettingReadSchema,
  })
  .strict()

export const SignalListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('signal_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(SignalDataSchema),
    count: z.number().int(),
  })
  .strict()

export const StrategyListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('strategy_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(StrategyProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const SystemStatusSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('system_status'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    trader: ProcessStatusSchema,
    backtests: z.record(z.string(), ProcessStatusSchema),
    strategies: z.array(StrategyStatusPayloadSchema).optional(),
  })
  .strict()

export const CreateUserRequestSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('create_user_request'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    username: z.string().min(3).max(64),
    email: z.string().max(255).nullable().optional(),
    password: z.string().min(8),
    role: UserRoleSchema,
    is_active: z.boolean(),
  })
  .strict()

export const UpdateUserRequestSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('update_user_request'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    email: z.string().max(255).nullable().optional(),
    role: UserRoleSchema.nullable().optional(),
    is_active: z.boolean().nullable().optional(),
  })
  .strict()

export const UserProfileSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('user_profile'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    username: z.string(),
    email: z.string().nullable().optional(),
    role: UserRoleSchema,
    is_active: z.boolean(),
    created_at: z.iso.datetime().optional(),
  })
  .strict()

export const HTTPValidationErrorSchema = z
  .object({
    detail: z.array(ValidationErrorSchema).optional(),
  })
  .strict()

export const WsStatsResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('ws_stats'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    websocket: WebSocketStatsSchema,
    zmq_bridge: ZmqBridgeStatsSchema,
    connections: ConnectionStatsSchemaSchema,
    topics: z.record(z.string(), TopicMetricSnapshotSchemaSchema),
    subscriptions: SubscriptionsStatsSchema,
    config: WsStatsConfigSchema,
  })
  .strict()

export const ZmqHealthResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('zmq_health'),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    status: z.enum(['healthy', 'warning', 'error']),
    components: ZmqComponentsSchema,
    config: ZmqConfigSchema,
    connections: ConnectionStatsSchemaSchema,
    message_stats: z.record(z.string(), TopicMetricSnapshotSchemaSchema),
    errors: z.array(z.string()).optional(),
  })
  .strict()

export const HealthCheckResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('health_check'),
    timestamp: z.iso.datetime(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    status: z.enum(['healthy', 'warning', 'error']),
    version: z.string(),
    connections: ConnectionStatsSchemaSchema,
    topics: HealthTopicsSchema,
    gap_detection: GapDetectionStatsSchema,
  })
  .strict()

export const LoginResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('login_response'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    message: z.string(),
    expires_in: z.number().int(),
    user: UserProfileSchema,
  })
  .strict()

export const RefreshResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('refresh_response'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    message: z.string(),
    ws_token: z.string(),
    ws_token_exp: z.iso.datetime(),
    csrf_token: z.string(),
    user: UserProfileSchema,
  })
  .strict()

export const UserListResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('user_list'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: z.array(UserProfileSchema),
    count: z.number().int(),
  })
  .strict()

export const UserResponseSchema = z
  .object({
    public_id: z.string().optional(),
    type: z.literal('user_response'),
    timestamp: z.iso.datetime().optional(),
    session_id: z.string(),
    sequence_id: z.number().int(),
    payload: UserProfileSchema,
  })
  .strict()

// Type exports
export type AdminResetPasswordRequest = z.infer<typeof AdminResetPasswordRequestSchema>
export type AvailableProcess = z.infer<typeof AvailableProcessSchema>
export type ChangePasswordRequest = z.infer<typeof ChangePasswordRequestSchema>
export type ConfiguredProcess = z.infer<typeof ConfiguredProcessSchema>
export type ConnectionStatsSchema = z.infer<typeof ConnectionStatsSchemaSchema>
export type ExchangeListResponse = z.infer<typeof ExchangeListResponseSchema>
export type ExecutionData = z.infer<typeof ExecutionDataSchema>
export type GapStatsSchema = z.infer<typeof GapStatsSchemaSchema>
export type HealthTopics = z.infer<typeof HealthTopicsSchema>
export type InstrumentListResponse = z.infer<typeof InstrumentListResponseSchema>
export type LoginRequest = z.infer<typeof LoginRequestSchema>
export type MessageResponse = z.infer<typeof MessageResponseSchema>
export type OrderData = z.infer<typeof OrderDataSchema>
export type PositionData = z.infer<typeof PositionDataSchema>
export type ProcessCategoryCount = z.infer<typeof ProcessCategoryCountSchema>
export type ProcessCreateRequest = z.infer<typeof ProcessCreateRequestSchema>
export type ProcessCreatedInfo = z.infer<typeof ProcessCreatedInfoSchema>
export type ProcessRun = z.infer<typeof ProcessRunSchema>
export type ProcessSchemaResponse = z.infer<typeof ProcessSchemaResponseSchema>
export type ProcessStartRequest = z.infer<typeof ProcessStartRequestSchema>
export type ProcessStartResponse = z.infer<typeof ProcessStartResponseSchema>
export type ProcessStatus = z.infer<typeof ProcessStatusSchema>
export type ProcessStopResponse = z.infer<typeof ProcessStopResponseSchema>
export type SettingCategoriesResponse = z.infer<typeof SettingCategoriesResponseSchema>
export type SettingRead = z.infer<typeof SettingReadSchema>
export type SettingUpdate = z.infer<typeof SettingUpdateSchema>
export type SignalData = z.infer<typeof SignalDataSchema>
export type StrategyProcess = z.infer<typeof StrategyProcessSchema>
export type StrategyStatusPayload = z.infer<typeof StrategyStatusPayloadSchema>
export type SubscriptionsStats = z.infer<typeof SubscriptionsStatsSchema>
export type TopicMetricSnapshotSchema = z.infer<typeof TopicMetricSnapshotSchemaSchema>
export type UserRole = z.infer<typeof UserRoleSchema>
export type ValidationError = z.infer<typeof ValidationErrorSchema>
export type WebSocketStats = z.infer<typeof WebSocketStatsSchema>
export type WsStatsConfig = z.infer<typeof WsStatsConfigSchema>
export type ZmqBridgeStats = z.infer<typeof ZmqBridgeStatsSchema>
export type ZmqComponents = z.infer<typeof ZmqComponentsSchema>
export type ZmqConfig = z.infer<typeof ZmqConfigSchema>
export type AvailableProcessesResponse = z.infer<typeof AvailableProcessesResponseSchema>
export type ConfiguredProcessesResponse = z.infer<typeof ConfiguredProcessesResponseSchema>
export type ExecutionListResponse = z.infer<typeof ExecutionListResponseSchema>
export type GapDetectionStats = z.infer<typeof GapDetectionStatsSchema>
export type OrderListResponse = z.infer<typeof OrderListResponseSchema>
export type PositionListResponse = z.infer<typeof PositionListResponseSchema>
export type ProcessSummaryResponse = z.infer<typeof ProcessSummaryResponseSchema>
export type ProcessCreateResponse = z.infer<typeof ProcessCreateResponseSchema>
export type ProcessRunsResponse = z.infer<typeof ProcessRunsResponseSchema>
export type SettingListResponse = z.infer<typeof SettingListResponseSchema>
export type SettingResponse = z.infer<typeof SettingResponseSchema>
export type SignalListResponse = z.infer<typeof SignalListResponseSchema>
export type StrategyListResponse = z.infer<typeof StrategyListResponseSchema>
export type SystemStatus = z.infer<typeof SystemStatusSchema>
export type CreateUserRequest = z.infer<typeof CreateUserRequestSchema>
export type UpdateUserRequest = z.infer<typeof UpdateUserRequestSchema>
export type UserProfile = z.infer<typeof UserProfileSchema>
export type HTTPValidationError = z.infer<typeof HTTPValidationErrorSchema>
export type WsStatsResponse = z.infer<typeof WsStatsResponseSchema>
export type ZmqHealthResponse = z.infer<typeof ZmqHealthResponseSchema>
export type HealthCheckResponse = z.infer<typeof HealthCheckResponseSchema>
export type LoginResponse = z.infer<typeof LoginResponseSchema>
export type RefreshResponse = z.infer<typeof RefreshResponseSchema>
export type UserListResponse = z.infer<typeof UserListResponseSchema>
export type UserResponse = z.infer<typeof UserResponseSchema>
