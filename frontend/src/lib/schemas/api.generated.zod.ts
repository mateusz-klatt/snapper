/**
 * Generated Zod schemas for REST API validation.
 * DO NOT EDIT - regenerate with: make ui-gen-api-zod
 */

import { z } from 'zod'

export const AdminResetPasswordRequestSchema = z
  .object({
    new_password: z.string().min(8),
  })
  .strict()

export const AvailableProcessSchema = z
  .object({
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

export const CandleSnapshotSchema = z
  .object({
    instrument: z.string(),
    timeframe: z.string(),
    timestamp: z.iso.datetime(),
    open: z.number(),
    high: z.number(),
    low: z.number(),
    close: z.number(),
    volume: z.number(),
    vwap: z.number().nullable().optional(),
    trades: z.number().int().nullable().optional(),
  })
  .strict()

export const ChangePasswordRequestSchema = z
  .object({
    current_password: z.string(),
    new_password: z.string().min(8),
  })
  .strict()

export const ConfiguredProcessSchema = z
  .object({
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
    active_run_id: z.string().nullable().optional(),
  })
  .strict()

export const ExecutionRecordSchema = z
  .object({
    id: z.number().int(),
    order_id: z.number().int(),
    timestamp: z.iso.datetime(),
    price: z.number(),
    size: z.number(),
    fee: z.number(),
    fee_asset: z.string(),
    instrument: z.string(),
    side: z.enum(['buy', 'sell']),
    exchange: z.string(),
  })
  .strict()

export const HealthTopicsSchema = z
  .object({
    available: z.number().int(),
    active: z.number().int(),
  })
  .strict()

export const LoginRequestSchema = z
  .object({
    username: z.string(),
    password: z.string(),
    remember_me: z.boolean(),
  })
  .strict()

export const MessageResponseSchema = z
  .object({
    message: z.string(),
  })
  .strict()

export const OrderStatusSchema = z
  .object({
    id: z.number().int(),
    instrument: z.string(),
    exchange: z.string(),
    client_order_id: z.string().nullable().optional(),
    exchange_order_id: z.string().nullable().optional(),
    created_at: z.iso.datetime(),
    updated_at: z.iso.datetime().nullable().optional(),
    side: z.enum(['buy', 'sell']),
    type: z.enum(['market', 'limit', 'stop', 'stop_limit']),
    price: z.number().nullable().optional(),
    size: z.number(),
    status: z.enum([
      'new',
      'submitted',
      'open',
      'filled',
      'partially_filled',
      'cancelled',
      'rejected',
    ]),
    time_in_force: z.string().nullable().optional(),
    error: z.string().nullable().optional(),
  })
  .strict()

export const PositionSnapshotSchema = z
  .object({
    id: z.number().int(),
    instrument: z.string(),
    exchange: z.string(),
    quantity: z.number(),
    average_price: z.number(),
    unrealized_pnl: z.number(),
    realized_pnl: z.number(),
    updated_at: z.iso.datetime(),
  })
  .strict()

export const ProcessCreateRequestSchema = z
  .object({
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
    name: z.string(),
    template: z.string(),
  })
  .strict()

export const ProcessRunSchema = z
  .object({
    run_id: z.string(),
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
    mode: z.enum(['thread', 'process']).nullable().optional(),
    args: z.array(z.unknown()).nullable().optional(),
    kwargs: z.record(z.string(), z.unknown()).nullable().optional(),
    autostart: z.boolean().nullable().optional(),
  })
  .strict()

export const ProcessStartResponseSchema = z
  .object({
    status: z.enum(['success', 'already_running', 'error']),
    name: z.string(),
    run_id: z.string().nullable().optional(),
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

export const ProcessStopResponseSchema = z
  .object({
    status: z.enum(['success', 'not_running', 'error']),
    name: z.string(),
    message: z.string().nullable().optional(),
  })
  .strict()

export const SettingCategoriesResponseSchema = z
  .object({
    categories: z.array(z.string()),
  })
  .strict()

export const SettingReadSchema = z
  .object({
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
    value: z.string(),
    category: z.string(),
    description: z.string().nullable().optional(),
  })
  .strict()

export const SubscriptionsStatsSchema = z
  .object({
    per_topic: z.record(z.string(), z.number().int()),
    per_client: z.record(z.string(), z.array(z.string())),
  })
  .strict()

export const TradingSignalSchema = z
  .object({
    id: z.number().int(),
    instrument: z.string(),
    exchange: z.string(),
    timestamp: z.iso.datetime(),
    side: z.enum(['buy', 'sell']),
    strength: z.number(),
    reason: z.string(),
    strategy_name: z.string().nullable().optional(),
    price: z.number().nullable().optional(),
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

export const AvailableProcessesResponseSchema = z
  .object({
    processes: z.array(AvailableProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const ConfiguredProcessesResponseSchema = z
  .object({
    processes: z.array(ConfiguredProcessSchema),
    count: z.number().int(),
  })
  .strict()

export const HealthCheckResponseSchema = z
  .object({
    status: z.enum(['healthy', 'unhealthy', 'warning', 'error']),
    timestamp: z.iso.datetime(),
    version: z.string(),
    connections: z.record(z.string(), z.unknown()),
    topics: HealthTopicsSchema,
  })
  .strict()

export const ProcessCreateResponseSchema = z
  .object({
    status: z.literal('created'),
    process: ProcessCreatedInfoSchema,
  })
  .strict()

export const ProcessRunsResponseSchema = z
  .object({
    runs: z.array(ProcessRunSchema),
    count: z.number().int(),
  })
  .strict()

export const SystemStatusSchema = z
  .object({
    trader: ProcessStatusSchema,
    backtests: z.record(z.string(), ProcessStatusSchema),
    strategies: z.array(z.record(z.string(), z.unknown())).optional(),
  })
  .strict()

export const CreateUserRequestSchema = z
  .object({
    username: z.string().min(3).max(64),
    email: z.string().max(255).nullable().optional(),
    password: z.string().min(8),
    role: UserRoleSchema,
    is_active: z.boolean(),
  })
  .strict()

export const UpdateUserRequestSchema = z
  .object({
    email: z.string().max(255).nullable().optional(),
    role: UserRoleSchema.nullable().optional(),
    is_active: z.boolean().nullable().optional(),
  })
  .strict()

export const UserProfileSchema = z
  .object({
    id: z.string(),
    username: z.string(),
    email: z.string().nullable().optional(),
    role: UserRoleSchema,
    is_active: z.boolean(),
    created_at: z.iso.datetime().optional(),
    last_login: z.iso.datetime().nullable().optional(),
  })
  .strict()

export const HTTPValidationErrorSchema = z
  .object({
    detail: z.array(ValidationErrorSchema).optional(),
  })
  .strict()

export const WsStatsResponseSchema = z
  .object({
    websocket: WebSocketStatsSchema,
    zmq_bridge: ZmqBridgeStatsSchema,
    connections: z.record(z.string(), z.unknown()),
    topics: z.record(z.string(), z.unknown()),
    subscriptions: SubscriptionsStatsSchema,
    config: WsStatsConfigSchema,
  })
  .strict()

export const ZmqHealthResponseSchema = z
  .object({
    status: z.enum(['healthy', 'unhealthy', 'warning', 'error']),
    timestamp: z.iso.datetime(),
    components: ZmqComponentsSchema,
    config: ZmqConfigSchema,
    connections: z.record(z.string(), z.unknown()),
    message_stats: z.record(z.string(), z.unknown()),
    errors: z.array(z.string()).optional(),
  })
  .strict()

export const LoginResponseSchema = z
  .object({
    message: z.string(),
    expires_in: z.number().int(),
    user: UserProfileSchema,
  })
  .strict()

export const RefreshResponseSchema = z
  .object({
    message: z.string(),
    ws_token: z.string(),
    ws_token_exp: z.iso.datetime(),
    csrf_token: z.string(),
    user: UserProfileSchema,
  })
  .strict()

export const UserListResponseSchema = z
  .object({
    users: z.array(UserProfileSchema),
    total_count: z.number().int(),
  })
  .strict()

// Type exports
export type AdminResetPasswordRequest = z.infer<typeof AdminResetPasswordRequestSchema>
export type AvailableProcess = z.infer<typeof AvailableProcessSchema>
export type CandleSnapshot = z.infer<typeof CandleSnapshotSchema>
export type ChangePasswordRequest = z.infer<typeof ChangePasswordRequestSchema>
export type ConfiguredProcess = z.infer<typeof ConfiguredProcessSchema>
export type ExecutionRecord = z.infer<typeof ExecutionRecordSchema>
export type HealthTopics = z.infer<typeof HealthTopicsSchema>
export type LoginRequest = z.infer<typeof LoginRequestSchema>
export type MessageResponse = z.infer<typeof MessageResponseSchema>
export type OrderStatus = z.infer<typeof OrderStatusSchema>
export type PositionSnapshot = z.infer<typeof PositionSnapshotSchema>
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
export type SubscriptionsStats = z.infer<typeof SubscriptionsStatsSchema>
export type TradingSignal = z.infer<typeof TradingSignalSchema>
export type UserRole = z.infer<typeof UserRoleSchema>
export type ValidationError = z.infer<typeof ValidationErrorSchema>
export type WebSocketStats = z.infer<typeof WebSocketStatsSchema>
export type WsStatsConfig = z.infer<typeof WsStatsConfigSchema>
export type ZmqBridgeStats = z.infer<typeof ZmqBridgeStatsSchema>
export type ZmqComponents = z.infer<typeof ZmqComponentsSchema>
export type ZmqConfig = z.infer<typeof ZmqConfigSchema>
export type AvailableProcessesResponse = z.infer<typeof AvailableProcessesResponseSchema>
export type ConfiguredProcessesResponse = z.infer<typeof ConfiguredProcessesResponseSchema>
export type HealthCheckResponse = z.infer<typeof HealthCheckResponseSchema>
export type ProcessCreateResponse = z.infer<typeof ProcessCreateResponseSchema>
export type ProcessRunsResponse = z.infer<typeof ProcessRunsResponseSchema>
export type SystemStatus = z.infer<typeof SystemStatusSchema>
export type CreateUserRequest = z.infer<typeof CreateUserRequestSchema>
export type UpdateUserRequest = z.infer<typeof UpdateUserRequestSchema>
export type UserProfile = z.infer<typeof UserProfileSchema>
export type HTTPValidationError = z.infer<typeof HTTPValidationErrorSchema>
export type WsStatsResponse = z.infer<typeof WsStatsResponseSchema>
export type ZmqHealthResponse = z.infer<typeof ZmqHealthResponseSchema>
export type LoginResponse = z.infer<typeof LoginResponseSchema>
export type RefreshResponse = z.infer<typeof RefreshResponseSchema>
export type UserListResponse = z.infer<typeof UserListResponseSchema>
