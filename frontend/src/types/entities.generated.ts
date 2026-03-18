/**
 * Generated entity types with Date objects instead of ISO strings.
 * DO NOT EDIT - regenerate with: make ui-gen-entities
 *
 * These are canonical entity types for use in the application.
 * They differ from raw API/WS types by using Date instead of string
 * for datetime fields and camelCase for field names.
 */

// Re-export common types from generated schemas
export type {
  Side1 as TradeSide,
  OrderType,
  Status2 as HeartbeatStatus,
} from './ws.generated'

type MarketDataExchange = 'kraken' | 'zonda' | 'walutomat' | 'polygon'
type OrderExchange = 'paper' | 'kraken' | 'zonda' | 'walutomat'
type TradeSide = 'buy' | 'sell'

/**
 * Canonical Candle entity.
 * From WebSocket CandleData.
 */
export interface Candle {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  instrument: string
  exchange: MarketDataExchange
  timeframe: string
  openAt: Date
  open: number
  high: number
  low: number
  close: number
  volume: number
  vwap?: number | null
  trades?: number | null
}

/**
 * Canonical Execution entity.
 * From WebSocket ExecutionData.
 */
export interface Execution {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  tradeId?: string | null
  exchangeOrderId?: string | null
  clientOrderId: string
  instrument: string
  exchange: OrderExchange
  side: TradeSide
  size: number
  price: number
  fee: number
  feeAsset: string
  status: 'filled' | 'partial'
  executedAt?: Date
}

/**
 * Canonical Heartbeat entity.
 * From WebSocket HeartbeatData.
 */
export interface Heartbeat {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  component: string
  sequence: number
  status: 'healthy' | 'warning' | 'error'
  lagMs: number
  meta?: Record<string, unknown>
}

/**
 * Canonical OrderCancel entity.
 * From WebSocket OrderCancelData.
 */
export interface OrderCancel {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  exchange: OrderExchange
  instrument: string
  exchangeOrderId: string
  clientOrderId: string
}

/**
 * Canonical Order entity.
 * From WebSocket OrderData.
 */
export interface Order {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  exchangeOrderId?: string | null
  clientOrderId: string
  instrument: string
  exchange: OrderExchange
  side: TradeSide
  status: string
  orderType: 'market' | 'limit' | 'stop' | 'stop_limit'
  size: number
  filledSize: number
  price?: number | null
  averagePrice?: number | null
  reason?: string | null
  timeInForce?: string | null
  error?: string | null
  createdAt?: Date
  updatedAt?: Date | null
}

/**
 * Canonical OrderEvent entity.
 * From WebSocket OrderEventData.
 */
export interface OrderEvent {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  exchangeOrderId: string
  clientOrderId: string
  exchange: OrderExchange
  instrument: string
  event: 'submitted' | 'accepted' | 'rejected' | 'cancelled' | 'expired' | 'replaced'
  reason?: string | null
}

/**
 * Canonical OrderReplace entity.
 * From WebSocket OrderReplaceData.
 */
export interface OrderReplace {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  exchange: OrderExchange
  instrument: string
  exchangeOrderId: string
  clientOrderId: string
  newQuantity?: number | null
  newPrice?: number | null
}

/**
 * Canonical OrderRequest entity.
 * From WebSocket OrderRequestData.
 */
export interface OrderRequest {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  strategyId: string
  exchange: OrderExchange
  instrument: string
  mode: 'live' | 'paper'
  side: TradeSide
  orderType: 'market' | 'limit' | 'stop' | 'stop_limit'
  quantity: number
  price?: number | null
  clientOrderId: string
  signaledAt?: Date | null
}

/**
 * Canonical Position entity.
 * From WebSocket PositionData.
 */
export interface Position {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  instrument: string
  exchange: OrderExchange
  quantity: number
  averagePrice: number
  unrealizedPnl: number
  realizedPnl: number
}

/**
 * Canonical ReplayEnd entity.
 * From WebSocket ReplayEndData.
 */
export interface ReplayEnd {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
}

/**
 * Canonical ReplayStart entity.
 * From WebSocket ReplayStartData.
 */
export interface ReplayStart {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  startedAt?: Date | null
}

/**
 * Canonical SettingChanged entity.
 * From WebSocket SettingChangedData.
 */
export interface SettingChanged {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  key: string
  value: string
  category: string
  updatedBy?: string | null
}

/**
 * Canonical Signal entity.
 * From WebSocket SignalData.
 */
export interface Signal {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  instrument: string
  exchange: OrderExchange
  side: TradeSide
  strength: number
  reason: string
  price?: number | null
  strategyName?: string | null
  firedAt?: Date
}

/**
 * Canonical SymbolAliasUpdate entity.
 * From WebSocket SymbolAliasUpdateData.
 */
export interface SymbolAliasUpdate {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  event: string
  action: string
}

/**
 * Canonical Tick entity.
 * From WebSocket TickData.
 */
export interface Tick {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  instrument: string
  exchange: MarketDataExchange
  volume: number
  bid?: number | null
  ask?: number | null
  last?: number | null
}

/**
 * Canonical Trade entity.
 * From WebSocket TradeData.
 */
export interface Trade {
  publicId?: string
  timestamp?: Date
  sessionId?: string
  sequenceId?: number
  instrument: string
  exchange: MarketDataExchange
  executedAt?: Date | null
  price: number
  volume: number
  side?: string | null
}


/**
 * AdminResetPassword request entity.
 * Use with adminResetPasswordToAPI() transform.
 */
export interface AdminResetPassword {
  newPassword: string
}

/**
 * ChangePassword request entity.
 * Use with changePasswordToAPI() transform.
 */
export interface ChangePassword {
  currentPassword: string
  newPassword: string
}

/**
 * CreateUser request entity.
 * Use with createUserToAPI() transform.
 */
export interface CreateUser {
  username: string
  email?: string | null
  password: string
  role: 'viewer' | 'operator' | 'admin'
  isActive?: boolean
}

/**
 * Login request entity.
 * Use with loginToAPI() transform.
 */
export interface Login {
  username: string
  password: string
  rememberMe?: boolean
}

/**
 * ProcessCreate request entity.
 * Use with processCreateToAPI() transform.
 */
export interface ProcessCreate {
  name: string
  template: string
  enabled?: boolean | null
  mode?: 'thread' | 'process' | null
  args?: unknown[] | null
  kwargs?: Record<string, unknown> | null
  note?: string | null
}

/**
 * ProcessStart request entity.
 * Use with processStartToAPI() transform.
 */
export interface ProcessStart {
  mode?: 'thread' | 'process' | null
  args?: unknown[] | null
  kwargs?: Record<string, unknown> | null
  autostart?: boolean | null
}

/**
 * UpdateUser request entity.
 * Use with updateUserToAPI() transform.
 */
export interface UpdateUser {
  email?: string | null
  role?: 'viewer' | 'operator' | 'admin' | null
  isActive?: boolean | null
}
