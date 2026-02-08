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

type Side = 'buy' | 'sell'
type Exchange = 'paper' | 'kraken' | 'zonda' | 'walutomat'

/**
 * Canonical Bar entity.
 * From WebSocket BarEnvelope.
 */
export interface Bar {
  timestamp?: Date
  instrument: string
  timeframe: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  vwap?: number | null
  trades?: number | null
  exchange: string
}

/**
 * Canonical Fill entity.
 * From WebSocket FillEnvelope.
 */
export interface Fill {
  timestamp?: Date
  tradeId?: string | null
  exchangeOrderId?: string | null
  clientOrderId: string
  instrument: string
  exchange: string
  side: Side
  size: number
  price: number
  fee: number
  feeAsset: string
  status: 'filled' | 'partial'
  executedAt?: Date
}

/**
 * Canonical Heartbeat entity.
 * From WebSocket HeartbeatEnvelope.
 */
export interface Heartbeat {
  timestamp?: Date
  component: string
  sequence: number
  status: 'healthy' | 'unhealthy' | 'warning' | 'error'
  lagMs: number
}

/**
 * Canonical OrderCancel entity.
 * From WebSocket OrderCancelEnvelope.
 */
export interface OrderCancel {
  timestamp?: Date
  exchange: Exchange
  instrument: string
  exchangeOrderId: string
  clientOrderId: string
}

/**
 * Canonical OrderEvent entity.
 * From WebSocket OrderEventEnvelope.
 */
export interface OrderEvent {
  timestamp?: Date
  exchangeOrderId: string
  clientOrderId: string
  exchange: Exchange
  instrument: string
  event: 'submitted' | 'accepted' | 'rejected' | 'cancelled' | 'expired' | 'replaced'
  reason?: string | null
}

/**
 * Canonical OrderReplace entity.
 * From WebSocket OrderReplaceEnvelope.
 */
export interface OrderReplace {
  timestamp?: Date
  exchange: Exchange
  instrument: string
  exchangeOrderId: string
  clientOrderId: string
  newQuantity?: number | null
  newPrice?: number | null
}

/**
 * Canonical OrderRequest entity.
 * From WebSocket OrderRequestEnvelope.
 */
export interface OrderRequest {
  timestamp?: Date
  strategyId: string
  exchange: Exchange
  instrument: string
  mode: 'live' | 'paper'
  side: Side
  orderType: 'market' | 'limit' | 'stop' | 'stop_limit'
  quantity: number
  price?: number | null
  clientOrderId: string
  signaledAt?: Date | null
}

/**
 * Canonical OrderStatus entity.
 * From WebSocket OrderStatusEnvelope.
 */
export interface OrderStatus {
  timestamp?: Date
  exchangeOrderId?: string | null
  clientOrderId: string
  instrument: string
  exchange: string
  side: Side
  status: 'submitted' | 'accepted' | 'rejected' | 'cancelled' | 'expired' | 'replaced'
  orderType: 'market' | 'limit' | 'stop' | 'stop_limit'
  size: number
  filledSize: number
  price?: number | null
  averagePrice?: number | null
  reason?: string | null
  createdAt?: Date
  updatedAt?: Date | null
}

/**
 * Canonical ReplayEnd entity.
 * From WebSocket ReplayEndEnvelope.
 */
export interface ReplayEnd {
  timestamp?: Date
}

/**
 * Canonical ReplayStart entity.
 * From WebSocket ReplayStartEnvelope.
 */
export interface ReplayStart {
  timestamp?: Date
  startedAt?: Date | null
}

/**
 * Canonical SettingChanged entity.
 * From WebSocket SettingChangedEnvelope.
 */
export interface SettingChanged {
  timestamp?: Date
  key: string
  value: string
  category: string
  updatedBy?: string | null
}

/**
 * Canonical Signal entity.
 * From WebSocket SignalEnvelope.
 */
export interface Signal {
  timestamp?: Date
  instrument: string
  side: Side
  strength: number
  reason: string
  price?: number | null
  strategyName?: string | null
  id?: string | number | null
  exchange: string
}

/**
 * Canonical SymbolAliasUpdate entity.
 * From WebSocket SymbolAliasUpdateEnvelope.
 */
export interface SymbolAliasUpdate {
  timestamp?: Date
  event: string
  action: string
}

/**
 * Canonical Tick entity.
 * From WebSocket TickEnvelope.
 */
export interface Tick {
  timestamp?: Date
  instrument: string
  volume: number
  bid?: number | null
  ask?: number | null
  last?: number | null
  exchange: string
}

/**
 * Canonical Trade entity.
 * From WebSocket TradeEnvelope.
 */
export interface Trade {
  timestamp?: Date
  instrument: string
  price: number
  volume: number
  side?: string | null
  exchange: string
}

/**
 * Canonical Candle entity.
 * From REST API CandleSnapshot.
 */
export interface Candle {
  instrument: string
  timeframe: string
  timestamp: Date
  open: number
  high: number
  low: number
  close: number
  volume: number
  vwap?: number | null
  trades?: number | null
}

/**
 * Canonical Position entity.
 * From REST API PositionSnapshot.
 */
export interface Position {
  id: string | number
  instrument: string
  exchange: string
  quantity: number
  averagePrice: number
  unrealizedPnl: number
  realizedPnl: number
  updatedAt: Date
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
