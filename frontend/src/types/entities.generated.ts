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

type Exchange = 'kraken' | 'kraken_futures' | 'kraken_equities' | 'zonda' | 'walutomat' | 'polygon'
type Exchange2 = 'paper' | 'kraken' | 'kraken_futures' | 'zonda' | 'walutomat'
type TradeSide = 'buy' | 'sell'
type Mode = 'live' | 'paper'

/**
 * Canonical Candle entity.
 * From WebSocket CandleData.
 */
export interface Candle {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  instrument: string
  exchange: Exchange
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
 * Canonical Contract entity.
 * From WebSocket ContractData.
 */
export interface Contract {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  instrumentPublicId: string
  nativeSymbol: string
  exchange: string
  expiryAt: Date | null
  instrumentKind: string | null
  relationshipType: string
  contractFamily: string | null
  isFrontMonth: boolean
}

/**
 * Canonical Execution entity.
 * From WebSocket ExecutionData.
 */
export interface Execution {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  tradeId?: string | null
  exchangeOrderId?: string | null
  clientOrderId: string
  instrument: string
  exchange: Exchange2
  side: TradeSide
  size: number
  price: number
  lastSize: number
  lastPrice: number
  fee: number
  feeAsset: string
  status: 'filled' | 'partial'
  executedAt: Date
}

/**
 * Canonical FrontMonth entity.
 * From WebSocket FrontMonthData.
 */
export interface FrontMonth {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  instrumentPublicId: string
  nativeSymbol: string
  exchange: string
  expiryAt: Date
  relationshipType: string
  contractFamily: string | null
}

/**
 * Canonical Heartbeat entity.
 * From WebSocket HeartbeatData.
 */
export interface Heartbeat {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
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
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  exchange: Exchange2
  instrument: string
  exchangeOrderId: string
  clientOrderId: string
}

/**
 * Canonical Order entity.
 * From WebSocket OrderData.
 */
export interface Order {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  exchangeOrderId?: string | null
  clientOrderId: string
  instrument: string
  exchange: Exchange2
  mode?: Mode
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
  createdAt: Date
  updatedAt?: Date | null
}

/**
 * Canonical OrderEvent entity.
 * From WebSocket OrderEventData.
 */
export interface OrderEvent {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  exchangeOrderId: string
  clientOrderId: string
  exchange: Exchange2
  instrument: string
  event: 'submitted' | 'accepted' | 'rejected' | 'cancelled' | 'expired' | 'replaced'
  reason?: string | null
}

/**
 * Canonical OrderReplace entity.
 * From WebSocket OrderReplaceData.
 */
export interface OrderReplace {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  exchange: Exchange2
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
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  strategyId: string
  exchange: Exchange2
  instrument: string
  mode: Mode
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
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  instrument: string
  exchange: Exchange2
  mode?: Mode
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
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
}

/**
 * Canonical ReplayStart entity.
 * From WebSocket ReplayStartData.
 */
export interface ReplayStart {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  startedAt?: Date | null
}

/**
 * Canonical SettingChanged entity.
 * From WebSocket SettingChangedData.
 */
export interface SettingChanged {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
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
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  instrument: string
  exchange: Exchange2
  side: TradeSide
  strength: number
  reason: string
  price?: number | null
  strategyName?: string | null
  firedAt: Date
}

/**
 * Canonical SymbolAliasUpdate entity.
 * From WebSocket SymbolAliasUpdateData.
 */
export interface SymbolAliasUpdate {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  event: string
  action: string
}

/**
 * Canonical Tick entity.
 * From WebSocket TickData.
 */
export interface Tick {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  instrument: string
  exchange: Exchange
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
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  instrument: string
  exchange: Exchange
  executedAt?: Date | null
  price: number
  volume: number
  side?: string | null
  tradeId?: string | null
}

/**
 * Canonical UnderlyingAsset entity.
 * From WebSocket UnderlyingAssetData.
 */
export interface UnderlyingAsset {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  ticker: string
  name: string
  assetClass: string
  sector: string | null
  instrumentCount: number
}

/**
 * Canonical UnderlyingInstrument entity.
 * From WebSocket UnderlyingInstrumentData.
 */
export interface UnderlyingInstrument {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  instrumentPublicId: string
  nativeSymbol: string
  exchange: string
  assetType: string
  relationshipType: string
  contractFamily: string | null
}

/**
 * Canonical TopicMetric entity.
 * From REST API TopicMetricSnapshot.
 */
export interface TopicMetric {
  activeSubscribers?: number
  received?: number
  forwarded?: number
  throttled?: number
  dropped?: number
  timeout?: number
  errors?: number
  invalidMessages?: number
  lastMessageTs?: number
  throttleMs?: number | null
  pattern?: string | null
}


/**
 * Login request entity.
 * Use with loginToAPI() transform.
 */
export interface Login {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}

/**
 * CreateUser request entity.
 * Use with createUserToAPI() transform.
 */
export interface CreateUser {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}

/**
 * UpdateUser request entity.
 * Use with updateUserToAPI() transform.
 */
export interface UpdateUser {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}

/**
 * DeactivateUser request entity.
 * Use with deactivateUserToAPI() transform.
 */
export interface DeactivateUser {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}

/**
 * ChangePassword request entity.
 * Use with changePasswordToAPI() transform.
 */
export interface ChangePassword {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}

/**
 * AdminResetPassword request entity.
 * Use with adminResetPasswordToAPI() transform.
 */
export interface AdminResetPassword {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}

/**
 * RemoveSetting request entity.
 * Use with removeSettingToAPI() transform.
 */
export interface RemoveSetting {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}

/**
 * ProcessCreate request entity.
 * Use with processCreateToAPI() transform.
 */
export interface ProcessCreate {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}

/**
 * ProcessStart request entity.
 * Use with processStartToAPI() transform.
 */
export interface ProcessStart {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  payload: Record<string, unknown>
}
