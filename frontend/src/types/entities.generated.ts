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

type Exchange = 'kraken' | 'kraken_futures' | 'kraken_equities' | 'walutomat' | 'polygon'
type Exchange2 = 'paper' | 'kraken' | 'kraken_futures' | 'walutomat'
type TradeSide = 'buy' | 'sell'
type Mode = 'live' | 'paper'

/**
 * Canonical AiReviewCapsViolationFrame entity.
 * From WebSocket AiReviewCapsViolationFrameData.
 */
export interface AiReviewCapsViolationFrame {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  reviewPublicId: string
  userPublicId: string
  strategyPublicId: string
  walletPublicId: string
  instrumentPublicId: string
  capType: string
  attempted: number
  limit: number
  dispatchVersion: number
}

/**
 * Canonical AiReviewDecisionAckFrame entity.
 * From WebSocket AiReviewDecisionAckFrameData.
 */
export interface AiReviewDecisionAckFrame {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  reviewPublicId: string
  userPublicId: string
  strategyPublicId: string
  walletPublicId: string
  instrumentPublicId: string
  respondingDelegatePublicId: string
  decision: string
  newStatus: string
  resolutionMode: string
  rationale: string | null
  dispatchVersion: number
}

/**
 * Canonical AiReviewDecision entity.
 * From WebSocket AiReviewDecisionData.
 */
export interface AiReviewDecision {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  reviewPublicId: string
  respondingDelegatePublicId: string
  decision: string
  newStatus: string
  resolutionMode: string
  dispatchVersion: number
}

/**
 * Canonical AiReviewRequestFrame entity.
 * From WebSocket AiReviewRequestFrameData.
 */
export interface AiReviewRequestFrame {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  reviewPublicId: string
  userPublicId: string
  strategyPublicId: string
  walletPublicId: string
  instrumentPublicId: string
  selectedDelegatePublicId: string
  deadline: Date
  signalEnvelope: Record<string, unknown>
  instrumentMetadata: Record<string, unknown>
  dispatchVersion: number
}

/**
 * Canonical AlertEvent entity.
 * From WebSocket AlertEventData.
 */
export interface AlertEvent {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  userPublicId: string
  operatorPublicId?: string | null
  walletPublicId?: string | null
  alertType: 'order_fill_full' | 'order_rejected' | 'position_stop_loss_fired' | 'margin_warning' | 'critical_system_error'
  priority?: 'low' | 'medium' | 'high'
  isSafetyCritical?: boolean
  title: string
  body: string
  payload?: Record<string, unknown> | null
  dedupKey?: string | null
  threadKey?: string | null
  sourceTopic?: string | null
}

/**
 * Canonical BacktestProgress entity.
 * From WebSocket BacktestProgressData.
 */
export interface BacktestProgress {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  runPublicId: string
  walletPublicId: string
  event: 'started' | 'progress' | 'milestone' | 'completed' | 'failed' | 'cancelled'
  milestone?: '25pct' | '50pct' | '75pct' | null
  candlesDone: number
  totalCandles: number | null
  signalsCount: number
  tradesCount: number
  equity: number
  progressPct: number
}

/**
 * Canonical Candle entity.
 * From WebSocket CandleData.
 */
export interface Candle {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
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
 * Canonical CapsViolationAfterAiApprove entity.
 * From WebSocket CapsViolationAfterAiApproveData.
 */
export interface CapsViolationAfterAiApprove {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  reviewPublicId: string
  userPublicId: string
  strategyPublicId: string
  walletPublicId: string
  instrumentPublicId: string
  capType: string
  attempted: number
  limit: number
  dispatchVersion: number
}

/**
 * Canonical ContinuousCandle entity.
 * From WebSocket ContinuousCandleData.
 */
export interface ContinuousCandle {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  openAt: Date
  timeframe: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  vwap: number | null
  trades: number | null
  sourceContract: string
  adjustmentFactor: number | null
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
  topic?: string | null
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
 * Canonical DelegateOffline entity.
 * From WebSocket DelegateOfflineData.
 */
export interface DelegateOffline {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  userPublicId: string
  delegatePublicId: string
  lastSeenAt: Date
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
  topic?: string | null
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
  walletPublicId?: string
  operatorPublicId?: string | null
  userPublicId?: string | null
  liquidityRole?: string
}

/**
 * Canonical ExecutionPlan entity.
 * From WebSocket ExecutionPlanData.
 */
export interface ExecutionPlan {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  planType: string
  status: string
  instrumentPublicId: string
  exchange: string
  mode: string
  side: string
  totalQuantity: number
  filledQuantity: number
  createdAt: Date
  createdVia: string
  walletPublicId: string
  operatorPublicId: string | null
  params: Record<string, unknown>
  positionCyclePublicId: string | null
  parentPlanPublicId: string | null
  lastError: string | null
  idempotencyKey: string | null
}

/**
 * Canonical ExecutionPlanDecisionEvent entity.
 * From WebSocket ExecutionPlanDecisionEventData.
 */
export interface ExecutionPlanDecisionEvent {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  decisionPublicId: string
  planPublicId: string
  decisionType: string
  triggerType: string
  reason: string
  triggeredAt: Date
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
  topic?: string | null
  instrumentPublicId: string
  nativeSymbol: string
  exchange: string
  expiryAt: Date
  relationshipType: string
  contractFamily: string | null
}

/**
 * Canonical FundingAccrual entity.
 * From WebSocket FundingAccrualData.
 */
export interface FundingAccrual {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  instrument: string
  exchange: Exchange2
  mode: Mode
  accrualType: 'funding' | 'rollover' | 'borrow'
  accruedAt: Date
  amount: number
  amountAsset: string
  rate: number
  notional: number
  positionQuantity: number
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
  topic?: string | null
  component: string
  sequence: number
  status: 'healthy' | 'warning' | 'error'
  lagMs: number
  meta?: Record<string, unknown>
}

/**
 * Canonical InstrumentCapability entity.
 * From WebSocket InstrumentCapabilityData.
 */
export interface InstrumentCapability {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  instrumentPublicId: string
  exchange: string
  supportedOrderTypes: string[]
  supportsPostOnly: boolean
  supportsReduceOnly: boolean
  supportsAmendInPlace: boolean
  supportsNativeStopLoss: boolean
  supportsNativeTakeProfit: boolean
  supportsTrailingStopClientSide: boolean
  supportsMarketMaking: boolean
  supportsShortSelling: boolean
  supportsLeverage: boolean
  maxLeverageLong: number
  maxLeverageShort: number
  minNotional: number | null
  maxOrderSize: number | null
  topOfBookQuality: string
}

/**
 * Canonical InstrumentDetail entity.
 * From WebSocket InstrumentDetailData.
 */
export interface InstrumentDetail {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  instrumentPublicId: string
  symbolPublicId: string
  symbol: string
  exchange: string
  canTrade: boolean
  canMarketData: boolean
  instrumentResolved: boolean
  instrumentKind: string | null
  expiryAt: Date | null
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
  topic?: string | null
  exchange: Exchange2
  instrument: string
  exchangeOrderId: string
  clientOrderId: string
  walletPublicId?: string
  operatorPublicId?: string | null
  userPublicId?: string | null
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
  topic?: string | null
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
  leverage?: number | null
  reduceOnly?: boolean
  walletPublicId?: string
  operatorPublicId?: string | null
  userPublicId?: string | null
  planPublicId?: string | null
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
  topic?: string | null
  exchangeOrderId: string
  clientOrderId: string
  exchange: Exchange2
  instrument: string
  event: 'submitted' | 'accepted' | 'rejected' | 'cancelled' | 'expired' | 'replaced'
  reason?: string | null
  walletPublicId?: string
  operatorPublicId?: string | null
  userPublicId?: string | null
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
  topic?: string | null
  exchange: Exchange2
  instrument: string
  exchangeOrderId: string
  clientOrderId: string
  newQuantity?: number | null
  newPrice?: number | null
  walletPublicId?: string
  operatorPublicId?: string | null
  userPublicId?: string | null
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
  topic?: string | null
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
  strategyTag?: string | null
  leverage?: number | null
  reduceOnly?: boolean
  walletPublicId?: string
  operatorPublicId?: string | null
  userPublicId?: string | null
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
  topic?: string | null
  instrument: string
  instrumentPublicId?: string
  exchange: Exchange2
  mode?: Mode
  quantity: number
  averagePrice: number
  unrealizedPnl: number
  realizedPnl: number
  positionCyclePublicId?: string | null
  walletPublicId?: string
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
  topic?: string | null
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
  topic?: string | null
  startedAt?: Date | null
}

/**
 * Canonical ScopeRevoked entity.
 * From WebSocket ScopeRevokedData.
 */
export interface ScopeRevoked {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  grantPublicId: string
  operatorPublicId: string
  walletPublicId: string
  scopeKind: 'underlying' | 'instrument'
  underlyingPublicId?: string | null
  instrumentPublicId?: string | null
  revokedAt: Date
  revokedByUserPublicId?: string | null
  reason?: string | null
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
  topic?: string | null
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
  topic?: string | null
  instrument: string
  exchange: Exchange2
  side: TradeSide
  strength: number
  reason: string
  price?: number | null
  strategyName?: string | null
  firedAt: Date
  walletPublicId?: string
  operatorPublicId?: string | null
  userPublicId?: string | null
  aiReviewPublicId?: string | null
  aiReviewDispatchVersion?: number | null
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
  topic?: string | null
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
  topic?: string | null
  instrument: string
  exchange: Exchange
  volume: number
  bid?: number | null
  ask?: number | null
  last?: number | null
  isDelayed?: boolean
  isExtendedHours?: boolean | null
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
  topic?: string | null
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
  topic?: string | null
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
  topic?: string | null
  instrumentPublicId: string
  nativeSymbol: string
  exchange: string
  assetType: string
  relationshipType: string
  contractFamily: string | null
}

/**
 * Canonical UserDeactivated entity.
 * From WebSocket UserDeactivatedData.
 */
export interface UserDeactivated {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  userPublicId: string
  deactivatedAt: Date
  reason?: string | null
}

/**
 * Canonical VenueFeeSchedule entity.
 * From WebSocket VenueFeeScheduleData.
 */
export interface VenueFeeSchedule {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  exchange: string
  instrumentPublicId: string | null
  feeTier: string
  makerBps: number
  takerBps: number
  minVolume30d: number | null
  currency: string
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
  topic?: string | null
  payload: Record<string, unknown>
}

/**
 * RefreshToken request entity.
 * Use with refreshTokenToAPI() transform.
 */
export interface RefreshToken {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
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
  topic?: string | null
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
  topic?: string | null
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
  topic?: string | null
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
  topic?: string | null
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
  topic?: string | null
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
  topic?: string | null
  payload: Record<string, unknown>
}

/**
 * DelegateCreate request entity.
 * Use with delegateCreateToAPI() transform.
 */
export interface DelegateCreate {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  payload: Record<string, unknown>
}

/**
 * DelegateCapsUpdate request entity.
 * Use with delegateCapsUpdateToAPI() transform.
 */
export interface DelegateCapsUpdate {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  payload: Record<string, unknown>
}

/**
 * DelegateDeactivate request entity.
 * Use with delegateDeactivateToAPI() transform.
 */
export interface DelegateDeactivate {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
  payload: Record<string, unknown>
}

/**
 * AiReviewDecision request entity.
 * Use with aiReviewDecisionToAPI() transform.
 */
export interface AiReviewDecision {
  decision: string
  rationale?: string | null
}

/**
 * BacktestCompare request entity.
 * Use with backtestCompareToAPI() transform.
 */
export interface BacktestCompare {
  sequenceId: number
  publicId: string
  timestamp: Date
  sessionId: string
  topic?: string | null
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
  topic?: string | null
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
  topic?: string | null
  payload: Record<string, unknown>
}
