import type {
  OrderStatus as OrderStatusApi,
  ExecutionRecord,
  TradingSignal,
  PositionSnapshot,
  CandleSnapshot,
} from '../types/api'
import type {
  OrderStatusEnvelope,
  FillEnvelope,
  SignalEnvelope,
  BarEnvelope,
  TickEnvelope,
  HeartbeatEnvelope,
} from '../types/ws'
import type {
  OrderStatus,
  Fill,
  Signal,
  Position,
  Candle,
  Bar,
  Tick,
  Heartbeat,
  OrderType,
  TradeSide,
} from '../types/entities'

function normalizeSide(side: string): TradeSide {
  const normalized = side.toLowerCase()

  if (normalized === 'buy' || normalized === 'sell') {
    return normalized
  }

  throw new Error(`Invalid trade side: "${side}". Expected "buy" or "sell".`)
}

export function orderFromAPI(api: OrderStatusApi): OrderStatus {
  return {
    id: api.id,
    instrument: api.instrument,
    exchange: api.exchange,
    side: normalizeSide(api.side),
    orderType: normalizeOrderType(api.type),
    size: api.size,
    filledSize: 0,
    price: api.price ?? null,
    averagePrice: null,
    status: api.status,
    createdAt: new Date(api.created_at),
    updatedAt: api.updated_at ? new Date(api.updated_at) : null,
  }
}

export function orderFromWS(ws: OrderStatusEnvelope): OrderStatus {
  if (!ws.created_at) {
    throw new Error('OrderStatusEnvelope missing required field: created_at')
  }

  return {
    id: ws.id,
    instrument: ws.instrument,
    exchange: ws.exchange,
    side: ws.side,
    orderType: ws.order_type,
    size: ws.size,
    filledSize: ws.filled_size,
    price: ws.price ?? null,
    averagePrice: ws.average_price ?? null,
    status: ws.status,
    createdAt: new Date(ws.created_at),
    updatedAt: ws.updated_at ? new Date(ws.updated_at) : null,
  }
}

function normalizeOrderType(type: string): OrderType {
  const normalized = type.toLowerCase()

  if (
    normalized === 'market' ||
    normalized === 'limit' ||
    normalized === 'stop' ||
    normalized === 'stop_limit'
  ) {
    return normalized
  }

  throw new Error(
    `Invalid order type: "${type}". Expected "market", "limit", "stop", or "stop_limit".`
  )
}

export function executionFromAPI(api: ExecutionRecord): Fill {
  return {
    id: api.id,
    orderId: api.order_id,
    exchange: api.exchange,
    instrument: api.instrument,
    side: normalizeSide(api.side),
    size: api.size,
    price: api.price,
    fee: api.fee,
    feeAsset: api.fee_asset,
    status: 'filled',
    executedAt: new Date(api.timestamp),
  }
}

export function executionFromWS(ws: FillEnvelope): Fill {
  if (!ws.executed_at) {
    throw new Error('FillEnvelope missing required field: executed_at')
  }

  return {
    id: ws.id,
    orderId: ws.order_id,
    exchange: ws.exchange,
    instrument: ws.instrument,
    side: ws.side,
    size: ws.size,
    price: ws.price,
    fee: ws.fee,
    feeAsset: ws.fee_asset,
    status: ws.status,
    executedAt: new Date(ws.executed_at),
  }
}

export function signalFromAPI(api: TradingSignal): Signal {
  return {
    id: api.id,
    exchange: api.exchange,
    instrument: api.instrument,
    side: normalizeSide(api.side),
    strength: api.strength,
    reason: api.reason,
    strategyName: api.strategy_name ?? null,
    price: api.price ?? null,
    timestamp: new Date(api.timestamp),
  }
}

export function signalFromWS(ws: SignalEnvelope): Signal {
  if (!ws.timestamp) {
    throw new Error('SignalEnvelope missing required field: timestamp')
  }

  return {
    id: ws.id ?? null,
    exchange: ws.exchange,
    instrument: ws.instrument,
    side: ws.side,
    strength: ws.strength,
    reason: ws.reason,
    strategyName: ws.strategy_name ?? null,
    price: ws.price ?? null,
    timestamp: new Date(ws.timestamp),
  }
}

export function positionFromAPI(api: PositionSnapshot): Position {
  return {
    id: api.id,
    instrument: api.instrument,
    exchange: api.exchange,
    quantity: api.quantity,
    averagePrice: api.average_price,
    unrealizedPnl: api.unrealized_pnl,
    realizedPnl: api.realized_pnl,
    updatedAt: new Date(api.updated_at),
  }
}

export function candleFromAPI(api: CandleSnapshot): Candle {
  return {
    instrument: api.instrument,
    exchange: api.exchange,
    timeframe: api.timeframe,
    open: api.open,
    high: api.high,
    low: api.low,
    close: api.close,
    volume: api.volume,
    vwap: api.vwap ?? undefined,
    trades: api.trades ?? undefined,
    timestamp: new Date(api.timestamp),
  }
}

export function barFromWS(ws: BarEnvelope): Bar {
  if (!ws.timestamp) {
    throw new Error('BarEnvelope missing required field: timestamp')
  }

  if (ws.timeframe === null || ws.timeframe === undefined) {
    throw new Error('BarEnvelope missing required field: timeframe')
  }

  if (ws.open === null || ws.open === undefined) {
    throw new Error('BarEnvelope missing required field: open')
  }

  if (ws.high === null || ws.high === undefined) {
    throw new Error('BarEnvelope missing required field: high')
  }

  if (ws.low === null || ws.low === undefined) {
    throw new Error('BarEnvelope missing required field: low')
  }

  if (ws.close === null || ws.close === undefined) {
    throw new Error('BarEnvelope missing required field: close')
  }

  return {
    instrument: ws.instrument,
    timeframe: ws.timeframe,
    open: ws.open,
    high: ws.high,
    low: ws.low,
    close: ws.close,
    volume: ws.volume,
    vwap: ws.vwap ?? undefined,
    trades: ws.trades ?? undefined,
    timestamp: new Date(ws.timestamp),
    exchange: ws.exchange,
  }
}

export function tickFromWS(ws: TickEnvelope): Tick {
  if (!ws.timestamp) {
    throw new Error('TickEnvelope missing required field: timestamp')
  }

  return {
    instrument: ws.instrument,
    bid: ws.bid ?? null,
    ask: ws.ask ?? null,
    last: ws.last ?? undefined,
    volume: ws.volume,
    timestamp: new Date(ws.timestamp),
    exchange: ws.exchange,
  }
}

export function heartbeatFromWS(ws: HeartbeatEnvelope): Heartbeat {
  if (!ws.timestamp) {
    throw new Error('HeartbeatEnvelope missing required field: timestamp')
  }

  return {
    component: ws.component,
    status: ws.status,
    sequence: ws.sequence,
    timestamp: new Date(ws.timestamp),
    lagMs: ws.lag_ms,
  }
}

export function ordersFromAPI(apis: OrderStatusApi[]): OrderStatus[] {
  return apis.map(orderFromAPI)
}

export function executionsFromAPI(apis: ExecutionRecord[]): Fill[] {
  return apis.map(executionFromAPI)
}

export function signalsFromAPI(apis: TradingSignal[]): Signal[] {
  return apis.map(signalFromAPI)
}

export function positionsFromAPI(apis: PositionSnapshot[]): Position[] {
  return apis.map(positionFromAPI)
}

export function candlesFromAPI(apis: CandleSnapshot[]): Candle[] {
  return apis.map(candleFromAPI)
}

export function isTradeSide(value: unknown): value is TradeSide {
  return value === 'buy' || value === 'sell'
}

export function isOrderStatus(value: unknown): value is OrderStatus {
  return (
    value === 'new' ||
    value === 'submitted' ||
    value === 'open' ||
    value === 'filled' ||
    value === 'partially_filled' ||
    value === 'cancelled' ||
    value === 'rejected'
  )
}

export function isOrderType(value: unknown): value is OrderType {
  return value === 'market' || value === 'limit' || value === 'stop' || value === 'stop_limit'
}

export function safeOrderFromAPI(api: OrderStatusApi): OrderStatus | null {
  try {
    return orderFromAPI(api)
  } catch (error) {
    console.error('Failed to transform order from API:', error, api)

    return null
  }
}

export function safeExecutionFromAPI(api: ExecutionRecord): Fill | null {
  try {
    return executionFromAPI(api)
  } catch (error) {
    console.error('Failed to transform execution from API:', error, api)

    return null
  }
}

export function safeSignalFromAPI(api: TradingSignal): Signal | null {
  try {
    return signalFromAPI(api)
  } catch (error) {
    console.error('Failed to transform signal from API:', error, api)

    return null
  }
}
