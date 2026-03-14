import type { PositionData } from '../types/api'
import type {
  OrderData,
  ExecutionData,
  SignalData,
  CandleData,
  TickData,
  HeartbeatData,
} from '../types/ws'
import type {
  Order,
  Execution,
  Signal,
  Position,
  Candle,
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

export function orderFromAPI(api: OrderData): Order {
  return {
    clientOrderId: api.client_order_id,
    exchangeOrderId: api.exchange_order_id ?? null,
    instrument: api.instrument,
    exchange: api.exchange,
    side: normalizeSide(api.side),
    orderType: normalizeOrderType(api.order_type),
    size: api.size,
    filledSize: api.filled_size,
    price: api.price ?? null,
    averagePrice: api.average_price ?? null,
    status: api.status,
    reason: api.reason ?? null,
    timeInForce: api.time_in_force ?? null,
    error: api.error ?? null,
    createdAt: api.created_at ? new Date(api.created_at) : new Date(),
    updatedAt: api.updated_at ? new Date(api.updated_at) : null,
  }
}

export function orderFromWS(ws: OrderData): Order {
  if (!ws.created_at) {
    throw new Error('OrderData missing required field: created_at')
  }

  return {
    clientOrderId: ws.client_order_id,
    exchangeOrderId: ws.exchange_order_id ?? null,
    instrument: ws.instrument,
    exchange: ws.exchange,
    side: ws.side,
    orderType: ws.order_type,
    size: ws.size,
    filledSize: ws.filled_size,
    price: ws.price ?? null,
    averagePrice: ws.average_price ?? null,
    status: ws.status,
    reason: ws.reason ?? null,
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

export function executionFromAPI(api: ExecutionData): Execution {
  return {
    clientOrderId: api.client_order_id,
    tradeId: api.trade_id ?? null,
    exchangeOrderId: api.exchange_order_id ?? null,
    exchange: api.exchange,
    instrument: api.instrument,
    side: normalizeSide(api.side),
    size: api.size,
    price: api.price,
    fee: api.fee,
    feeAsset: api.fee_asset,
    status: api.status,
    executedAt: api.executed_at ? new Date(api.executed_at) : new Date(),
  }
}

export function executionFromWS(ws: ExecutionData): Execution {
  if (!ws.executed_at) {
    throw new Error('ExecutionData missing required field: executed_at')
  }

  return {
    clientOrderId: ws.client_order_id,
    tradeId: ws.trade_id ?? null,
    exchangeOrderId: ws.exchange_order_id ?? null,
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

export function signalFromAPI(api: SignalData): Signal {
  return {
    exchange: api.exchange,
    instrument: api.instrument,
    side: normalizeSide(api.side),
    strength: api.strength,
    reason: api.reason,
    strategyName: api.strategy_name ?? null,
    price: api.price ?? null,
    firedAt: api.fired_at ? new Date(api.fired_at) : new Date(),
  }
}

export function signalFromWS(ws: SignalData): Signal {
  if (!ws.fired_at && !ws.timestamp) {
    throw new Error('SignalData missing required field: timestamp')
  }

  const firedAtSource = ws.fired_at ?? ws.timestamp
  const firedAt = new Date(firedAtSource as string)
  const timestamp = ws.timestamp ? new Date(ws.timestamp) : undefined

  return {
    exchange: ws.exchange,
    instrument: ws.instrument,
    side: ws.side,
    strength: ws.strength,
    reason: ws.reason,
    strategyName: ws.strategy_name ?? null,
    price: ws.price ?? null,
    firedAt,
    timestamp,
  }
}

export function positionFromAPI(api: PositionData): Position {
  return {
    publicId: api.instrument,
    instrument: api.instrument,
    exchange: api.exchange,
    quantity: api.quantity,
    averagePrice: api.average_price,
    unrealizedPnl: api.unrealized_pnl,
    realizedPnl: api.realized_pnl,
    updatedAt: api.updated_at ? new Date(api.updated_at) : new Date(),
  }
}

export function candleFromAPI(api: CandleData): Candle {
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
    openAt: new Date(api.open_at),
  }
}

export function candleFromWS(ws: CandleData): Candle {
  if (ws.timeframe === null || ws.timeframe === undefined) {
    throw new Error('CandleData missing required field: timeframe')
  }

  if (ws.open === null || ws.open === undefined) {
    throw new Error('CandleData missing required field: open')
  }

  if (ws.high === null || ws.high === undefined) {
    throw new Error('CandleData missing required field: high')
  }

  if (ws.low === null || ws.low === undefined) {
    throw new Error('CandleData missing required field: low')
  }

  if (ws.close === null || ws.close === undefined) {
    throw new Error('CandleData missing required field: close')
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
    openAt: new Date(ws.open_at),
    timestamp: ws.timestamp ? new Date(ws.timestamp) : undefined,
    exchange: ws.exchange,
  }
}

export function tickFromWS(ws: TickData): Tick {
  if (!ws.timestamp) {
    throw new Error('TickData missing required field: timestamp')
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

export function heartbeatFromWS(ws: HeartbeatData): Heartbeat {
  if (!ws.timestamp) {
    throw new Error('HeartbeatData missing required field: timestamp')
  }

  return {
    component: ws.component,
    status: ws.status,
    sequence: ws.sequence,
    timestamp: new Date(ws.timestamp),
    lagMs: ws.lag_ms,
  }
}

export function orderDataFromEnvelope(env: OrderData): OrderData {
  return {
    type: env.type,
    public_id: env.public_id,
    exchange_order_id: env.exchange_order_id,
    client_order_id: env.client_order_id,
    instrument: env.instrument,
    exchange: env.exchange,
    side: env.side,
    status: env.status,
    order_type: env.order_type,
    size: env.size,
    filled_size: env.filled_size,
    price: env.price,
    average_price: env.average_price,
    reason: env.reason,
    time_in_force: env.time_in_force,
    error: env.error,
    created_at: env.created_at,
    updated_at: env.updated_at,
  }
}

export function executionDataFromEnvelope(env: ExecutionData): ExecutionData {
  return {
    type: env.type,
    public_id: env.public_id,
    trade_id: env.trade_id,
    exchange_order_id: env.exchange_order_id,
    client_order_id: env.client_order_id,
    instrument: env.instrument,
    exchange: env.exchange,
    side: env.side,
    size: env.size,
    price: env.price,
    fee: env.fee,
    fee_asset: env.fee_asset,
    status: env.status,
    executed_at: env.executed_at,
  }
}

export function signalDataFromEnvelope(env: SignalData): SignalData {
  return {
    type: env.type,
    public_id: env.public_id,
    instrument: env.instrument,
    exchange: env.exchange,
    side: env.side,
    strength: env.strength,
    reason: env.reason,
    price: env.price,
    strategy_name: env.strategy_name,
    fired_at: env.fired_at ?? env.timestamp,
  }
}

export function ordersFromAPI(apis: OrderData[]): Order[] {
  return apis.map(orderFromAPI)
}

export function executionsFromAPI(apis: ExecutionData[]): Execution[] {
  return apis.map(executionFromAPI)
}

export function signalsFromAPI(apis: SignalData[]): Signal[] {
  return apis.map(signalFromAPI)
}

export function positionsFromAPI(apis: PositionData[]): Position[] {
  return apis.map(positionFromAPI)
}

export function candlesFromAPI(apis: CandleData[]): Candle[] {
  return apis.map(candleFromAPI)
}

export function isTradeSide(value: unknown): value is TradeSide {
  return value === 'buy' || value === 'sell'
}

export function isOrder(value: unknown): value is Order {
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

export function safeOrderFromAPI(api: OrderData): Order | null {
  try {
    return orderFromAPI(api)
  } catch (error) {
    console.error('Failed to transform order from API:', error, api)

    return null
  }
}

export function safeExecutionFromAPI(api: ExecutionData): Execution | null {
  try {
    return executionFromAPI(api)
  } catch (error) {
    console.error('Failed to transform execution from API:', error, api)

    return null
  }
}

export function safeSignalFromAPI(api: SignalData): Signal | null {
  try {
    return signalFromAPI(api)
  } catch (error) {
    console.error('Failed to transform signal from API:', error, api)

    return null
  }
}
