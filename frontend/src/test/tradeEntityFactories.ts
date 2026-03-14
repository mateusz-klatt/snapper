import type { Order, Execution, Signal, Position } from '../types/entities'

const DEFAULT_ORDER_OVERRIDES: Partial<Order> & {
  clientOrderId: string
  instrument: string
} = {
  clientOrderId: 'order-1',
  instrument: 'BTC-USD',
}

export function createTestOrder(
  overrides: Partial<Order> & {
    clientOrderId: string
    instrument: string
  } = DEFAULT_ORDER_OVERRIDES
): Order {
  const now = new Date()

  return {
    clientOrderId: overrides.clientOrderId,
    exchangeOrderId: overrides.exchangeOrderId ?? null,
    instrument: overrides.instrument,
    exchange: overrides.exchange ?? 'kraken',
    side: overrides.side ?? 'buy',
    orderType: overrides.orderType ?? 'limit',
    size: overrides.size ?? 1,
    filledSize: overrides.filledSize ?? 0,
    price: overrides.price ?? 50000,
    averagePrice: overrides.averagePrice ?? null,
    status: overrides.status ?? 'open',
    createdAt: overrides.createdAt ?? now,
    updatedAt: overrides.updatedAt ?? null,
  }
}

const DEFAULT_EXECUTION_OVERRIDES: Partial<Execution> & {
  clientOrderId: string
} = {
  clientOrderId: 'order-1',
}

export function createTestExecution(
  overrides: Partial<Execution> & {
    clientOrderId: string
  } = DEFAULT_EXECUTION_OVERRIDES
): Execution {
  const now = new Date()

  return {
    clientOrderId: overrides.clientOrderId,
    tradeId: overrides.tradeId ?? null,
    exchangeOrderId: overrides.exchangeOrderId ?? null,
    exchange: overrides.exchange ?? 'kraken',
    instrument: overrides.instrument ?? 'BTC-USD',
    side: overrides.side ?? 'buy',
    size: overrides.size ?? 0.5,
    price: overrides.price ?? 50000,
    fee: overrides.fee ?? 0.001,
    feeAsset: overrides.feeAsset ?? 'BTC',
    status: overrides.status ?? 'filled',
    executedAt: overrides.executedAt ?? now,
  }
}

const DEFAULT_SIGNAL_OVERRIDES: Partial<Signal> & { instrument: string } = {
  instrument: 'BTC-USD',
}

export function createTestSignal(
  overrides: Partial<Signal> & { instrument: string } = DEFAULT_SIGNAL_OVERRIDES
): Signal {
  const now = new Date()

  return {
    exchange: overrides.exchange ?? 'kraken',
    instrument: overrides.instrument,
    side: overrides.side ?? 'buy',
    strength: overrides.strength ?? 0.8,
    reason: overrides.reason ?? 'Test signal',
    strategyName: overrides.strategyName ?? 'test-strategy',
    price: overrides.price ?? 50000,
    firedAt: overrides.firedAt ?? now,
  }
}

const DEFAULT_POSITION_OVERRIDES: Partial<Position> & { instrument: string } = {
  instrument: 'BTC-USD',
}

export function createTestPosition(
  overrides: Partial<Position> & { instrument: string } = DEFAULT_POSITION_OVERRIDES
): Position {
  const now = new Date()

  return {
    publicId: overrides.publicId ?? 1,
    instrument: overrides.instrument,
    exchange: overrides.exchange ?? 'kraken',
    quantity: overrides.quantity ?? 1,
    averagePrice: overrides.averagePrice ?? 50000,
    unrealizedPnl: overrides.unrealizedPnl ?? 0,
    realizedPnl: overrides.realizedPnl ?? 0,
    updatedAt: overrides.updatedAt ?? now,
  }
}
