import { describe, it, expect, beforeEach } from 'vitest'
import { useTradeStore } from './trade'
import type { Order, Execution, Signal, Position } from '../types/entities'
import {
  createTestOrder,
  createTestExecution,
  createTestSignal,
  createTestPosition,
} from '../test/tradeEntityFactories'

describe('useTradeStore', () => {
  beforeEach(() => {
    useTradeStore.setState({
      orders: [],
      executions: [],
      positions: [],
      signals: [],
      lastUpdate: Date.now(),
    })
  })
  describe('initial state', () => {
    it('has correct default values', () => {
      const state = useTradeStore.getState()

      expect(state.orders).toEqual([])
      expect(state.executions).toEqual([])
      expect(state.positions).toEqual([])
      expect(state.signals).toEqual([])
    })
  })
  describe('updateOrders', () => {
    it('sets orders array', () => {
      const orders: Order[] = [
        createTestOrder({ clientOrderId: '1', instrument: 'BTC-USD', size: 1, price: 50000 }),
      ]

      useTradeStore.getState().updateOrders(orders)
      expect(useTradeStore.getState().orders).toEqual(orders)
    })
    it('replaces existing orders', () => {
      const initialOrders: Order[] = [
        createTestOrder({ clientOrderId: '1', instrument: 'BTC-USD', size: 1, price: 50000 }),
      ]
      const newOrders: Order[] = [
        createTestOrder({
          clientOrderId: '2',
          instrument: 'ETH-USD',
          side: 'sell',
          orderType: 'market',
          size: 2,
          price: null,
        }),
      ]

      useTradeStore.getState().updateOrders(initialOrders)
      useTradeStore.getState().updateOrders(newOrders)
      expect(useTradeStore.getState().orders).toEqual(newOrders)
    })
  })
  describe('addOrder', () => {
    it('adds new order to the beginning', () => {
      const order: Order = createTestOrder({
        clientOrderId: '1',
        instrument: 'BTC-USD',
        size: 1,
        price: 50000,
      })

      useTradeStore.getState().addOrder(order)
      expect(useTradeStore.getState().orders[0]).toEqual(order)
    })
    it('updates existing order by id', () => {
      const order: Order = createTestOrder({
        clientOrderId: '1',
        instrument: 'BTC-USD',
        size: 1,
        price: 50000,
      })

      useTradeStore.getState().addOrder(order)
      const updatedOrder: Order = createTestOrder({
        clientOrderId: '1',
        instrument: 'BTC-USD',
        size: 2,
        price: 51000,
      })

      useTradeStore.getState().addOrder(updatedOrder)
      expect(useTradeStore.getState().orders).toHaveLength(1)
      expect(useTradeStore.getState().orders[0].size).toBe(2)
      expect(useTradeStore.getState().orders[0].price).toBe(51000)
    })
  })
  describe('updateOrder', () => {
    it('updates order by id with partial data', () => {
      const order: Order = createTestOrder({
        clientOrderId: '1',
        instrument: 'BTC-USD',
        size: 1,
        price: 50000,
        status: 'open',
      })

      useTradeStore.getState().addOrder(order)
      useTradeStore.getState().updateOrder('1', { status: 'filled' })
      expect(useTradeStore.getState().orders[0].status).toBe('filled')
      expect(useTradeStore.getState().orders[0].size).toBe(1)
    })
    it('does not modify other orders', () => {
      const order1: Order = createTestOrder({
        clientOrderId: '1',
        instrument: 'BTC-USD',
        size: 1,
      })
      const order2: Order = createTestOrder({
        clientOrderId: '2',
        instrument: 'ETH-USD',
        side: 'sell',
        orderType: 'market',
        size: 2,
        price: null,
      })

      useTradeStore.getState().addOrder(order1)
      useTradeStore.getState().addOrder(order2)
      useTradeStore.getState().updateOrder('1', { status: 'filled' })
      const orders = useTradeStore.getState().orders
      const ethOrder = orders.find(o => o.clientOrderId === '2')

      expect(ethOrder?.status).toBe('open')
    })
  })
  describe('updateExecutions', () => {
    it('sets executions array', () => {
      const executions: Execution[] = [
        createTestExecution({
          clientOrderId: 'o1',
          instrument: 'BTC-USD',
          price: 50000,
          size: 1,
        }),
      ]

      useTradeStore.getState().updateExecutions(executions)
      expect(useTradeStore.getState().executions).toEqual(executions)
    })
  })
  describe('addExecution', () => {
    it('adds new execution to the beginning', () => {
      const execution: Execution = createTestExecution({
        clientOrderId: 'o1',
        instrument: 'BTC-USD',
        price: 50000,
        size: 1,
      })

      useTradeStore.getState().addExecution(execution)
      expect(useTradeStore.getState().executions[0]).toEqual(execution)
    })
    it('does not add duplicate execution', () => {
      const execution: Execution = createTestExecution({
        clientOrderId: 'o1',
        instrument: 'BTC-USD',
        price: 50000,
        size: 1,
      })

      useTradeStore.getState().addExecution(execution)
      useTradeStore.getState().addExecution(execution)
      expect(useTradeStore.getState().executions).toHaveLength(1)
    })
  })
  describe('updatePositions', () => {
    it('sets positions array', () => {
      const positions: Position[] = [
        createTestPosition({
          instrument: 'BTC-USD',
          quantity: 1,
          averagePrice: 50000,
          unrealizedPnl: 100,
        }),
      ]

      useTradeStore.getState().updatePositions(positions)
      expect(useTradeStore.getState().positions).toEqual(positions)
    })
  })
  describe('updatePosition', () => {
    it('updates position by instrument with partial data', () => {
      const position: Position = createTestPosition({
        instrument: 'BTC-USD',
        quantity: 1,
        averagePrice: 50000,
        unrealizedPnl: 100,
      })

      useTradeStore.setState({ positions: [position] })
      useTradeStore.getState().updatePosition('BTC-USD', 'kraken', { unrealizedPnl: 200 })
      expect(useTradeStore.getState().positions[0].unrealizedPnl).toBe(200)
      expect(useTradeStore.getState().positions[0].quantity).toBe(1)
    })
    it('does not modify other positions', () => {
      const positions: Position[] = [
        createTestPosition({
          instrument: 'BTC-USD',
          quantity: 1,
          averagePrice: 50000,
          unrealizedPnl: 100,
        }),
        createTestPosition({
          publicId: 2,
          instrument: 'ETH-USD',
          quantity: 2,
          averagePrice: 3000,
          unrealizedPnl: 50,
        }),
      ]

      useTradeStore.setState({ positions })
      useTradeStore.getState().updatePosition('BTC-USD', 'kraken', { unrealizedPnl: 200 })
      const ethPosition = useTradeStore.getState().positions.find(p => p.instrument === 'ETH-USD')

      expect(ethPosition?.unrealizedPnl).toBe(50)
    })
  })
  describe('updateSignals', () => {
    it('sets signals array', () => {
      const signals: Signal[] = [
        createTestSignal({
          instrument: 'BTC-USD',
          strategyName: 'macd',
        }),
      ]

      useTradeStore.getState().updateSignals(signals)
      expect(useTradeStore.getState().signals).toEqual(signals)
    })
  })
  describe('addSignal', () => {
    it('adds new signal to the beginning', () => {
      const signal: Signal = createTestSignal({
        instrument: 'BTC-USD',
        strategyName: 'macd',
      })

      useTradeStore.getState().addSignal(signal)
      expect(useTradeStore.getState().signals[0]).toEqual(signal)
    })
    it('does not add duplicate signal with same firedAt and strategy', () => {
      const firedAt = new Date()
      const signal: Signal = createTestSignal({
        instrument: 'BTC-USD',
        strategyName: 'macd',
        firedAt,
      })

      useTradeStore.getState().addSignal(signal)
      useTradeStore.getState().addSignal({ ...signal, reason: 'duplicate' })
      expect(useTradeStore.getState().signals).toHaveLength(1)
    })
    it('limits signals to 100', () => {
      const firedAt = new Date()

      for (let i = 0; i < 105; i++) {
        const signal: Signal = createTestSignal({
          instrument: 'BTC-USD',
          strategyName: `strategy_${i}`,
          firedAt: new Date(firedAt.getTime() + i),
        })

        useTradeStore.getState().addSignal(signal)
      }

      expect(useTradeStore.getState().signals).toHaveLength(100)
    })
  })
  describe('clearTradeData', () => {
    it('resets all trade data to defaults', () => {
      useTradeStore.setState({
        orders: [createTestOrder({ clientOrderId: '1', instrument: 'BTC-USD', size: 1 })],
        executions: [
          createTestExecution({
            clientOrderId: 'o1',
            instrument: 'BTC-USD',
            price: 50000,
            size: 1,
          }),
        ],
        positions: [
          createTestPosition({ instrument: 'BTC-USD', quantity: 1, averagePrice: 50000 }),
        ],
        signals: [
          createTestSignal({
            instrument: 'BTC-USD',
            strategyName: 'macd',
          }),
        ],
      })
      useTradeStore.getState().clearTradeData()
      const state = useTradeStore.getState()

      expect(state.orders).toEqual([])
      expect(state.executions).toEqual([])
      expect(state.positions).toEqual([])
      expect(state.signals).toEqual([])
    })
  })
})
