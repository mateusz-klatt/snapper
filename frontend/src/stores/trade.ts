import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'
import type { TradeState } from '../types/ui'
import type { Order, Execution, Signal, Position } from '../types/entities'

interface TradeStore extends TradeState {
  updateOrders: (orders: Order[]) => void
  addOrder: (order: Order) => void
  updateOrder: (orderId: string | number, updates: Partial<Order>) => void
  updateExecutions: (executions: Execution[]) => void
  addExecution: (execution: Execution) => void
  updatePositions: (positions: Position[]) => void
  updatePosition: (instrument: string, exchange: string, updates: Partial<Position>) => void
  updateSignals: (signals: Signal[]) => void
  addSignal: (signal: Signal) => void
  clearTradeData: () => void
}

export const useTradeStore = create<TradeStore>()(
  subscribeWithSelector((set, get) => ({
    orders: [],
    executions: [],
    positions: [],
    signals: [],
    lastUpdate: Date.now(),
    updateOrders: orders => set({ orders }),
    addOrder: order => {
      const current = get().orders
      const existingIndex = current.findIndex(o => o.clientOrderId === order.clientOrderId)

      if (existingIndex >= 0) {
        const updated = [...current]

        updated[existingIndex] = { ...current[existingIndex], ...order }
        set({ orders: updated })
      } else {
        set({ orders: [order, ...current] })
      }
    },
    updateOrder: (orderId, updates) => {
      const current = get().orders
      const updated = current.map(order =>
        order.clientOrderId === orderId ? { ...order, ...updates } : order
      )

      set({ orders: updated })
    },
    updateExecutions: executions => set({ executions }),
    addExecution: execution => {
      const current = get().executions
      const isDuplicate = current.some(e => e.publicId === execution.publicId)

      if (!isDuplicate) {
        set({ executions: [execution, ...current] })
      }
    },
    updatePositions: positions => set({ positions }),
    updatePosition: (instrument, exchange, updates) => {
      const current = get().positions
      const updated = current.map(pos =>
        pos.instrument === instrument && pos.exchange === exchange ? { ...pos, ...updates } : pos
      )

      set({ positions: updated })
    },
    updateSignals: signals => set({ signals }),
    addSignal: signal => {
      const current = get().signals
      const isDuplicate = current.some(
        s =>
          s.firedAt?.getTime() === signal.firedAt?.getTime() &&
          s.strategyName === signal.strategyName &&
          s.instrument === signal.instrument &&
          s.exchange === signal.exchange
      )

      if (!isDuplicate) {
        set({ signals: [signal, ...current].slice(0, 100) })
      }
    },
    clearTradeData: () =>
      set({
        orders: [],
        executions: [],
        positions: [],
        signals: [],
        lastUpdate: Date.now(),
      }),
  }))
)
