import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'
import type { TradeState } from '../types/ui'
import type { OrderStatus, Fill, Signal, Position } from '../types/entities'

interface TradeStore extends TradeState {
  updateOrders: (orders: OrderStatus[]) => void
  addOrder: (order: OrderStatus) => void
  updateOrder: (orderId: string | number, updates: Partial<OrderStatus>) => void
  updateExecutions: (executions: Fill[]) => void
  addExecution: (execution: Fill) => void
  updatePositions: (positions: Position[]) => void
  updatePosition: (instrument: string, updates: Partial<Position>) => void
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
      const existingIndex = current.findIndex(o => o.id === order.id)

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
        order.id === orderId ? { ...order, ...updates } : order
      )

      set({ orders: updated })
    },
    updateExecutions: executions => set({ executions }),
    addExecution: execution => {
      const current = get().executions
      const existing = current.find(e => e.id === execution.id)

      if (!existing) {
        set({ executions: [execution, ...current] })
      }
    },
    updatePositions: positions => set({ positions }),
    updatePosition: (instrument, updates) => {
      const current = get().positions
      const updated = current.map(pos =>
        pos.instrument === instrument ? { ...pos, ...updates } : pos
      )

      set({ positions: updated })
    },
    updateSignals: signals => set({ signals }),
    addSignal: signal => {
      const current = get().signals
      const existing = current.find(
        s =>
          s.timestamp?.getTime() === signal.timestamp?.getTime() &&
          s.strategyName === signal.strategyName
      )

      if (!existing) {
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
