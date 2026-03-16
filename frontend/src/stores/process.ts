import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'
import { ProcessControlState, UIProcessStatus } from '../types/ui'

interface ProcessControlStore extends ProcessControlState {
  updateFeedStatus: (feedId: string, status: UIProcessStatus) => void
  updateStrategyStatus: (strategyId: string, status: UIProcessStatus) => void
  updateExecutorStatus: (executorId: string, status: UIProcessStatus) => void
  updateBrokerStatus: (brokerId: string, status: UIProcessStatus) => void
  resetProcessStates: () => void
}

export const useProcessStore = create<ProcessControlStore>()(
  subscribeWithSelector((set, get) => ({
    feeds: {},
    strategies: {},
    executors: {},
    brokers: {},
    updateFeedStatus: (feedId, status) => {
      const current = get().feeds

      set({ feeds: { ...current, [feedId]: status } })
    },
    updateStrategyStatus: (strategyId, status) => {
      const current = get().strategies

      set({ strategies: { ...current, [strategyId]: status } })
    },
    updateExecutorStatus: (executorId, status) => {
      const current = get().executors

      set({ executors: { ...current, [executorId]: status } })
    },
    updateBrokerStatus: (brokerId, status) => {
      const current = get().brokers

      set({ brokers: { ...current, [brokerId]: status } })
    },
    resetProcessStates: () =>
      set({
        feeds: {},
        strategies: {},
        executors: {},
        brokers: {},
      }),
  }))
)
