import type { BarEnvelope, TickEnvelope } from './ws'
import type { OrderStatus, Fill, Signal, Position } from './entities'

export interface AppState {
  isDarkMode: boolean
  subscribedTopics: string[]
  lastUpdate: string | null
  isConnected: boolean
  connectionLag: number
}
export interface MarketDataState {
  selectedExchange: string | null
  selectedInstrument: string | null
  selectedTimeframe: string
  lastPrice: number | null
  candles: Record<string, BarEnvelope>
  ticks: Record<string, TickEnvelope>
  lastUpdate: number
}
export interface TradeState {
  orders: OrderStatus[]
  executions: Fill[]
  positions: Position[]
  signals: Signal[]
  lastUpdate: number
}
export interface ProcessStatus {
  running: boolean
  lastHeartbeat?: number
  details?: Record<string, unknown>
}
export interface ProcessControlState {
  feeds: Record<string, ProcessStatus>
  strategies: Record<string, ProcessStatus>
  executors: Record<string, ProcessStatus>
  brokers: Record<string, ProcessStatus>
}
