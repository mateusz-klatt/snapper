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
  lastUpdate: number
}
export interface UIProcessStatus {
  running: boolean
  lastHeartbeat?: number
  details?: Record<string, unknown>
}
export interface ProcessControlState {
  feeds: Record<string, UIProcessStatus>
  strategies: Record<string, UIProcessStatus>
  executors: Record<string, UIProcessStatus>
  brokers: Record<string, UIProcessStatus>
}
