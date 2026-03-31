export interface AppState {
  isDarkMode: boolean
  subscribedTopics: string[]
  lastUpdate: string | null
  isConnected: boolean
  connectionLag: number
  asOf: string | null
  isTimeTraveling: boolean
}
export interface MarketDataState {
  selectedExchange: string | null
  selectedInstrument: string | null
  selectedTimeframe: string
  lastPrice: number | null
  lastUpdate: number
}
