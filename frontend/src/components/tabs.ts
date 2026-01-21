export type TabType =
  | 'overview'
  | 'market'
  | 'processes'
  | 'strategies'
  | 'orders'
  | 'signals'
  | 'health'
  | 'admin'
  | 'settings'
interface TabConfig {
  id: TabType
  label: string
  icon: string
}

export const ALL_TABS: readonly TabConfig[] = [
  { id: 'overview', label: 'Overview', icon: '📊' },
  { id: 'market', label: 'Market Data', icon: '📈' },
  { id: 'processes', label: 'Processes', icon: '⚙️' },
  { id: 'strategies', label: 'Strategies', icon: '🎯' },
  { id: 'orders', label: 'Orders & Fills', icon: '📋' },
  { id: 'signals', label: 'Signals', icon: '🔔' },
  { id: 'health', label: 'Health', icon: '❤️' },
  { id: 'admin', label: 'Administration', icon: '👨‍💼' },
  { id: 'settings', label: 'Settings', icon: '🔧' },
] as const
