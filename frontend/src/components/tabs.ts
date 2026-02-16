import type { LucideIcon } from 'lucide-react'
import {
  Bell,
  ChartCandlestick,
  ClipboardList,
  Gauge,
  HeartPulse,
  LayoutDashboard,
  Settings,
  Shield,
  Workflow,
} from 'lucide-react'

type TabType =
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
  icon: LucideIcon
}

export const ALL_TABS: readonly TabConfig[] = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'market', label: 'Market Data', icon: ChartCandlestick },
  { id: 'processes', label: 'Processes', icon: Workflow },
  { id: 'strategies', label: 'Strategies', icon: Gauge },
  { id: 'orders', label: 'Orders & Fills', icon: ClipboardList },
  { id: 'signals', label: 'Signals', icon: Bell },
  { id: 'health', label: 'Health', icon: HeartPulse },
  { id: 'admin', label: 'Administration', icon: Shield },
  { id: 'settings', label: 'Settings', icon: Settings },
] as const
