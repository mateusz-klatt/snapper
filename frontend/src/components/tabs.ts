import type { LucideIcon } from 'lucide-react'
import {
  BarChart3,
  Bell,
  ChartCandlestick,
  ClipboardList,
  Gauge,
  HeartPulse,
  Inbox,
  LayoutDashboard,
  Settings,
  Shield,
  TrendingUp,
  Workflow,
  Zap,
} from 'lucide-react'

type TabType =
  | 'overview'
  | 'market'
  | 'processes'
  | 'strategies'
  | 'orders'
  | 'positions'
  | 'signals'
  | 'backtests'
  | 'health'
  | 'admin'
  | 'ai-integration'
  | 'ai-reviews'
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
  { id: 'orders', label: 'Orders & Executions', icon: ClipboardList },
  { id: 'positions', label: 'Positions', icon: TrendingUp },
  { id: 'signals', label: 'Signals', icon: Bell },
  { id: 'backtests', label: 'Backtests', icon: BarChart3 },
  { id: 'health', label: 'Health', icon: HeartPulse },
  { id: 'admin', label: 'Administration', icon: Shield },
  { id: 'ai-integration', label: 'AI Integration', icon: Zap },
  { id: 'ai-reviews', label: 'AI Reviews', icon: Inbox },
  { id: 'settings', label: 'Settings', icon: Settings },
] as const
