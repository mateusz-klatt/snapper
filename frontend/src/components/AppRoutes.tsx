import React from 'react'
import ErrorBoundary from './ErrorBoundary'
import { Overview } from '../features/overview/Overview'
import { MarketData } from '../features/market/MarketData'
import { Processes } from '../features/processes/Processes'
import { Strategies } from '../features/strategies/Strategies'
import { Orders } from '../features/orders/Orders'
import { Signals } from '../features/signals/Signals'
import { Health } from '../features/health/Health'
import { Admin } from '../features/admin/Admin'
import { Settings } from '../features/settings/Settings'

interface AppRoutesProps {
  activeTab: string
}

export function AppRoutes({ activeTab }: Readonly<AppRoutesProps>): React.ReactElement {
  switch (activeTab) {
    case 'market':
      return (
        <ErrorBoundary componentName='Market Data'>
          <MarketData />
        </ErrorBoundary>
      )
    case 'processes':
      return (
        <ErrorBoundary componentName='Processes'>
          <Processes />
        </ErrorBoundary>
      )
    case 'strategies':
      return (
        <ErrorBoundary componentName='Strategies'>
          <Strategies />
        </ErrorBoundary>
      )
    case 'orders':
      return (
        <ErrorBoundary componentName='Orders'>
          <Orders />
        </ErrorBoundary>
      )
    case 'signals':
      return (
        <ErrorBoundary componentName='Signals'>
          <Signals />
        </ErrorBoundary>
      )
    case 'health':
      return (
        <ErrorBoundary componentName='Health'>
          <Health />
        </ErrorBoundary>
      )
    case 'admin':
      return (
        <ErrorBoundary componentName='Administration'>
          <Admin />
        </ErrorBoundary>
      )
    case 'settings':
      return (
        <ErrorBoundary componentName='Settings'>
          <Settings />
        </ErrorBoundary>
      )
    case 'overview':
    default:
      return (
        <ErrorBoundary componentName='Overview'>
          <Overview />
        </ErrorBoundary>
      )
  }
}
